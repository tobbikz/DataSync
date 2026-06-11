#!/usr/bin/env python3
"""Simulate OLTP load on MariaDB — one transaction per interval (default 1s).

Used with DataSync daemon running: generates binlog events for CDC capture/apply.

Usage:
  ./scripts/mariadb_txn_simulator.py --conn-id MARIADB_LOCAL --schema test --table smoke_cdc
  ./scripts/mariadb_txn_simulator.py --duration 300 --interval 1.0
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path


def load_pg_dsn() -> dict:
    root = Path(__file__).resolve().parents[1]
    cfg_path = os.environ.get("DATASYNC_CONFIG", str(root / "config.json"))
    with open(cfg_path) as f:
        cfg = json.load(f)
    pg = cfg["datasync"]
    return {
        "host": pg["host"],
        "port": int(pg.get("port", 5432)),
        "user": pg["user"],
        "password": pg["password"],
        "database": pg["database"],
    }


def load_mariadb_conn(conn_id: str) -> dict:
    import psycopg2

    pg = load_pg_dsn()
    with psycopg2.connect(**pg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT host, port, db_name, username, password
                FROM cdc_catalog.connections
                WHERE alias = %s AND active
                """,
                (conn_id,),
            )
            row = cur.fetchone()
    if not row:
        raise SystemExit(f"connection not found or inactive: {conn_id}")
    host, port, db_name, user, password = row
    override = os.environ.get("MARIADB_PORT_OVERRIDE")
    if override:
        port = int(override)
    return {
        "host": host,
        "port": int(port),
        "user": user or os.environ.get("MARIADB_USER", "root"),
        "password": password or os.environ.get("MARIADB_PASSWORD", ""),
        "database": db_name or "test",
    }


def mariadb_cli(mysql_cfg: dict, sql: str) -> str:
    cmd = ["mariadb", "--batch", "--skip-column-names"]
    local = mysql_cfg["host"] in ("127.0.0.1", "localhost", "::1")
    use_socket = local and os.environ.get("MARIADB_USE_TCP", "0") != "1"
    if os.environ.get("MARIADB_USE_TCP") == "1":
        use_socket = False
    # Local dev: unix_socket auth (omit -h/-u/-p). Prod/docker: set MARIADB_USE_TCP=1.
    if not use_socket:
        cmd.extend(["-h", mysql_cfg["host"], "-P", str(mysql_cfg["port"])])
        if mysql_cfg["user"]:
            cmd.extend(["-u", mysql_cfg["user"]])
        if mysql_cfg["password"]:
            cmd.append(f"-p{mysql_cfg['password']}")
    cmd.extend(["-e", sql])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "mariadb failed")
    return proc.stdout.strip()


def ensure_schema(mysql_cfg: dict, schema: str, table: str) -> None:
    mariadb_cli(
        mysql_cfg,
        f"""
        CREATE DATABASE IF NOT EXISTS `{schema}`;
        CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(128) NOT NULL DEFAULT '',
            amount INT NOT NULL DEFAULT 0,
            deleted_at DATETIME NULL,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        INSERT INTO `{schema}`.`{table}` (name, amount)
        SELECT * FROM (
            SELECT 'seed_1', 1 UNION ALL SELECT 'seed_2', 2
        ) s
        WHERE (SELECT COUNT(*) FROM `{schema}`.`{table}`) = 0;
        """,
    )


def run_txn(mysql_cfg: dict, schema: str, table: str) -> str:
    op = random.choice(["insert", "update", "update", "delete"])
    if op == "insert":
        n = random.randint(1, 999_999)
        mariadb_cli(
            mysql_cfg,
            f"INSERT INTO `{schema}`.`{table}` (name, amount) "
            f"VALUES ('sim_{int(time.time())}_{n}', {n % 1000});",
        )
        return f"INSERT sim_{n}"
    pk = mariadb_cli(
        mysql_cfg,
        f"SELECT id FROM `{schema}`.`{table}` ORDER BY RAND() LIMIT 1;",
    )
    if not pk:
        mariadb_cli(
            mysql_cfg,
            f"INSERT INTO `{schema}`.`{table}` (name, amount) VALUES ('bootstrap', 1);",
        )
        return "INSERT bootstrap"
    pk_id = pk.splitlines()[0].strip()
    if op == "delete":
        mariadb_cli(mysql_cfg, f"DELETE FROM `{schema}`.`{table}` WHERE id = {pk_id};")
        return f"DELETE id={pk_id}"
    mariadb_cli(
        mysql_cfg,
        f"UPDATE `{schema}`.`{table}` SET amount = amount + 1, "
        f"name = 'upd_{int(time.time())}' WHERE id = {pk_id};",
    )
    return f"UPDATE id={pk_id}"


def main() -> int:
    parser = argparse.ArgumentParser(description="MariaDB OLTP simulator for CDC stress")
    parser.add_argument("--conn-id", default=os.environ.get("CONN_ID", "MARIADB_LOCAL"))
    parser.add_argument("--schema", default=os.environ.get("SOURCE_SCHEMA", "test"))
    parser.add_argument("--table", default=os.environ.get("SOURCE_TABLE", "smoke_cdc"))
    parser.add_argument("--interval", type=float, default=float(os.environ.get("TXN_INTERVAL_SEC", "1.0")))
    parser.add_argument("--duration", type=int, default=int(os.environ.get("TXN_DURATION_SEC", "0")),
                        help="0 = run until SIGINT")
    parser.add_argument("--bootstrap-only", action="store_true")
    args = parser.parse_args()

    mysql_cfg = load_mariadb_conn(args.conn_id)
    ensure_schema(mysql_cfg, args.schema, args.table)
    if args.bootstrap_only:
        print(f"OK bootstrap {args.schema}.{args.table} @ {mysql_cfg['host']}:{mysql_cfg['port']}")
        return 0

    print(
        f"txn simulator started conn={args.conn_id} table={args.schema}.{args.table} "
        f"interval={args.interval}s duration={args.duration or '∞'}s",
        flush=True,
    )
    start = time.monotonic()
    n = 0
    try:
        while True:
            t0 = time.monotonic()
            try:
                detail = run_txn(mysql_cfg, args.schema, args.table)
                n += 1
                print(f"[{n}] {detail}", flush=True)
            except Exception as ex:
                print(f"[{n}] ERROR {ex}", file=sys.stderr, flush=True)
            if args.duration > 0 and (time.monotonic() - start) >= args.duration:
                break
            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, args.interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        pass
    print(f"txn simulator stopped after {n} transactions", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
