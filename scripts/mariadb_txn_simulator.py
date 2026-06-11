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
import sys
import time
from pathlib import Path

try:
    import pymysql
except ImportError:
    print("Install pymysql: pip install pymysql", file=sys.stderr)
    sys.exit(1)


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
        "user": user,
        "password": password,
        "database": db_name or "test",
    }


def ensure_schema(mysql_cfg: dict, schema: str, table: str) -> None:
    conn = pymysql.connect(**mysql_cfg, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
            cur.execute(f"USE `{schema}`")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{table}` (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL DEFAULT '',
                    amount INT NOT NULL DEFAULT 0,
                    deleted_at DATETIME NULL,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                        ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB
                """
            )
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            if cur.fetchone()[0] == 0:
                cur.executemany(
                    f"INSERT INTO `{table}` (name, amount) VALUES (%s, %s)",
                    [(f"seed_{i}", i) for i in range(1, 101)],
                )
    finally:
        conn.close()


def run_txn(mysql_cfg: dict, schema: str, table: str) -> str:
    conn = pymysql.connect(**mysql_cfg, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"USE `{schema}`")
            op = random.choice(["insert", "update", "update", "delete"])
            if op == "insert":
                n = random.randint(1, 999_999)
                cur.execute(
                    f"INSERT INTO `{table}` (name, amount) VALUES (%s, %s)",
                    (f"sim_{int(time.time())}_{n}", n % 1000),
                )
                return f"INSERT id={cur.lastrowid}"
            cur.execute(f"SELECT id FROM `{table}` ORDER BY RAND() LIMIT 1")
            row = cur.fetchone()
            if not row:
                cur.execute(
                    f"INSERT INTO `{table}` (name, amount) VALUES (%s, %s)",
                    ("bootstrap", 1),
                )
                return f"INSERT bootstrap id={cur.lastrowid}"
            pk = row[0]
            if op == "delete":
                cur.execute(f"DELETE FROM `{table}` WHERE id = %s", (pk,))
                return f"DELETE id={pk}"
            cur.execute(
                f"UPDATE `{table}` SET amount = amount + 1, name = %s WHERE id = %s",
                (f"upd_{int(time.time())}", pk),
            )
            return f"UPDATE id={pk}"
    finally:
        conn.close()


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
