#!/usr/bin/env python3
"""Verify OLTP sources from config.json / cdc_catalog.connections — TCP + MariaDB CDC preflight."""
from __future__ import annotations

import importlib.util
import os
import shutil
import socket
import subprocess
import sys
from typing import Any

ROOT = os.path.dirname(os.path.abspath(__file__))
BOOTSTRAP = os.path.join(ROOT, "catalog_bootstrap.py")


def load_bootstrap():
    spec = importlib.util.spec_from_file_location("catalog_bootstrap", BOOTSTRAP)
    if spec is None or spec.loader is None:
        raise RuntimeError("catalog_bootstrap.py not found")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def tcp_ok(host: str, port: int, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def check_mariadb(src: dict[str, Any]) -> tuple[bool, list[str], list[str]]:
    host = str(src.get("host", "localhost"))
    port = int(src.get("port", 3306))
    errors: list[str] = []
    warnings: list[str] = []

    if not tcp_ok(host, port):
        errors.append(f"TCP {host}:{port} unreachable")
        return False, errors, warnings

    mysql_bin = shutil.which("mysql")
    if not mysql_bin:
        warnings.append("mysql client not in PATH — TCP only (binlog ROW not verified)")
        return True, errors, warnings

    user = str(src.get("user", src.get("username", "")))
    password = str(src.get("password", ""))
    database = str(src.get("database", src.get("db_name", "")) or "mysql")

    cmd = [
        mysql_bin,
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        "-N",
        "-B",
        "-e",
        "SELECT @@log_bin, @@binlog_format;",
        database,
    ]
    env = os.environ.copy()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=15, env=env)
    except subprocess.TimeoutExpired:
        errors.append("mysql client timed out")
        return False, errors, warnings

    if res.returncode != 0:
        detail = (res.stderr or res.stdout or "mysql auth/query failed").strip().splitlines()[-1]
        errors.append(detail)
        return False, errors, warnings

    parts = (res.stdout or "").strip().split("\t")
    if len(parts) < 2:
        errors.append("unexpected mysql variables output")
        return False, errors, warnings

    log_bin, binlog_format = parts[0].upper(), parts[1].upper()

    if log_bin not in ("ON", "1"):
        errors.append(f"log_bin={log_bin} (required ON)")
    if binlog_format != "ROW":
        errors.append(f"binlog_format={binlog_format} (required ROW)")

    binlog_bin = shutil.which("mariadb-binlog") or shutil.which("mysqlbinlog")
    if not binlog_bin:
        errors.append("mariadb-binlog not in PATH (CDC capture requires mariadb-client)")
        return False, errors, warnings

    master_cmd = [
        mysql_bin,
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        "-N",
        "-B",
        "-e",
        "SHOW MASTER STATUS;",
        database,
    ]
    try:
        master_res = subprocess.run(master_cmd, capture_output=True, text=True, timeout=15, env=env)
    except subprocess.TimeoutExpired:
        errors.append("SHOW MASTER STATUS timed out")
        return False, errors, warnings
    if master_res.returncode != 0:
        errors.append("SHOW MASTER STATUS failed")
        return False, errors, warnings

    master_parts = (master_res.stdout or "").strip().split("\t")
    if len(master_parts) < 2 or not master_parts[0]:
        errors.append("SHOW MASTER STATUS empty (binlog disabled?)")
        return False, errors, warnings

    binlog_file = master_parts[0]
    binlog_pos = master_parts[1]
    probe_cmd = [
        binlog_bin,
        "--read-from-remote-server",
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        f"-p{password}",
        f"--start-position={binlog_pos}",
        "--stop-never",
        "-v",
        binlog_file,
    ]
    try:
        probe_res = subprocess.run(
            probe_cmd,
            capture_output=True,
            text=True,
            timeout=8,
            env=env,
        )
    except subprocess.TimeoutExpired:
        pass
    else:
        detail = (probe_res.stderr or probe_res.stdout or "").strip()
        if probe_res.returncode != 0 and detail:
            tail = detail.splitlines()[-1]
            if "Access denied" in detail or "REPLICATION" in detail.upper():
                errors.append(f"binlog replication denied: {tail}")
            elif "Could not find" in detail or "binlog" in detail.lower():
                errors.append(f"binlog read failed: {tail}")
            else:
                warnings.append(f"binlog probe exit {probe_res.returncode}: {tail}")

    return len(errors) == 0, errors, warnings


def check_tcp_only(src: dict[str, Any], default_port: int) -> tuple[bool, list[str], list[str]]:
    host = str(src.get("host", "localhost"))
    port = int(src.get("port", default_port))
    if tcp_ok(host, port):
        return True, [], []
    return False, [f"TCP {host}:{port} unreachable"], []


def main() -> int:
    bootstrap = load_bootstrap()
    config_path = os.environ.get("DATASYNC_CONFIG", "/app/config.json")
    if not os.path.isfile(config_path):
        print("verify_sources: no config.json", file=sys.stderr)
        return 1

    cfg = bootstrap.load_config(config_path)
    pg = bootstrap.pg_section(cfg, "datasync")
    host = str(pg.get("host", "localhost"))
    port = str(pg.get("port", 5432))
    database = str(pg.get("database", "datasync"))
    user = str(pg.get("user", ""))
    password = bootstrap.resolve_password("datasync", pg)
    sslmode = str(pg.get("sslmode", os.environ.get("DATASYNC_PG_SSLMODE", "")))

    if not user or not password:
        print("verify_sources: missing datasync credentials", file=sys.stderr)
        return 1

    env = bootstrap.pg_env(host, port, database, user, password, sslmode)

    sources = bootstrap.sources_from_config(cfg)
    if not sources:
        sources = bootstrap.sources_from_env()

    if not sources:
        print("✔ no sources configured — skip (add sources[] later, then verify-sources + discover)")
        return 0

    failed = 0
    for src in sources:
        conn_id = src["conn_id"]
        engine = src.get("engine", "mariadb")
        ok = False
        errors: list[str] = []
        warnings: list[str] = []

        if engine == "mariadb":
            ok, errors, warnings = check_mariadb(src)
        elif engine == "mssql":
            ok, errors, warnings = check_tcp_only(src, 1433)
        elif engine == "mongodb":
            ok, errors, warnings = check_tcp_only(src, 27017)
        else:
            errors = [f"unsupported engine {engine}"]
            ok = False

        if ok:
            print(f"✔ source {conn_id} ({engine})")
            for w in warnings:
                print(f"WARN: {conn_id}: {w}", file=sys.stderr)
        else:
            failed += 1
            print(f"✖ source {conn_id} ({engine})", file=sys.stderr)
            for e in errors:
                print(f"  {e}", file=sys.stderr)

    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as ex:
        print(f"verify_sources failed: {ex}", file=sys.stderr)
        raise SystemExit(1) from ex
