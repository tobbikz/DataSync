#!/usr/bin/env python3
"""Seed cdc_catalog.connections from config.json sources + legacy env. Idempotent UPSERT."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any


def pg_env(host: str, port: str, db: str, user: str, password: str, sslmode: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    if sslmode:
        env["PGSSLMODE"] = sslmode
    return env


def psql_sql(env: dict[str, str], host: str, port: str, user: str, db: str, sql: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["psql", "-h", host, "-p", port, "-U", user, "-d", db, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
        env=env,
        capture_output=True,
        text=True,
    )


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def pg_section(cfg: dict[str, Any], key: str) -> dict[str, Any]:
    base = cfg.get("datasync", {})
    sec = cfg.get(key, base) if key != "datasync" else base
    if not isinstance(sec, dict):
        return dict(base) if isinstance(base, dict) else {}
    out = dict(base)
    out.update(sec)
    return out


def resolve_password(section: str, pg: dict[str, Any]) -> str:
    env_key = "DATASYNC_PG_PASSWORD" if section == "datasync" else "DATALAKE_PG_PASSWORD"
    return os.environ.get(env_key, pg.get("password", ""))


def engine_default_port(engine: str) -> int:
    return {"mariadb": 3306, "mssql": 1433, "mongodb": 27017}.get(engine, 0)


def normalize_source(item: dict[str, Any], default_engine: str) -> dict[str, Any] | None:
    conn_id = (item.get("conn_id") or item.get("alias") or "").strip()
    if not conn_id:
        return None
    out = dict(item)
    out["conn_id"] = conn_id
    engine = str(out.get("engine", default_engine)).lower()
    if engine == "mongo":
        engine = "mongodb"
    out["engine"] = engine
    prefix = {"mariadb": "MARIADB", "mssql": "MSSQL", "mongodb": "MONGO"}.get(engine, "")
    if prefix and not out.get("password"):
        out["password"] = os.environ.get(f"{prefix}_PASSWORD", "")
    if prefix and not out.get("user") and not out.get("username"):
        out["user"] = os.environ.get(f"{prefix}_USER", "")
    return out


def sources_from_config(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}

    def add(item: dict[str, Any] | None) -> None:
        if not item:
            return
        by_id[item["conn_id"]] = item

    if isinstance(cfg.get("sources"), list):
        for raw in cfg["sources"]:
            if isinstance(raw, dict):
                add(normalize_source(raw, str(raw.get("engine", "mariadb")).lower()))

    for engine in ("mariadb", "mssql", "mongodb"):
        val = cfg.get(engine)
        if isinstance(val, list):
            for raw in val:
                if isinstance(raw, dict):
                    add(normalize_source(raw, engine))
        elif isinstance(val, dict):
            add(normalize_source(val, engine))

    return list(by_id.values())


def sources_from_env() -> list[dict[str, Any]]:
    specs = [
        ("mariadb", "MARIADB"),
        ("mssql", "MSSQL"),
        ("mongodb", "MONGO"),
    ]
    out: list[dict[str, Any]] = []
    for engine, prefix in specs:
        conn_id = os.environ.get(f"{prefix}_CONN_ID", "").strip()
        if not conn_id:
            continue
        out.append(
            {
                "conn_id": conn_id,
                "engine": engine,
                "host": os.environ.get(f"{prefix}_HOST", "localhost"),
                "port": int(os.environ.get(f"{prefix}_PORT", engine_default_port(engine))),
                "database": os.environ.get(f"{prefix}_DB", os.environ.get(f"{prefix}_DATABASE", "")),
                "user": os.environ.get(f"{prefix}_USER", ""),
                "password": os.environ.get(f"{prefix}_PASSWORD", ""),
                "active": os.environ.get(f"{prefix}_ACTIVE", "true").lower() != "false",
                "extras": {},
            }
        )
    return out


def sql_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def upsert_connection(env, host, port, user, db, src: dict[str, Any]) -> None:
    engine = src.get("engine", "mariadb")
    if engine not in ("mariadb", "mssql", "mongodb"):
        raise ValueError(f"unsupported engine: {engine}")
    alias = src["conn_id"]
    db_name = src.get("database", src.get("db_name", ""))
    extras = src.get("extras", {})
    if not isinstance(extras, dict):
        extras = {}
    active = "true" if src.get("active", True) else "false"
    sql = f"""
INSERT INTO cdc_catalog.connections
    (alias, db_engine, host, port, db_name, username, password, extras, active)
VALUES (
    {sql_literal(alias)},
    {sql_literal(engine)}::cdc_catalog.db_engine,
    {sql_literal(str(src.get("host", "localhost")))},
    {int(src.get("port", engine_default_port(engine)))},
    {sql_literal(str(db_name))},
    {sql_literal(str(src.get("user", src.get("username", ""))))},
    {sql_literal(str(src.get("password", "")))},
    {sql_literal(json.dumps(extras))}::jsonb,
    {active}
)
ON CONFLICT (alias) DO UPDATE SET
    db_engine = EXCLUDED.db_engine,
    host = EXCLUDED.host,
    port = EXCLUDED.port,
    db_name = EXCLUDED.db_name,
    username = EXCLUDED.username,
    password = EXCLUDED.password,
    extras = EXCLUDED.extras,
    active = EXCLUDED.active,
    updated_at = now();
"""
    res = psql_sql(env, host, port, user, db, sql)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.strip() or "connection upsert failed")


def main() -> int:
    config_path = os.environ.get("DATASYNC_CONFIG", "/app/config.json")
    if not os.path.isfile(config_path):
        print("catalog_bootstrap: no config — skip connections seed", file=sys.stderr)
        return 0

    cfg = load_config(config_path)
    pg = pg_section(cfg, "datasync")
    host = str(pg.get("host", "localhost"))
    port = str(pg.get("port", 5432))
    database = str(pg.get("database", "datasync"))
    user = str(pg.get("user", ""))
    password = resolve_password("datasync", pg)
    sslmode = str(pg.get("sslmode", os.environ.get("DATASYNC_PG_SSLMODE", "")))

    if not user or not password:
        print("catalog_bootstrap: missing datasync credentials", file=sys.stderr)
        return 1

    env = pg_env(host, port, database, user, password, sslmode)

    sources = sources_from_config(cfg)
    if not sources:
        sources = sources_from_env()

    seeded = 0
    for src in sources:
        upsert_connection(env, host, port, user, database, src)
        seeded += 1

    if seeded:
        print(f"connections seeded/updated: {seeded}")
    else:
        print("catalog_bootstrap: no sources in config.json (add sources[] — see config.json.example)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as ex:
        print(f"catalog_bootstrap failed: {ex}", file=sys.stderr)
        raise SystemExit(1) from ex
