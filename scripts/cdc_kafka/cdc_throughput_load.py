#!/usr/bin/env python3
"""Bulk INSERT load for CDC throughput testing (stdlib only)."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from typing import Callable


def run_cmd(
    cmd: list[str],
    *,
    input_text: str | None = None,
    check: bool = True,
    env: dict[str, str] | None = None,
    log_cmd: str | None = None,
) -> subprocess.CompletedProcess[str]:
    print(f"  $ {log_cmd or ' '.join(cmd)}", flush=True)
    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        check=check,
        env=env,
    )


def mariadb_run_sql(base: list[str], sql: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run SQL via stdin — avoids ARG_MAX on large multi-row INSERT."""
    return run_cmd(base, input_text=sql, check=False, env=env, log_cmd=f"{' '.join(base)} <stdin>")


def timed(label: str, fn: Callable[[], None]) -> float:
    print(f"\n== {label} ==", flush=True)
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    rate = ""
    print(f"  done in {elapsed:.1f}s {rate}", flush=True)
    return elapsed


def mariadb_cross_join_sql(schema: str, table: str, run_id: str, row_count: int) -> str:
    """Single-statement bulk insert using cross joins (4^10 >= 1M rows)."""
    digit_cols = " + ".join(f"d{i}.d * {4 ** i}" for i in range(10))
    joins = "\n       ".join(
        f"CROSS JOIN (SELECT 0 AS d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) d{i}"
        for i in range(1, 10)
    )
    return f"""
USE `{schema}`;
SET SESSION max_statement_time = 0;
INSERT INTO `{table}` (`name`, `last_name`)
SELECT
  CONCAT('stress-{run_id}-m-', n),
  CONCAT('ln-', n)
FROM (
  SELECT ({digit_cols} + 1) AS n
  FROM (SELECT 0 AS d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) d0
       {joins}
) numbered
WHERE n <= {row_count};
SELECT COUNT(*) AS source_rows FROM `{table}`;
"""


def mariadb_batch_insert(
    host: str,
    port: int,
    user: str,
    password: str,
    schema: str,
    table: str,
    run_id: str,
    row_count: int,
    batch_size: int,
) -> None:
    env = os.environ.copy()
    env["MYSQL_PWD"] = password
    base = [
        "mariadb",
        "-h",
        host,
        "-P",
        str(port),
        "-u",
        user,
        "-N",
        "-B",
    ]

    if row_count <= 1_048_576:
        sql = mariadb_cross_join_sql(schema, table, run_id, row_count)
        res = mariadb_run_sql(base, sql, env)
        if res.returncode == 0 and res.stdout.strip():
            print(res.stdout.strip())
            return
        err = (res.stderr or res.stdout or "unknown error").strip()
        print(f"  cross-join insert failed ({err[:500]}); falling back to batched stdin inserts", flush=True)

    inserted = 0
    batch_num = 0
    while inserted < row_count:
        n = min(batch_size, row_count - inserted)
        values = []
        for i in range(n):
            seq = inserted + i + 1
            values.append(f"('stress-{run_id}-m-{seq}','ln-{seq}')")
        sql = (
            f"INSERT INTO `{schema}`.`{table}` (`name`, `last_name`) VALUES "
            + ",".join(values)
            + ";"
        )
        res = mariadb_run_sql(base, sql, env)
        if res.returncode != 0:
            print(res.stderr or res.stdout, file=sys.stderr)
            raise SystemExit(f"MariaDB batch {batch_num} failed")
        inserted += n
        batch_num += 1
        if batch_num % 20 == 0 or inserted >= row_count:
            print(f"  inserted {inserted}/{row_count}", flush=True)

    res = mariadb_run_sql(base, f"SELECT COUNT(*) FROM `{schema}`.`{table}`;", env)
    print(f"  source row count: {res.stdout.strip()}")


def mssql_bulk_sql(db: str, schema: str, table: str, run_id: str, row_count: int) -> str:
    # id is NOT NULL and not IDENTITY on dev table — allocate from MAX(id)+1
    return f"""
USE [{db}];
SET NOCOUNT ON;
DECLARE @start INT = (SELECT ISNULL(MAX([id]), 0) FROM [{schema}].[{table}]);
INSERT INTO [{schema}].[{table}] ([id], [name])
SELECT @start + n, CONCAT('stress-{run_id}-s-', n)
FROM (
    SELECT TOP ({row_count}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
    CROSS JOIN sys.all_objects c
) seq;
SELECT COUNT(*) AS source_rows FROM [{schema}].[{table}];
SELECT COUNT(*) AS stress_rows FROM [{schema}].[{table}] WHERE [name] LIKE 'stress-{run_id}-s-%';
"""


def mssql_run_sql(sqlcmd: list[str], sql: str) -> subprocess.CompletedProcess[str]:
    """Run SQL via sqlcmd -Q (statement is compact set-based INSERT)."""
    return run_cmd(sqlcmd + ["-Q", sql], check=False, log_cmd=f"{' '.join(sqlcmd)} -Q <sql>")


def mssql_insert_failed(output: str) -> bool:
    return any(
        token in output
        for token in ("Msg 515", "Msg 2628", "Msg 547", "Level 16", "INSERT fails", "terminated")
    )


def mssql_insert(
    sqlcmd: list[str],
    db: str,
    schema: str,
    table: str,
    run_id: str,
    row_count: int,
    batch_size: int,
) -> None:
    del batch_size  # MSSQL uses single set-based INSERT
    sql = mssql_bulk_sql(db, schema, table, run_id, row_count)
    res = mssql_run_sql(sqlcmd, sql)
    combined = (res.stdout or "") + (res.stderr or "")
    if res.returncode != 0 or mssql_insert_failed(combined):
        print(combined, file=sys.stderr)
        raise SystemExit("MSSQL bulk insert failed")
    print(res.stdout.strip())


def mongo_batch_js(db: str, coll: str, run_id: str, row_count: int, batch_size: int) -> str:
    return f"""
const dbName = {db!r};
const collName = {coll!r};
const runId = {run_id!r};
const total = {row_count};
const batchSize = {batch_size};
const col = db.getSiblingDB(dbName).getCollection(collName);
let inserted = 0;
while (inserted < total) {{
  const n = Math.min(batchSize, total - inserted);
  const docs = [];
  for (let i = 0; i < n; i++) {{
    const seq = inserted + i + 1;
    docs.push({{ name: `stress-${{runId}}-o-${{seq}}`, tier: 'bronze', stress_run: runId, seq: seq }});
  }}
  col.insertMany(docs, {{ ordered: false }});
  inserted += n;
  if (inserted % (batchSize * 20) === 0 || inserted >= total) {{
    print(`inserted ${{inserted}}/${{total}}`);
  }}
}}
print('source count', col.countDocuments({{ stress_run: runId }}));
"""


def mongo_insert(uri: str, db: str, coll: str, run_id: str, row_count: int, batch_size: int) -> None:
    js = mongo_batch_js(db, coll, run_id, row_count, batch_size)
    res = run_cmd(["mongosh", "--quiet", uri, "--eval", js], check=False)
    if res.returncode != 0:
        print(res.stderr or res.stdout, file=sys.stderr)
        raise SystemExit("MongoDB insert failed")
    print(res.stdout.strip())


def main() -> None:
    p = argparse.ArgumentParser(description="CDC throughput bulk insert (MariaDB → MSSQL → Mongo)")
    p.add_argument("--row-count", type=int, default=int(os.environ.get("ROW_COUNT", "1000000")))
    p.add_argument("--batch-size", type=int, default=int(os.environ.get("BATCH_SIZE", "5000")))
    p.add_argument("--run-id", default=os.environ.get("STRESS_RUN_ID", time.strftime("%Y%m%d_%H%M%S")))
    p.add_argument("--engines", default=os.environ.get("ENGINES", "mariadb,mssql,mongo"))

    p.add_argument("--mariadb-host", default=os.environ.get("MARIADB_HOST", "127.0.0.1"))
    p.add_argument("--mariadb-port", type=int, default=int(os.environ.get("MARIADB_PORT", "3306")))
    p.add_argument("--mariadb-user", default=os.environ.get("MARIADB_USER", "tomy.berrios"))
    p.add_argument("--mariadb-password", default=os.environ.get("MARIADB_PASSWORD", "Yucaquemada1"))
    p.add_argument("--mariadb-schema", default=os.environ.get("MARIADB_SCHEMA", "datasync_test"))
    p.add_argument("--mariadb-table", default=os.environ.get("MARIADB_TABLE", "test"))

    p.add_argument("--mssql-mode", choices=("docker", "host"), default=os.environ.get("MSSQL_MODE", "docker"))
    p.add_argument("--mssql-container", default=os.environ.get("MSSQL_CONTAINER", "datalake-mssql"))
    p.add_argument("--mssql-host", default=os.environ.get("MSSQL_HOST", "localhost"))
    p.add_argument("--mssql-user", default=os.environ.get("MSSQL_USER", "sa"))
    p.add_argument("--mssql-password", default=os.environ.get("MSSQL_PASSWORD", "Yucaquemada1"))
    p.add_argument("--mssql-db", default=os.environ.get("MSSQL_DB", "testing"))
    p.add_argument("--mssql-schema", default=os.environ.get("MSSQL_SCHEMA", "user"))
    p.add_argument("--mssql-table", default=os.environ.get("MSSQL_TABLE", "costumers"))

    p.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0"))
    p.add_argument("--mongo-db", default=os.environ.get("MONGO_DB", "cdc_test"))
    p.add_argument("--mongo-coll", default=os.environ.get("MONGO_COLL", "users"))

    args = p.parse_args()
    engines = {e.strip().lower() for e in args.engines.split(",") if e.strip()}
    timings: dict[str, float] = {}

    print(f"STRESS_RUN_ID={args.run_id}")
    print(f"ROW_COUNT={args.row_count} BATCH_SIZE={args.batch_size}")
    print(f"Engines (sequential): {', '.join(e for e in ('mariadb', 'mssql', 'mongo') if e in engines)}")

    if "mariadb" in engines:
        def mariadb_phase() -> None:
            mariadb_batch_insert(
                args.mariadb_host,
                args.mariadb_port,
                args.mariadb_user,
                args.mariadb_password,
                args.mariadb_schema,
                args.mariadb_table,
                args.run_id,
                args.row_count,
                args.batch_size,
            )

        timings["mariadb"] = timed(
            f"MariaDB INSERT {args.row_count} → {args.mariadb_schema}.{args.mariadb_table}",
            mariadb_phase,
        )

    if "mssql" in engines:
        if args.mssql_mode == "docker":
            sqlcmd = [
                "docker",
                "exec",
                args.mssql_container,
                "/opt/mssql-tools18/bin/sqlcmd",
                "-S",
                "localhost",
                "-U",
                args.mssql_user,
                "-P",
                args.mssql_password,
                "-C",
            ]
        else:
            sqlcmd = [
                "sqlcmd",
                "-S",
                args.mssql_host,
                "-U",
                args.mssql_user,
                "-P",
                args.mssql_password,
                "-C",
            ]

        def mssql_phase() -> None:
            mssql_insert(
                sqlcmd,
                args.mssql_db,
                args.mssql_schema,
                args.mssql_table,
                args.run_id,
                args.row_count,
                args.batch_size,
            )

        timings["mssql"] = timed(
            f"MSSQL INSERT {args.row_count} → {args.mssql_db}.{args.mssql_schema}.{args.mssql_table}",
            mssql_phase,
        )

    if "mongo" in engines:
        def mongo_phase() -> None:
            mongo_insert(
                args.mongo_uri,
                args.mongo_db,
                args.mongo_coll,
                args.run_id,
                args.row_count,
                args.batch_size,
            )

        timings["mongo"] = timed(
            f"Mongo INSERT {args.row_count} → {args.mongo_db}.{args.mongo_coll}",
            mongo_phase,
        )

    print("\n== Summary ==")
    total = sum(timings.values())
    for eng, sec in timings.items():
        rps = args.row_count / sec if sec > 0 else 0
        print(f"  {eng}: {sec:.1f}s ({rps:,.0f} rows/s insert)")
    print(f"  total insert wall time: {total:.1f}s")
    print(f"\nSTRESS_RUN_ID={args.run_id}")
    print("CDC catch-up runs in DataSync daemon — check apply_batch_stats after lag drains.")


if __name__ == "__main__":
    main()
