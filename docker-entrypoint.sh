#!/usr/bin/env bash
# Docker/Podman ENTRYPOINT for the datasync image — not for host use.
# Host ops: ./install.sh initial | start | stop
set -euo pipefail

CONFIG="${DATASYNC_CONFIG:-/app/config.json}"
ROOT="${DATASYNC_ROOT:-/app}"
BIN="${DATASYNC_BIN:-/usr/local/bin/DataSync}"
QUIET="${DATASYNC_INSTALL_QUIET:-0}"

log() { [[ "$QUIET" == "1" ]] || printf '==> %s\n' "$*" >&2; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
fail() { printf '✖ %s\n' "$*" >&2; exit 1; }
health_fail() { printf '✖ %s\n' "$*" >&2; }

read_pg_cfg() {
  python3 - "$CONFIG" <<'PY'
import json, sys, os
with open(sys.argv[1]) as f:
    ds = json.load(f)["datasync"]
host = ds["host"]
if (os.path.exists("/.dockerenv")
        and os.environ.get("DATASYNC_HOST_NETWORK") != "1"
        and host in ("localhost", "127.0.0.1")):
    host = "host.docker.internal"
sslmode = ds.get("sslmode", os.environ.get("DATASYNC_PG_SSLMODE", ""))
for val in (host, ds["port"], ds["database"], ds["user"], ds["password"], sslmode):
    print(val)
PY
}

read_datalake_cfg() {
  python3 - "$CONFIG" <<'PY'
import json, sys, os
with open(sys.argv[1]) as f:
    root = json.load(f)
ds = root["datasync"]
if root.get("datalake") and isinstance(root["datalake"], dict):
    dl = root["datalake"]
    host = dl.get("host", ds["host"])
    port = dl.get("port", ds["port"])
    database = dl.get("database", ds["database"])
    user = dl.get("user", ds["user"])
    password = dl.get("password", ds["password"])
    sslmode = dl.get("sslmode", ds.get("sslmode", ""))
else:
    host = ds["host"]
    port = ds["port"]
    database = ds.get("database", "DataLake")
    user = ds["user"]
    password = ds["password"]
    sslmode = ds.get("sslmode", "")
if (os.path.exists("/.dockerenv")
        and os.environ.get("DATASYNC_HOST_NETWORK") != "1"
        and host in ("localhost", "127.0.0.1")):
    host = "host.docker.internal"
if not sslmode:
    sslmode = os.environ.get("DATALAKE_PG_SSLMODE", os.environ.get("DATASYNC_PG_SSLMODE", ""))
for val in (host, port, database, user, password, sslmode):
    print(val)
PY
}

apply_pg_sslmode() {
  local sslmode="${1:-}"
  if [[ -n "$sslmode" ]]; then
    export PGSSLMODE="$sslmode"
  else
    unset PGSSLMODE 2>/dev/null || true
  fi
}

refresh_pg_collation_versions() {
  local host="$1" port="$2" user="$3"
  local db
  for db in template1 postgres template0; do
    psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=0 -q \
      -c "ALTER DATABASE ${db} REFRESH COLLATION VERSION;" 2>/dev/null || true
  done
}

check_pg_auth() {
  local host="$1" port="$2" user="$3"
  refresh_pg_collation_versions "$host" "$port" "$user"
  psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=1 -q -tAc "SELECT 1" >/dev/null 2>&1
}

check_catalog_integrity() {
  local host="$1" port="$2" user="$3" db="$4"
  local tbl found
  for tbl in catalog logs connections apply_position; do
    found=$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
      "SELECT 1 FROM information_schema.tables
       WHERE table_schema = 'cdc_catalog' AND table_name = '${tbl}'" 2>/dev/null || true)
    [[ "$found" == "1" ]] || return 1
  done
}

catalog_schema_exists() {
  local host="$1" port="$2" user="$3" db="$4"
  [[ "$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
    "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cdc_catalog'" 2>/dev/null || true)" == "1" ]]
}

apply_lake_schema() {
  "$BIN" lake-schema-only
}

lake_schema_ok() {
  local host="$1" port="$2" user="$3" db="$4"
  [[ "$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
    "SELECT 1 FROM pg_proc p
     JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'lake' AND p.proname = 'ensure_monthly_partitions'" 2>/dev/null || true)" == "1" ]]
}

kafka_tcp_ok() {
  [[ -n "${KAFKA_BOOTSTRAP:-}" ]] || return 0
  local khost="${KAFKA_BOOTSTRAP%%:*}"
  local kport="${KAFKA_BOOTSTRAP##*:}"
  (echo >/dev/tcp/"$khost"/"$kport") 2>/dev/null
}

run_health_checks() {
  local failed=0
  [[ -f "$CONFIG" ]] || { fail "health-only: missing config.json"; }

  if ! mapfile -t pg < <(read_pg_cfg); then
    fail "health-only: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"

  if check_pg_auth "${pg[0]}" "${pg[1]}" "${pg[3]}"; then
    printf '✔ PostgreSQL auth %s@%s:%s\n' "${pg[3]}" "${pg[0]}" "${pg[1]}"
  else
    health_fail "PostgreSQL auth ${pg[3]}@${pg[0]}:${pg[1]}"
    failed=1
  fi

  if catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}" \
      && check_catalog_integrity "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    printf '✔ cdc_catalog integrity @ %s\n' "${pg[2]}"
  else
    health_fail "cdc_catalog integrity @ ${pg[2]}"
    failed=1
  fi

  if mapfile -t dl < <(read_datalake_cfg); then
    export PGPASSWORD="${dl[4]}"
    apply_pg_sslmode "${dl[5]:-}"
    if lake_schema_ok "${dl[0]}" "${dl[1]}" "${dl[3]}" "${dl[2]}"; then
      printf '✔ lake schema @ %s\n' "${dl[2]}"
    else
      health_fail "lake schema missing @ ${dl[2]}"
      failed=1
    fi
  else
    health_fail "datalake config unreadable"
    failed=1
  fi

  if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
    if kafka_tcp_ok; then
      printf '✔ Kafka TCP %s\n' "${KAFKA_BOOTSTRAP}"
    else
      health_fail "Kafka TCP ${KAFKA_BOOTSTRAP}"
      failed=1
    fi
  fi

  return "$failed"
}

if [[ "${1:-}" == "health-only" ]]; then
  run_health_checks
  exit $?
fi

if [[ -f "$CONFIG" ]]; then
  if mapfile -t pg < <(read_pg_cfg); then
    export PGPASSWORD="${pg[4]}"
    apply_pg_sslmode "${pg[5]:-}"
    for _ in $(seq 1 30); do
      if (echo >/dev/tcp/"${pg[0]}"/"${pg[1]}") 2>/dev/null; then
        break
      fi
      sleep 2
    done
  fi
fi

if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
  for _ in $(seq 1 30); do
    if kafka_tcp_ok; then
      break
    fi
    sleep 2
  done
  if ! kafka_tcp_ok; then
    fail "Kafka unreachable at ${KAFKA_BOOTSTRAP} — start stack first (./install.sh start)"
  fi
fi

if mapfile -t dl < <(read_datalake_cfg 2>/dev/null); then
  export PGPASSWORD="${dl[4]}"
  apply_pg_sslmode "${dl[5]:-}"
  apply_lake_schema
fi

if [[ "${1:-}" == "schema-only" ]]; then
  exit 0
fi

if [[ "${1:-}" == "diagnose-only" ]]; then
  fail "schema diagnostics are manual (app off + psql); binary has no migrate command"
fi

cd "$ROOT"
exec "$BIN" "$@"
