#!/usr/bin/env bash
# DataSync container entrypoint — wait for deps, optional schema, run CLI.
set -euo pipefail

CONFIG="${DATASYNC_CONFIG:-/app/config.json}"
ROOT="${DATASYNC_ROOT:-/app}"
BIN="${DATASYNC_BIN:-/usr/local/bin/DataSync}"
SCHEMA_SQL="$ROOT/sql/backup/cdc_catalog_schema_structure.sql"
LAKE_SCHEMA_SQL="$ROOT/sql/backup/datalake_lake_schema.sql"
SEED_SQL="$ROOT/sql/backup/cdc_catalog_seed.sql"
MIGRATIONS_DIR="$ROOT/sql/migrations"
BOOTSTRAP_PY="$ROOT/docker/catalog_bootstrap.py"
VERIFY_SOURCES_PY="$ROOT/docker/verify_sources.py"
QUIET="${DATASYNC_INSTALL_QUIET:-0}"

log() { [[ "$QUIET" == "1" ]] || printf '==> %s\n' "$*" >&2; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
fail() { printf '✖ %s\n' "$*" >&2; exit 1; }
health_fail() { printf '✖ %s\n' "$*" >&2; }
ok() { printf '✔ %s\n' "$*"; }

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

verify_pg_auth() {
  local host="$1" port="$2" user="$3"
  refresh_pg_collation_versions "$host" "$port" "$user"
  if ! psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=1 -q -tAc "SELECT 1" >/dev/null 2>&1; then
    fail "PostgreSQL auth failed for ${user}@${host}:${port} (check config.json → datasync)"
  fi
}

warn_createdb_privilege() {
  local host="$1" port="$2" user="$3"
  local can
  can=$(psql -h "$host" -p "$port" -U "$user" -d postgres -tAc \
    "SELECT has_database_privilege('${user}', 'CREATEDB')" 2>/dev/null || echo "f")
  if [[ "$can" != "t" ]]; then
    warn "PostgreSQL user ${user} lacks CREATEDB — pre-create datasync/datalake databases or grant CREATEDB"
  fi
}

pg_database_exists() {
  local host="$1" port="$2" user="$3" db="$4"
  psql -h "$host" -p "$port" -U "$user" -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '${db}'" 2>/dev/null | grep -q 1
}

ensure_pg_database() {
  local host="$1" port="$2" db="$3" user="$4"
  refresh_pg_collation_versions "$host" "$port" "$user"

  if pg_database_exists "$host" "$port" "$user" "$db"; then
    printf '0'
    return 0
  fi

  log "Creating database ${db}"
  if ! psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=1 -q \
      -c "CREATE DATABASE \"${db}\" OWNER \"${user}\""; then
    fail "CREATE DATABASE ${db} failed — run as postgres superuser: ALTER DATABASE template1 REFRESH COLLATION VERSION; (then re-run ./install.sh)"
  fi

  if ! pg_database_exists "$host" "$port" "$user" "$db"; then
    fail "database ${db} still missing after CREATE DATABASE"
  fi

  psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=0 -q \
    -c "ALTER DATABASE \"${db}\" REFRESH COLLATION VERSION;" 2>/dev/null || true
  printf '1'
}

catalog_schema_exists() {
  local host="$1" port="$2" user="$3" db="$4"
  [[ "$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
    "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cdc_catalog'" 2>/dev/null || true)" == "1" ]]
}

apply_catalog_schema() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
  [[ -f "$SCHEMA_SQL" ]] || fail "missing $(basename "$SCHEMA_SQL")"
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_pg_cfg); then
    fail "cdc_catalog: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"

  local db_created
  db_created="$(ensure_pg_database "${pg[0]}" "${pg[1]}" "${pg[2]}" "${pg[3]}")"

  if catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    if [[ "$QUIET" == "1" ]]; then
      ok "cdc_catalog @ ${pg[2]} (exists)"
    else
      log "cdc_catalog @ ${pg[2]} already exists — skip DDL"
    fi
    return 0
  fi

  log "Applying cdc_catalog → ${pg[2]}"
  psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" \
    -v ON_ERROR_STOP=1 -q -f "$SCHEMA_SQL"
  if [[ "$QUIET" == "1" ]]; then
    if [[ "$db_created" == "1" ]]; then
      ok "cdc_catalog @ ${pg[2]} (created db + schema)"
    else
      ok "cdc_catalog @ ${pg[2]} (applied)"
    fi
  fi
}

apply_lake_schema() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
  [[ -f "$LAKE_SCHEMA_SQL" ]] || fail "missing $(basename "$LAKE_SCHEMA_SQL")"
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_datalake_cfg); then
    fail "lake schema: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"

  local db_created
  db_created="$(ensure_pg_database "${pg[0]}" "${pg[1]}" "${pg[2]}" "${pg[3]}")"

  local exists
  exists=$(psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" -tAc \
    "SELECT 1 FROM pg_proc p
     JOIN pg_namespace n ON n.oid = p.pronamespace
     WHERE n.nspname = 'lake' AND p.proname = 'ensure_monthly_partitions'" 2>/dev/null || true)

  if [[ "$exists" == "1" ]]; then
    if [[ "$QUIET" == "1" ]]; then
      ok "lake @ ${pg[2]} (exists)"
    else
      log "lake @ ${pg[2]} already exists — skip"
    fi
    return 0
  fi

  log "Applying lake schema → ${pg[2]}"
  psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" \
    -v ON_ERROR_STOP=1 -q -f "$LAKE_SCHEMA_SQL"
  if [[ "$QUIET" == "1" ]]; then
    if [[ "$db_created" == "1" ]]; then
      ok "lake @ ${pg[2]} (created db + schema)"
    else
      ok "lake @ ${pg[2]} (applied)"
    fi
  fi
}

verify_catalog_integrity() {
  local host="$1" port="$2" user="$3" db="$4"
  local missing=""
  local tbl
  for tbl in catalog logs connections runtime_config; do
    local found
    found=$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
      "SELECT 1 FROM information_schema.tables
       WHERE table_schema = 'cdc_catalog' AND table_name = '${tbl}'" 2>/dev/null || true)
    if [[ "$found" != "1" ]]; then
      missing="${missing} cdc_catalog.${tbl}"
    fi
  done

  if [[ -n "$missing" ]]; then
    fail "partial cdc_catalog schema — missing:${missing} (re-run with DATASYNC_RUN_MIGRATIONS=1 or restore from backup SQL)"
  fi
}

check_pg_auth() {
  local host="$1" port="$2" user="$3"
  refresh_pg_collation_versions "$host" "$port" "$user"
  psql -h "$host" -p "$port" -U "$user" -d postgres -v ON_ERROR_STOP=1 -q -tAc "SELECT 1" >/dev/null 2>&1
}

check_catalog_integrity() {
  local host="$1" port="$2" user="$3" db="$4"
  local tbl found
  for tbl in catalog logs connections runtime_config; do
    found=$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
      "SELECT 1 FROM information_schema.tables
       WHERE table_schema = 'cdc_catalog' AND table_name = '${tbl}'" 2>/dev/null || true)
    [[ "$found" == "1" ]] || return 1
  done
}

ensure_schema_migrations_table() {
  local host="$1" port="$2" user="$3" db="$4"
  psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE TABLE IF NOT EXISTS cdc_catalog.schema_migrations (
    version integer NOT NULL PRIMARY KEY,
    description text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);
SQL
  local count
  count=$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
    "SELECT COUNT(*) FROM cdc_catalog.schema_migrations" 2>/dev/null || echo "0")
  if [[ "$count" == "0" ]]; then
    psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q \
      -c "INSERT INTO cdc_catalog.schema_migrations (version, description)
          VALUES (1, 'baseline cdc_catalog_schema_structure')
          ON CONFLICT (version) DO NOTHING"
  fi
}

apply_pending_migrations() {
  local host="$1" port="$2" user="$3" db="$4"
  [[ -d "$MIGRATIONS_DIR" ]] || return 0

  shopt -s nullglob
  local files=("$MIGRATIONS_DIR"/*.sql)
  shopt -u nullglob
  [[ ${#files[@]} -gt 0 ]] || return 0

  local f base ver applied desc
  for f in $(printf '%s\n' "${files[@]}" | sort); do
    base=$(basename "$f")
    ver="${base%%_*}"
    if [[ ! "$ver" =~ ^[0-9]+$ ]]; then
      warn "skip migration ${base} (filename must start with numeric version)"
      continue
    fi
    applied=$(psql -h "$host" -p "$port" -U "$user" -d "$db" -tAc \
      "SELECT 1 FROM cdc_catalog.schema_migrations WHERE version = ${ver}" 2>/dev/null || true)
    [[ "$applied" == "1" ]] && continue

    log "Applying migration ${base}"
    psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q -f "$f"
    desc="${base#${ver}_}"
    desc="${desc%.sql}"
    psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q \
      -c "INSERT INTO cdc_catalog.schema_migrations (version, description)
          VALUES (${ver}, '${desc//\'/''}')
          ON CONFLICT (version) DO NOTHING"
  done
}

apply_catalog_seed() {
  local host="$1" port="$2" user="$3" db="$4"
  [[ -f "$SEED_SQL" ]] || return 0
  log "Applying runtime_config seed"
  psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q -f "$SEED_SQL"
}

sync_connections_from_config() {
  [[ -f "$CONFIG" ]] || return 0
  [[ -f "$BOOTSTRAP_PY" ]] || return 0
  if ! mapfile -t pg < <(read_pg_cfg); then
    return 0
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"
  if ! catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    return 0
  fi

  local out rc
  out="$(mktemp)"
  if [[ "$QUIET" == "1" ]]; then
    if python3 "$BOOTSTRAP_PY" >"$out" 2>&1; then
      rc=0
    else
      rc=1
    fi
  else
    log "Seeding cdc_catalog.connections"
    if python3 "$BOOTSTRAP_PY" >"$out" 2>&1; then
      rc=0
    else
      rc=1
    fi
  fi

  if [[ "$rc" -eq 0 ]]; then
    if grep -q 'connections seeded/updated:' "$out"; then
      if [[ "$QUIET" == "1" ]]; then
        ok "$(grep 'connections seeded/updated:' "$out" | head -1)"
      else
        grep 'connections seeded/updated:' "$out" | head -1 >&2 || true
      fi
    fi
  else
    warn "connections bootstrap failed"
    tail -5 "$out" >&2 || true
  fi
  rm -f "$out"
  return "$rc"
}

post_schema_bootstrap() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_pg_cfg); then
    fail "post-schema bootstrap: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"

  if ! catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    fail "cdc_catalog schema missing after apply — check PostgreSQL logs"
  fi

  verify_catalog_integrity "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
  ensure_schema_migrations_table "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
  apply_pending_migrations "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
  apply_catalog_seed "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
  patch_kafka_bootstrap
}

patch_kafka_bootstrap() {
  [[ -n "${KAFKA_BOOTSTRAP:-}" ]] || return 0
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_pg_cfg); then
    return 0
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"

  if ! pg_database_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    return 0
  fi

  if ! catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    return 0
  fi

  local json_val
  json_val=$(python3 -c "import json; print(json.dumps('${KAFKA_BOOTSTRAP}'))")

  psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" -v ON_ERROR_STOP=1 -q <<SQL
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES ('kafka_bootstrap_servers', 'cdc_kafka_apply', '', '${json_val}'::jsonb, 'Kafka bootstrap (install default)')
ON CONFLICT (config_key, component, conn_id)
DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = now();
SQL
  [[ "$QUIET" == "1" ]] || log "runtime_config kafka_bootstrap_servers=${KAFKA_BOOTSTRAP}"
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
    ok "PostgreSQL auth ${pg[3]}@${pg[0]}:${pg[1]}"
  else
    health_fail "PostgreSQL auth ${pg[3]}@${pg[0]}:${pg[1]}"
    failed=1
  fi

  if catalog_schema_exists "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}" \
      && check_catalog_integrity "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"; then
    ok "cdc_catalog integrity @ ${pg[2]}"
  else
    health_fail "cdc_catalog integrity @ ${pg[2]}"
    failed=1
  fi

  local conn_count
  conn_count=$(psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" -tAc \
    "SELECT COUNT(*) FROM cdc_catalog.connections WHERE active" 2>/dev/null || echo "")
  if [[ -n "$conn_count" ]]; then
    if [[ "$conn_count" == "0" ]]; then
      ok "connections active=0 (add sources later)"
    else
      ok "connections active=${conn_count}"
    fi
  else
    health_fail "connections query failed"
    failed=1
  fi

  if mapfile -t dl < <(read_datalake_cfg); then
    export PGPASSWORD="${dl[4]}"
    apply_pg_sslmode "${dl[5]:-}"
    if lake_schema_ok "${dl[0]}" "${dl[1]}" "${dl[3]}" "${dl[2]}"; then
      ok "lake schema @ ${dl[2]}"
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
      ok "Kafka TCP ${KAFKA_BOOTSTRAP}"
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

if [[ "${1:-}" == "verify-sources" ]]; then
  [[ -f "$CONFIG" ]] || fail "verify-sources: missing config.json"
  if ! mapfile -t pg < <(read_pg_cfg); then
    fail "verify-sources: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"
  sync_connections_from_config || true
  python3 "$VERIFY_SOURCES_PY"
  exit $?
fi

if [[ -f "$CONFIG" ]] && [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]]; then
  if mapfile -t pg < <(read_pg_cfg); then
    export PGPASSWORD="${pg[4]}"
    apply_pg_sslmode "${pg[5]:-}"
    warn_createdb_privilege "${pg[0]}" "${pg[1]}" "${pg[3]}"
    verify_pg_auth "${pg[0]}" "${pg[1]}" "${pg[3]}"
    if [[ "$QUIET" == "1" ]]; then
      ok "PostgreSQL ${pg[3]}@${pg[0]}:${pg[1]}"
    else
      log "PostgreSQL auth ok"
    fi
  else
    warn "could not read PostgreSQL settings from $CONFIG"
  fi
elif [[ -f "$CONFIG" ]]; then
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
fi

apply_catalog_schema
apply_lake_schema
post_schema_bootstrap
sync_connections_from_config

if [[ "${1:-}" == "schema-only" ]]; then
  exit 0
fi

cd "$ROOT"
exec "$BIN" "$@"
