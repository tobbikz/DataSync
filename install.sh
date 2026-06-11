#!/usr/bin/env bash
# Host:      ./install.sh              → build + Kafka + daemon + discover
# Systemd:   ExecStart=$ROOT/install.sh  with  Environment=DATASYNC_FOREGROUND=1
# Container: install.sh container …    (Docker ENTRYPOINT only)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

DATASYNC_CONTAINER_ENGINE="${DATASYNC_CONTAINER_ENGINE:-}"

warn() { printf '✖ %s\n' "$*" >&2; }

_container_compose_try_start_podman() {
  command -v systemctl >/dev/null 2>&1 || return 0
  local uid="${DATASYNC_UID:-$(id -u)}"
  local runtime="${XDG_RUNTIME_DIR:-/run/user/${uid}}"
  export XDG_RUNTIME_DIR="${runtime}"
  export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${runtime}/bus}"
  systemctl --user start podman.socket >/dev/null 2>&1 || true
}

_container_compose_allow_sudo() {
  [[ -z "${INVOCATION_ID:-}" ]] || return 1
  [[ -t 0 ]] || return 1
  command -v sudo >/dev/null 2>&1 || return 1
  sudo -n true 2>/dev/null
}

_container_compose_podman_sock() {
  if [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
    printf '%s/podman/podman.sock' "$XDG_RUNTIME_DIR"
  else
    printf '/run/user/%s/podman/podman.sock' "$(id -u)"
  fi
}

ensure_container_runtime() {
  [[ -n "$DATASYNC_CONTAINER_ENGINE" ]] && return 0

  _container_compose_try_start_podman

  if [[ -n "${DOCKER_HOST:-}" ]] && command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=podman
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=podman
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  local sock
  sock="$(_container_compose_podman_sock)"
  if [[ -S "$sock" ]] && command -v podman >/dev/null 2>&1; then
    export DOCKER_HOST="unix://${sock}"
    if podman info >/dev/null 2>&1; then
      DATASYNC_CONTAINER_ENGINE=podman
      export DATASYNC_CONTAINER_ENGINE
      return 0
    fi
  fi

  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=docker
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  if _container_compose_allow_sudo && sudo docker info >/dev/null 2>&1; then
    DATASYNC_CONTAINER_ENGINE=docker_sudo
    export DATASYNC_CONTAINER_ENGINE
    return 0
  fi

  return 1
}

docker_compose() {
  if ! ensure_container_runtime; then
    warn "need podman or docker as $(id -un 2>/dev/null || echo datalake)"
    printf '  loginctl enable-linger datalake\n' >&2
    return 1
  fi

  case "$DATASYNC_CONTAINER_ENGINE" in
    docker) docker compose "$@" ;;
    docker_sudo) sudo docker compose "$@" ;;
    podman)
      if podman compose version >/dev/null 2>&1; then
        podman compose "$@"
      elif command -v docker-compose >/dev/null 2>&1; then
        docker-compose "$@"
      else
        warn "podman compose plugin required"
        return 1
      fi
      ;;
    *)
      warn "unknown container engine: $DATASYNC_CONTAINER_ENGINE"
      return 1
      ;;
  esac
}

# exec(1) cannot run shell functions — resolve real compose binary for foreground/systemd.
compose_exec_up() {
  ensure_container_runtime || exit 1
  case "$DATASYNC_CONTAINER_ENGINE" in
    docker) exec docker compose up --remove-orphans "$@" ;;
    docker_sudo) exec sudo docker compose up --remove-orphans "$@" ;;
    podman)
      if podman compose version >/dev/null 2>&1; then
        exec podman compose up --remove-orphans "$@"
      elif command -v docker-compose >/dev/null 2>&1; then
        exec docker-compose up --remove-orphans "$@"
      fi
      warn "podman compose plugin required"
      exit 1
      ;;
    *)
      warn "unknown container engine: $DATASYNC_CONTAINER_ENGINE"
      exit 1
      ;;
  esac
}

kafka_tcp_ok() {
  local bootstrap="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  local host="${bootstrap%%:*}" port="${bootstrap##*:}"
  [[ -n "$host" && -n "$port" ]] || return 1
  (echo >/dev/tcp/"$host"/"$port") 2>/dev/null
}

container_dispatch() {
  local CONFIG="${DATASYNC_CONFIG:-/app/config.json}"
  local ROOT="${DATASYNC_ROOT:-/app}"
  local BIN="${DATASYNC_BIN:-/usr/local/bin/DataSync}"
  local PROD_OPS_SQL="${DATASYNC_PROD_OPS_SQL:-$ROOT/prod_ops.sql}"
  local QUIET="${DATASYNC_INSTALL_QUIET:-0}"

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

apply_sql_section() {
  local section="$1" host="$2" port="$3" user="$4" db="$5"
  [[ -f "$PROD_OPS_SQL" ]] || fail "missing $(basename "$PROD_OPS_SQL")"
  local tmp
  tmp="$(mktemp)"
  awk -v sec="$section" '
    $0 ~ "^-- @section:" sec "$" {in_sec=1; next}
    /^-- @section:/ {if (in_sec) exit; next}
    in_sec {print}
  ' "$PROD_OPS_SQL" >"$tmp"
  if [[ ! -s "$tmp" ]]; then
    rm -f "$tmp"
    fail "empty section ${section} in $(basename "$PROD_OPS_SQL")"
  fi
  psql -h "$host" -p "$port" -U "$user" -d "$db" -v ON_ERROR_STOP=1 -q -f "$tmp"
  rm -f "$tmp"
}

apply_catalog_schema() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
  [[ -f "$PROD_OPS_SQL" ]] || fail "missing $(basename "$PROD_OPS_SQL")"
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
  apply_sql_section datasync_baseline "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
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
  [[ -f "$PROD_OPS_SQL" ]] || fail "missing $(basename "$PROD_OPS_SQL")"
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
  apply_sql_section datalake_lake "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
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

apply_catalog_incremental() {
  local host="$1" port="$2" user="$3" db="$4"
  log "Applying prod_ops incremental (migrations, seed, tune)"
  apply_sql_section datasync_incremental "$host" "$port" "$user" "$db"
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
  apply_catalog_incremental "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
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
VALUES ('kafka_bootstrap_servers', 'cdc_kafka_apply', '', '${json_val}'::jsonb, 'Kafka bootstrap fallback when KAFKA_BOOTSTRAP env unset')
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

  if command -v mariadb-binlog >/dev/null 2>&1 || command -v mysqlbinlog >/dev/null 2>&1; then
    ok "mariadb-binlog in PATH"
  else
    health_fail "mariadb-binlog missing (install mariadb-client for CDC capture)"
    failed=1
  fi

  return "$failed"
}

if [[ "${1:-}" == "health-only" ]]; then
  run_health_checks
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
  if ! kafka_tcp_ok; then
    fail "Kafka unreachable at ${KAFKA_BOOTSTRAP} — start kafka first (systemctl restart DataSync)"
  fi
fi

apply_catalog_schema
apply_lake_schema
post_schema_bootstrap

if [[ "${1:-}" == "schema-only" ]]; then
  exit 0
fi

if [[ "${1:-}" == "diagnose-only" ]]; then
  if ! mapfile -t pg < <(read_pg_cfg); then
    fail "diagnose-only: config unreadable"
  fi
  export PGPASSWORD="${pg[4]}"
  apply_pg_sslmode "${pg[5]:-}"
  apply_sql_section diagnostics "${pg[0]}" "${pg[1]}" "${pg[3]}" "${pg[2]}"
  exit 0
fi

cd "$ROOT"
exec "$BIN" "$@"

}

wait_kafka_compose() {
  local i state
  for i in $(seq 1 120); do
    if kafka_tcp_ok; then
      return 0
    fi
    state="$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)"
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      docker_compose logs kafka --tail 25 >&2 || true
      return 1
    fi
    sleep 2
  done
  warn "Kafka timeout on ${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  docker_compose logs kafka --tail 25 >&2 || true
  return 1
}

run_host_discover() {
  if ! kafka_tcp_ok; then
    warn "Kafka not ready — skipping discover"
    return 1
  fi
  docker_compose run --rm --no-deps \
    -e DATASYNC_RUN_MIGRATIONS="${DATASYNC_RUN_MIGRATIONS:-0}" \
    datasync discover || return 1
}

host_maybe_build() {
  if [[ "${DATASYNC_FORCE_BUILD:-0}" == "1" ]]; then
    docker_compose build datasync
    return
  fi
  # systemd restart: skip rebuild when image already exists (avoid rebuild loop on failure)
  if [[ -n "${INVOCATION_ID:-}" ]]; then
    case "$DATASYNC_CONTAINER_ENGINE" in
      docker) docker image inspect datasync:local >/dev/null 2>&1 && return 0 ;;
      docker_sudo) sudo docker image inspect datasync:local >/dev/null 2>&1 && return 0 ;;
      podman) podman image inspect datasync:local >/dev/null 2>&1 && return 0 ;;
    esac
  fi
  docker_compose build datasync
}

host_install_and_run() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  host_maybe_build

  if [[ "${DATASYNC_FOREGROUND:-0}" == "1" ]]; then
    docker_compose up -d kafka
    wait_kafka_compose || exit 1
    if [[ "${DATASYNC_AUTO_DISCOVER:-1}" == "1" ]]; then
      run_host_discover || true
    fi
    compose_exec_up
  fi

  docker_compose up -d --remove-orphans
  wait_kafka_compose || exit 1
  if [[ "${DATASYNC_AUTO_DISCOVER:-1}" == "1" ]]; then
    run_host_discover || true
  fi
  docker_compose ps
}

case "${1:-}" in
  container)
    shift
    container_dispatch "$@"
    ;;
  "")
    host_install_and_run
    ;;
  *)
    warn "usage: ./install.sh  |  install.sh container <cmd>"
    exit 2
    ;;
esac
