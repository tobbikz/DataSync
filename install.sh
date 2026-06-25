#!/usr/bin/env bash
# Host:      ./install.sh | ./install.sh start  → build + Kafka + daemon (+ discover opcional)
#             ./install.sh kafka-retention [TOPIC_PREFIX=…]  → alter existing topic retention
# Migration: ./install.sh stop; rsync -a kafka-data/ newhost:…/DataSync/kafka-data/; ./install.sh start
#            (keep CLUSTER_ID in docker-compose.yml unchanged on the target host)
# Systemd:   ExecStart=$ROOT/install.sh start  ExecStop=$ROOT/install.sh stop
#            sudo ./install.sh systemd-install  (once — installs DataSync.service)
# Container: install.sh container …            (Docker ENTRYPOINT only)
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

apply_catalog_schema() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
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

  log "Applying cdc_catalog baseline → ${pg[2]}"
  "$BIN" migrate --baseline --config "$CONFIG"
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
  "$BIN" migrate --lake --config "$CONFIG"
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

apply_catalog_incremental() {
  log "Applying schema incremental (DataSync migrate)"
  "$BIN" migrate --config "$CONFIG"
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
  apply_catalog_incremental
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
  exec "$BIN" migrate --diagnostics --config "$CONFIG"
fi

cd "$ROOT"
exec "$BIN" "$@"

}

ensure_kafka_data_dir() {
  local dir="${DATASYNC_KAFKA_DATA:-$ROOT/kafka-data}"
  local created=0

  [[ -d "$dir" ]] || created=1

  if ! mkdir -p "$dir" 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1 && sudo mkdir -p "$dir" 2>/dev/null; then
      :
    else
      warn "could not create ${dir}"
      return 1
    fi
  fi

  if [[ "$created" == "1" ]]; then
    printf '✔ created Kafka data dir: %s\n' "$dir"
  fi

  # cp-kafka runs as appuser (uid 1000); world-writable bind mount works across systemd/podman/SELinux.
  if ! chmod 777 "$dir" 2>/dev/null; then
    if command -v sudo >/dev/null 2>&1 && sudo chmod 777 "$dir" 2>/dev/null; then
      :
    else
      warn "could not chmod 777 ${dir}"
      return 1
    fi
  fi
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

host_build_datasync() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  docker_compose build datasync
}

host_stack_start() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  export DATASYNC_KAFKA_DATA="${DATASYNC_KAFKA_DATA:-$ROOT/kafka-data}"
  ensure_kafka_data_dir
  host_build_datasync
  docker_compose up -d kafka
  wait_kafka_compose || exit 1
  docker_compose up -d --force-recreate --remove-orphans datasync
  if [[ "${DATASYNC_AUTO_DISCOVER:-1}" == "1" ]]; then
    run_host_discover || true
  fi
  docker_compose ps
}

host_stack_stop() {
  ensure_container_runtime || exit 0
  cd "$ROOT"
  docker_compose stop datasync kafka 2>/dev/null || true
}

kafka_set_topic_retention() {
  ensure_container_runtime || exit 1
  cd "$ROOT"
  local bootstrap="${KAFKA_BOOTSTRAP:-127.0.0.1:9092}"
  local retention_ms="${KAFKA_RETENTION_MS:-259200000}"
  local retention_bytes="${KAFKA_RETENTION_BYTES:-1073741824}"
  local segment_bytes="${KAFKA_SEGMENT_BYTES:-134217728}"
  local topic_prefix="${TOPIC_PREFIX:-}"
  local config="retention.ms=${retention_ms},retention.bytes=${retention_bytes},segment.bytes=${segment_bytes},cleanup.policy=delete"
  local -a kafka_topics kafka_configs

  if docker_compose ps kafka --format '{{.State}}' 2>/dev/null | grep -qi running; then
    kafka_topics=(docker_compose exec -T kafka kafka-topics --bootstrap-server "$bootstrap")
    kafka_configs=(docker_compose exec -T kafka kafka-configs --bootstrap-server "$bootstrap")
  elif command -v kafka-topics >/dev/null 2>&1; then
    kafka_topics=(kafka-topics --bootstrap-server "$bootstrap")
    kafka_configs=(kafka-configs --bootstrap-server "$bootstrap")
  else
    warn "kafka-topics not found — start Kafka (./install.sh start) or install Kafka CLI"
    exit 1
  fi

  mapfile -t topics < <("${kafka_topics[@]}" --list 2>/dev/null | sort)
  if [[ ${#topics[@]} -eq 0 ]]; then
    warn "no topics at ${bootstrap}"
    exit 1
  fi

  local count=0 topic
  for topic in "${topics[@]}"; do
    [[ -n "$topic" ]] || continue
    if [[ -n "$topic_prefix" && "$topic" != "${topic_prefix}"* ]]; then
      continue
    fi
    [[ "$topic" == __* ]] && continue
    printf '==> alter %s -> %s\n' "$topic" "$config" >&2
    "${kafka_configs[@]}" \
      --entity-type topics \
      --entity-name "$topic" \
      --alter \
      --add-config "$config"
    count=$((count + 1))
  done
  ok "kafka retention: ${count} topic(s) updated"
  printf '  disk may not shrink until log cleaner runs (~5m)\n' >&2
}

install_systemd_unit() {
  local unit_src="$ROOT/DataSync.service"
  local unit_dst="/etc/systemd/system/DataSync.service"
  [[ -f "$unit_src" ]] || fail "missing DataSync.service in $ROOT"
  if [[ "${EUID}" -ne 0 ]]; then
    fail "systemd-install requires root — run: sudo $ROOT/install.sh systemd-install"
  fi
  sed "s|@DATASYNC_ROOT@|$ROOT|g" "$unit_src" >"${unit_dst}.tmp"
  install -m 644 "${unit_dst}.tmp" "$unit_dst"
  rm -f "${unit_dst}.tmp"
  systemctl daemon-reload
  printf '✔ installed %s (ExecStart=%s/install.sh start)\n' "$unit_dst" "$ROOT"
  printf '  enable: systemctl enable --now DataSync\n'
}

case "${1:-}" in
  container)
    shift
    container_dispatch "$@"
    ;;
  start|"")
    host_stack_start
    ;;
  stop)
    host_stack_stop
    ;;
  systemd-install)
    install_systemd_unit
    ;;
  kafka-retention)
    kafka_set_topic_retention
    ;;
  *)
    warn "usage: ./install.sh [start|stop|kafka-retention|systemd-install]  |  install.sh container <cmd>"
    exit 2
    ;;
esac
