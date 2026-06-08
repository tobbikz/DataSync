#!/usr/bin/env bash
# DataSync container entrypoint — wait for deps, optional schema, run CLI.
set -euo pipefail

CONFIG="${DATASYNC_CONFIG:-/app/config.json}"
ROOT="${DATASYNC_ROOT:-/app}"
BIN="${DATASYNC_BIN:-/usr/local/bin/DataSync}"
SCHEMA_SQL="$ROOT/sql/backup/cdc_catalog_schema_structure.sql"

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }

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
for val in (host, ds["port"], ds["database"], ds["user"], ds["password"]):
    print(val)
PY
}

wait_tcp() {
  local host="$1" port="$2" label="${3:-$host:$port}"
  for _ in $(seq 1 60); do
    if (echo >/dev/tcp/"$host"/"$port") 2>/dev/null; then
      log "$label ready"
      return 0
    fi
    sleep 2
  done
  warn "timeout waiting for $label"
  return 1
}

apply_catalog_schema() {
  [[ "${DATASYNC_RUN_MIGRATIONS:-0}" == "1" ]] || return 0
  [[ -f "$SCHEMA_SQL" ]] || { warn "schema file missing — skip"; return 0; }
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_pg_cfg); then
    warn "schema apply skipped — config unreadable"
    return 0
  fi
  export PGPASSWORD="${pg[4]}"

  local exists
  exists=$(psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" -tAc \
    "SELECT 1 FROM information_schema.schemata WHERE schema_name = 'cdc_catalog'" 2>/dev/null || true)

  if [[ "$exists" == "1" ]]; then
    log "cdc_catalog already exists — skip schema apply"
    return 0
  fi

  log "Applying catalog schema → ${pg[2]}@${pg[0]}:${pg[1]}"
  psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" \
    -v ON_ERROR_STOP=1 -f "$SCHEMA_SQL"
}

patch_kafka_bootstrap() {
  [[ -n "${KAFKA_BOOTSTRAP:-}" ]] || return 0
  [[ -f "$CONFIG" ]] || return 0

  if ! mapfile -t pg < <(read_pg_cfg); then
    warn "patch_kafka_bootstrap skipped — config unreadable"
    return 0
  fi
  export PGPASSWORD="${pg[4]}"
  local json_val
  json_val=$(python3 -c "import json; print(json.dumps('${KAFKA_BOOTSTRAP}'))")

  psql -h "${pg[0]}" -p "${pg[1]}" -U "${pg[3]}" -d "${pg[2]}" -v ON_ERROR_STOP=1 -q <<SQL
INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
VALUES ('kafka_bootstrap_servers', 'cdc_kafka_apply', '', '${json_val}'::jsonb, 'Kafka bootstrap (docker)')
ON CONFLICT (config_key, component, conn_id)
DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = now();
SQL
  log "runtime_config kafka_bootstrap_servers=${KAFKA_BOOTSTRAP}"
}

if [[ -f "$CONFIG" ]]; then
  if mapfile -t pg < <(read_pg_cfg); then
    wait_tcp "${pg[0]}" "${pg[1]}" "PostgreSQL" || true
  else
    warn "could not read PostgreSQL settings from $CONFIG"
  fi
fi

if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
  wait_tcp "${KAFKA_BOOTSTRAP%%:*}" "${KAFKA_BOOTSTRAP##*:}" "Kafka" || true
fi

apply_catalog_schema
patch_kafka_bootstrap

if [[ "${1:-}" == "schema-only" ]]; then
  log "schema-only complete"
  exit 0
fi

cd "$ROOT"
exec "$BIN" "$@"
