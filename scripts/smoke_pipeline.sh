#!/usr/bin/env bash
# Smoke: discover → full-load → capture → apply per active connection (MariaDB/MSSQL/MongoDB).
# Usage:
#   ./scripts/smoke_pipeline.sh [config.json]
#   SMOKE_TIER=bronze SMOKE_CONN_ID=MARIADB01 ./scripts/smoke_pipeline.sh
#
# Requires: built DataSync binary OR podman/docker compose; PG + sources reachable from config.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${1:-${DATASYNC_CONFIG:-$ROOT/config.json}}"
TIER="${SMOKE_TIER:-bronze}"
BIN="${DATASYNC_BIN:-$ROOT/cpp/build/DataSync}"

# shellcheck source=scripts/container-compose.sh
source "$ROOT/scripts/container-compose.sh"

run_cli() {
  if [[ -x "$BIN" ]]; then
    DATASYNC_CONFIG="$CONFIG" "$BIN" "$@"
  else
    ensure_container_runtime || { echo "✖ need $BIN or podman/docker" >&2; exit 1; }
    docker_compose run --rm \
      -e DATASYNC_CONFIG=/app/config.json \
      -e DATASYNC_HOST_NETWORK=1 \
      -e KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" \
      -v "$CONFIG:/app/config.json:ro" \
      datasync "$@"
  fi
}

read_pg_dsn() {
  python3 - "$CONFIG" <<'PY'
import json, sys
ds = json.load(open(sys.argv[1]))["datasync"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
}

list_connections() {
  if [[ -n "${SMOKE_CONN_ID:-}" ]]; then
    printf '%s\n' "$SMOKE_CONN_ID"
    return 0
  fi
  read -r pg_host pg_port pg_db pg_user pg_pass < <(read_pg_dsn)
  export PGPASSWORD="$pg_pass"
  psql -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" -At -c \
    "SELECT alias FROM cdc_catalog.connections WHERE active = true ORDER BY alias"
}

conn_engine() {
  read -r pg_host pg_port pg_db pg_user pg_pass < <(read_pg_dsn)
  export PGPASSWORD="$pg_pass"
  psql -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" -At -c \
    "SELECT db_engine::text FROM cdc_catalog.connections WHERE alias = '$1' AND active = true LIMIT 1"
}

catalog_pending_full_load() {
  local conn_id="$1"
  read -r pg_host pg_port pg_db pg_user pg_pass < <(read_pg_dsn)
  export PGPASSWORD="$pg_pass"
  psql -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" -At -c \
    "SELECT count(*) FROM cdc_catalog.catalog
     WHERE conn_id = '$conn_id' AND active = true AND needs_full_load = true
       AND service_tier::text = lower('$TIER')"
}

catalog_apply_ready() {
  local conn_id="$1"
  read -r pg_host pg_port pg_db pg_user pg_pass < <(read_pg_dsn)
  export PGPASSWORD="$pg_pass"
  psql -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" -At -c \
    "SELECT count(*) FROM cdc_catalog.catalog
     WHERE conn_id = '$conn_id' AND active = true AND cdc_enabled = true
       AND needs_full_load = false AND has_pk = true
       AND status NOT IN ('skipped','disabled')
       AND service_tier::text = lower('$TIER')"
}

smoke_conn() {
  local conn_id="$1"
  local engine
  engine="$(conn_engine "$conn_id")"
  [[ -n "$engine" ]] || { echo "✖ unknown conn_id: $conn_id" >&2; return 1; }

  echo ""
  echo "━━ Smoke $conn_id ($engine) tier=$TIER ━━"

  echo "→ discover"
  run_cli discover || echo "WARN: discover had errors (continuing)"

  local pending
  pending="$(catalog_pending_full_load "$conn_id")"
  echo "→ full-load (pending=$pending)"
  if [[ "${pending:-0}" -gt 0 ]]; then
    run_cli full-load --tier "$TIER" --conn-id "$conn_id"
  else
    echo "  skip: no needs_full_load=true on tier $TIER"
  fi

  local ready
  ready="$(catalog_apply_ready "$conn_id")"
  echo "→ capture (apply_ready=$ready)"
  run_cli capture --tier "$TIER" --conn-id "$conn_id"

  echo "→ kafka-apply"
  run_cli kafka-apply --tier "$TIER" --conn-id "$conn_id"

  ready="$(catalog_apply_ready "$conn_id")"
  pending="$(catalog_pending_full_load "$conn_id")"
  echo "✔ $conn_id done — apply_ready=$ready pending_full_load=$pending"
}

main() {
  [[ -f "$CONFIG" ]] || { echo "✖ missing config: $CONFIG" >&2; exit 1; }

  mapfile -t conns < <(list_connections)
  if [[ "${#conns[@]}" -eq 0 ]]; then
    echo "✖ no active connections in cdc_catalog.connections" >&2
    exit 1
  fi

  local failed=0
  for c in "${conns[@]}"; do
    [[ -n "$c" ]] || continue
    if ! smoke_conn "$c"; then
      failed=$((failed + 1))
    fi
  done

  if [[ "$failed" -gt 0 ]]; then
    echo "✖ smoke failed for $failed connection(s)" >&2
    exit 1
  fi
  echo ""
  echo "✔ smoke_pipeline passed (${#conns[@]} connection(s))"
}

main "$@"
