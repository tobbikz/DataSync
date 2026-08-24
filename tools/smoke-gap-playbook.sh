#!/usr/bin/env bash
# Natural test: gap playbook sweep recovers apply_position gap_detected (SQL-only path).
# Capture-side sweep needs live MariaDB — see Gap Playbook.md.
# Docs: Obsidian DataSync/Gap Playbook.md
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
CONN_ID="${CONN_ID:-MARIADBTEST}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"

log() { printf '[gap-smoke] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

read -r DS_HOST DS_PORT DS_DB DS_USER DS_PASS <<< "$(
  python3 - "$CONFIG" <<'PY'
import json, sys
ds = json.load(open(sys.argv[1]))["datasync"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
)"

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -tA -c "$1"
}

run_daemon_once() {
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" daemon --once >/dev/null 2>&1 || true
}

TAG="gap_smoke_$(date +%Y%m%d_%H%M%S)"

log "pick catalog row conn=$CONN_ID"
catalog_sql "SELECT 1" >/dev/null || fail "datasync PG down"

read -r CATALOG_ID SCHEMA TABLE <<< "$(
  catalog_sql "
    SELECT c.catalog_id, c.source_schema, c.source_table
    FROM cdc_catalog.catalog c
    JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
    WHERE c.conn_id = '$CONN_ID' AND c.active = true AND c.has_pk = true
    ORDER BY c.catalog_id
    LIMIT 1;
  " | tr '|' ' '
)"

[[ -n "${CATALOG_ID:-}" ]] || fail "no apply_position row for $CONN_ID"

log "seed apply gap_detected catalog_id=$CATALOG_ID ($SCHEMA.$TABLE) tag=$TAG"

catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false,
    cdc_enabled = true,
    status = 'success'::cdc_catalog.replication_status,
    last_error = NULL,
    updated_at = now()
WHERE catalog_id = $CATALOG_ID;

UPDATE cdc_catalog.apply_position
SET status = 'gap_detected'::cdc_catalog.cdc_health_status,
    last_error = 'kafka offset out of range ($TAG)',
    updated_at = now()
WHERE catalog_id = $CATALOG_ID;
" >/dev/null

BEFORE=$(catalog_sql "SELECT count(*) FROM cdc_catalog.gap_events WHERE conn_id='$CONN_ID' AND detail LIKE '%$TAG%'")
[[ "$BEFORE" == "0" ]] || fail "precondition: no prior gap_events for tag"

log "trigger daemon --once (apply gap sweep)..."
run_daemon_once

AP=$(catalog_sql "SELECT status::text FROM cdc_catalog.apply_position WHERE catalog_id=$CATALOG_ID")
[[ "$AP" == "healthy" ]] || fail "apply_position still $AP (expected healthy after sweep)"

EVENTS=$(catalog_sql "SELECT count(*) FROM cdc_catalog.gap_events WHERE conn_id='$CONN_ID' AND detail LIKE '%$TAG%'")
[[ "${EVENTS:-0}" -ge 1 ]] || fail "no gap_events row recorded for tag"

log "PASS apply gap playbook — gap_events=$EVENTS, apply healthy"
