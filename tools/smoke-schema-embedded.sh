#!/usr/bin/env bash
# Smoke: embedded schema ensure (catalog + lake) is idempotent; pipeline OK for all engines.
# Docs: Obsidian DataSync/Embedded Schema Change Process.md
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"

log() { printf '[schema-smoke] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

read -r DS_HOST DS_PORT DS_DB DS_USER DS_PASS <<< "$(
  python3 - "$CONFIG" <<'PY'
import json, sys
ds = json.load(open(sys.argv[1]))["datasync"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
)"
read -r DL_HOST DL_PORT DL_DB DL_USER DL_PASS <<< "$(
  python3 - "$CONFIG" <<'PY'
import json, sys
dl = json.load(open(sys.argv[1]))["datalake"]
print(dl["host"], dl["port"], dl["database"], dl["user"], dl["password"])
PY
)"

run_ds() {
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@"
}

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

lake_sql() {
  PGPASSWORD="$DL_PASS" psql -h "$DL_HOST" -p "$DL_PORT" -U "$DL_USER" -d "$DL_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

assert_catalog_objects() {
  catalog_sql "SELECT 1 FROM cdc_catalog.connections LIMIT 1;" >/dev/null || fail "connections missing"
  catalog_sql "SELECT 1 FROM cdc_catalog.catalog LIMIT 1;" >/dev/null || fail "catalog missing"
  catalog_sql "SELECT 1 FROM cdc_catalog.gap_events LIMIT 1;" >/dev/null 2>&1 || catalog_sql "SELECT 1 FROM information_schema.tables WHERE table_schema='cdc_catalog' AND table_name='gap_events';" | grep -qx 1 || fail "gap_events missing"
  catalog_sql "SELECT COUNT(*) FROM cdc_catalog.connections WHERE alias IN ('MARIADBTEST','MSSQLTEST','MONGOTEST') AND active;" | grep -qx 3 || fail "expected 3 dev connections"
}

assert_lake_helpers() {
  lake_sql "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname='lake' AND p.proname='ensure_monthly_partitions';" | grep -qx 1 || fail "lake.ensure_monthly_partitions missing"
}

log "catalog-schema-only + lake-schema-only (pass 1)"
run_ds catalog-schema-only >/dev/null
run_ds lake-schema-only >/dev/null
assert_catalog_objects
assert_lake_helpers

log "idempotent re-apply (pass 2)"
run_ds catalog-schema-only >/dev/null
run_ds lake-schema-only >/dev/null
assert_catalog_objects
assert_lake_helpers

log "3-engine coercion scan (pipeline parity after schema ensure)"
"$ROOT/tools/smoke-type-coercion-all-engines.sh"

log "schema-smoke OK"
