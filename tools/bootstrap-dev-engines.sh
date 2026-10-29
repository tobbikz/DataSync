#!/usr/bin/env bash
# Register MSSQLTEST + MONGOTEST connections, discover, activate, full-load, coercion-audit (all engines).
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"

# Git Bash /c/... paths break Windows python + docker.exe volume mounts.
host_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$p"
  else
    printf '%s' "$p"
  fi
}
CONFIG_HOST="$(host_path "$CONFIG")"
PYBIN=python3
command -v python >/dev/null 2>&1 && PYBIN=python

log() { printf '[bootstrap-dev] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

read -r DS_HOST DS_PORT DS_DB DS_USER DS_PASS <<< "$(
  "$PYBIN" - "$CONFIG_HOST" <<'PY'
import json, sys
ds = json.load(open(sys.argv[1]))["datasync"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
)"

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

run_ds() {
  docker run --rm --network host \
    -v "${CONFIG_HOST}:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@" >/dev/null
}

log "ensure dev engines up"
"$ROOT/tools/start-dev-engines.sh"

log "ensure catalog + lake schemas"
run_ds catalog-schema-only
run_ds lake-schema-only

log "upsert cdc_catalog.connections (MARIADBTEST, MSSQLTEST, MONGOTEST)"
catalog_sql "
INSERT INTO cdc_catalog.connections (alias, db_engine, host, port, db_name, username, password, extras, active)
VALUES
  ('MARIADBTEST', 'mariadb', '127.0.0.1', 3306, 'testdb', 'datasync', 'DataSync_Dev1!', '{}'::jsonb, true),
  ('MSSQLTEST', 'mssql', '127.0.0.1', 1433, 'testdb', 'sa', '${MSSQL_SA_PASSWORD}', '{}'::jsonb, true),
  ('MONGOTEST', 'mongodb', '127.0.0.1', 27017, 'mongotest', '', '', '{\"replica_set\":\"rs0\"}'::jsonb, true)
ON CONFLICT (alias) DO UPDATE SET
  db_engine = EXCLUDED.db_engine,
  host = EXCLUDED.host,
  port = EXCLUDED.port,
  db_name = EXCLUDED.db_name,
  username = EXCLUDED.username,
  password = EXCLUDED.password,
  extras = EXCLUDED.extras,
  active = true,
  updated_at = now();
"

log "discover"
run_ds discover

log "activate catalog rows (discover inserts active=false by default)"
catalog_sql "
UPDATE cdc_catalog.catalog
SET active = true, cdc_enabled = true, capture_during_full_load = true, updated_at = now()
WHERE conn_id IN ('MARIADBTEST', 'MSSQLTEST', 'MONGOTEST') AND has_pk = true;
"

for conn in MARIADBTEST MSSQLTEST MONGOTEST; do
  log "full-load --conn-id $conn"
  run_ds full-load --conn-id "$conn" || fail "full-load failed for $conn"
done

log "coercion audit dry-run (all engines)"
REPORT=$(CONN_ID= ./tools/audit-type-coercion.sh 2>/dev/null | python3 - <<'PY'
import json, sys
raw = sys.stdin.read()
start = raw.find("{")
if start < 0:
    sys.exit("no JSON from coercion-audit")
report = json.loads(raw[start:])
for eng in ("mariadb", "mssql", "mongodb"):
    block = report.get(eng, {})
    scanned = block.get("scanned", 0)
    print(f"{eng}: scanned={scanned} stale={block.get('stale',0)}")
    if scanned < 1:
        sys.exit(f"expected {eng} tables, got {scanned}")
print(json.dumps({"scanned": report["scanned"], "stale": report["stale"]}))
PY
) || fail "coercion audit validation failed"
log "coercion audit: $REPORT"
log "bootstrap-dev OK"
