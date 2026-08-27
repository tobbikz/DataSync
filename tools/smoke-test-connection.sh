#!/usr/bin/env bash
# Smoke: DataSync test-connection preflight.
# Engine-agnostic on purpose — it asserts the contract of the command (exit codes, report
# shape, catalog logging) plus a synthetic unreachable connection, so it passes whether or
# not the dev engines happen to be up. Any engine that IS reachable gets its facts checked.
# Usage: ./tools/smoke-test-connection.sh
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
BOGUS_ALIAS="SMOKE_UNREACHABLE"
BOGUS_PORT="${BOGUS_PORT:-59999}"
STANDALONE_ALIAS="SMOKE_MONGO_STANDALONE"
STANDALONE_NAME="datasync-smoke-mongo-standalone"
STANDALONE_PORT="${STANDALONE_PORT:-27099}"

log() { printf '[test-connection-smoke] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

read -r DS_HOST DS_PORT DS_DB DS_USER DS_PASS <<< "$(
  python3 - "$CONFIG" <<'PY'
import json, sys
ds = json.load(open(sys.argv[1]))["datasync"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
PY
)"

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

# stdout is the JSON report; libpq NOTICEs go to stderr and would break the parse.
run_ds() {
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@" 2>/dev/null
}

drop_bogus() {
  catalog_sql "DELETE FROM cdc_catalog.connections WHERE alias = '$BOGUS_ALIAS';" >/dev/null 2>&1 || true
}

log "=== test-connection smoke ==="

REPORT_FILE=$(mktemp)
cleanup_report() { rm -f "$REPORT_FILE"; }

log "phase 1: full sweep over the real connections"
run_ds test-connection > "$REPORT_FILE" || true
[[ -s "$REPORT_FILE" ]] || fail "test-connection produced no output"

python3 - "$REPORT_FILE" <<'PY' || fail "phase 1 assertions failed"
import json, sys
report = json.load(open(sys.argv[1]))
for key in ("batch_id", "connections", "checked", "failed", "warnings"):
    assert key in report, f"missing top-level key: {key}"
assert report["checked"] == len(report["connections"]), "checked does not match the report length"

# Every engine that answered must expose the facts its preflight is supposed to collect.
required = {
    "mariadb": ["log_bin", "binlog_format", "server_id"],
    "mssql": ["is_cdc_enabled", "server_version"],
    "mongodb": [],
}
for conn in report["connections"]:
    for key in ("conn_id", "engine", "target", "reachable", "ok", "errors", "warnings", "facts"):
        assert key in conn, f"{conn.get('conn_id')}: missing key {key}"
    if not conn["reachable"]:
        assert not conn["ok"], f"{conn['conn_id']}: unreachable but reported ok"
        assert conn["errors"], f"{conn['conn_id']}: unreachable without an error"
        print(f"  {conn['conn_id']} ({conn['engine']}): unreachable, reported as failed")
        continue
    for fact in required.get(conn["engine"], []):
        assert fact in conn["facts"], f"{conn['conn_id']}: missing fact {fact}"
    print(f"  {conn['conn_id']} ({conn['engine']}): ok={conn['ok']} warnings={len(conn['warnings'])}")

# A mongodb connection that answered must have resolved its topology.
for conn in report["connections"]:
    if conn["engine"] == "mongodb" and conn["reachable"] and conn["ok"]:
        assert "replica_set" in conn["facts"] or "topology" in conn["facts"], \
            "mongodb reported ok without a replica set or sharded topology"
PY

BATCH_ID=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['batch_id'])" "$REPORT_FILE")
LOGGED=$(catalog_sql "
SELECT COUNT(*) FROM cdc_catalog.logs
WHERE component = 'connection_test' AND batch_id = '$BATCH_ID';
")
[[ "${LOGGED:-0}" -ge 2 ]] || fail "expected per-connection + summary logs for batch $BATCH_ID, got $LOGGED"
log "phase 1 ok (batch=$BATCH_ID, $LOGGED rows in cdc_catalog.logs)"

log "phase 2: a connection pointing nowhere must fail without taking the sweep down"
trap 'drop_bogus; cleanup_report' EXIT
drop_bogus
catalog_sql "
INSERT INTO cdc_catalog.connections (alias, db_engine, host, port, db_name, username, password, active)
VALUES ('$BOGUS_ALIAS', 'mariadb', '127.0.0.1', $BOGUS_PORT, 'nope', 'nobody', 'nothing', true);
" >/dev/null

set +e
run_ds test-connection --conn-id "$BOGUS_ALIAS" > "$REPORT_FILE"
BOGUS_EXIT=$?
set -e
[[ "$BOGUS_EXIT" -eq 1 ]] || fail "unreachable connection should exit 1, got $BOGUS_EXIT"

python3 - "$REPORT_FILE" <<'PY' || fail "phase 2 assertions failed"
import json, sys
report = json.load(open(sys.argv[1]))
assert report["checked"] == 1, "expected exactly one connection in scope"
assert report["failed"] == 1, "expected the connection to be reported as failed"
conn = report["connections"][0]
assert conn["reachable"] is False, "a closed port must not be reachable"
assert conn["ok"] is False, "a closed port must not be ok"
assert any("connect failed" in e for e in conn["errors"]), f"unexpected errors: {conn['errors']}"
PY
drop_bogus
log "phase 2 ok (exit 1, connect failure reported)"

log "phase 3: unknown conn-id is a usage error, not a silent pass"
set +e
run_ds test-connection --conn-id DOES_NOT_EXIST >/dev/null
UNKNOWN_EXIT=$?
set -e
[[ "$UNKNOWN_EXIT" -eq 2 ]] || fail "unknown conn-id should exit 2, got $UNKNOWN_EXIT"
log "phase 3 ok (exit 2)"

# The only Mongo check that blocks a discover, so it is worth a real server rather than a
# hand-written assertion: change streams cannot open on a node outside a replica set.
log "phase 4: a standalone mongod must be rejected, not merely warned about"
drop_standalone() {
  docker rm -f "$STANDALONE_NAME" >/dev/null 2>&1 || true
  catalog_sql "DELETE FROM cdc_catalog.connections WHERE alias = '$STANDALONE_ALIAS';" >/dev/null 2>&1 || true
}
trap 'drop_bogus; cleanup_report; drop_standalone' EXIT
drop_standalone

if ! docker run -d --name "$STANDALONE_NAME" -p "$STANDALONE_PORT:$STANDALONE_PORT" mongo:7 \
      mongod --bind_ip_all --port "$STANDALONE_PORT" >/dev/null 2>&1; then
  log "phase 4 skipped (could not start a standalone mongod)"
else
  for _ in $(seq 1 30); do
    docker exec "$STANDALONE_NAME" mongosh --quiet --port "$STANDALONE_PORT" \
      --eval 'db.adminCommand("ping").ok' >/dev/null 2>&1 && break
    sleep 1
  done
  catalog_sql "
  INSERT INTO cdc_catalog.connections (alias, db_engine, host, port, db_name, username, password, active)
  VALUES ('$STANDALONE_ALIAS', 'mongodb', '127.0.0.1', $STANDALONE_PORT, 'probe', '', '', true);
  " >/dev/null

  set +e
  run_ds test-connection --conn-id "$STANDALONE_ALIAS" > "$REPORT_FILE"
  STANDALONE_EXIT=$?
  set -e
  [[ "$STANDALONE_EXIT" -eq 1 ]] || fail "standalone mongod should exit 1, got $STANDALONE_EXIT"

  python3 - "$REPORT_FILE" <<'PY' || fail "phase 4 assertions failed"
import json, sys
conn = json.load(open(sys.argv[1]))["connections"][0]
assert conn["reachable"] is True, "the standalone node answered, so it must be reachable"
assert conn["ok"] is False, "a standalone node cannot serve change streams"
assert any("standalone" in e for e in conn["errors"]), f"unexpected errors: {conn['errors']}"
PY
  drop_standalone
  log "phase 4 ok (reachable but rejected, exit 1)"
fi

log "PASS: test-connection reports, fails and logs as specified"
