#!/usr/bin/env bash
# Smoke: apply dead-letter queue (MariaDB + MSSQL + MongoDB).
# Poisons exactly one row by putting a CHECK on the lake mirror that only that row violates,
# then asserts the three things the queue exists to guarantee: the good rows in the same
# batch still land, the bad row is parked with its payload, and the table keeps replicating
# instead of being quarantined over it.
# Usage: ./tools/smoke-apply-dlq.sh [mariadb|mssql|mongo|all]
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
TARGET="${1:-all}"

log() { printf '[dlq-smoke] %s\n' "$*"; }
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

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

lake_sql() {
  PGPASSWORD="$DL_PASS" psql -h "$DL_HOST" -p "$DL_PORT" -U "$DL_USER" -d "$DL_DB" -v ON_ERROR_STOP=1 -tA -c "$1"
}

run_ds() {
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@"
}

mariadb_sql() {
  docker exec datasync-mariadb-test mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" -D testdb -N -B -e "$1"
}

# sqlcmd reports errors on stdout, so a call site redirecting stdout would swallow them.
mssql_sql() {
  local out rc
  out=$(docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd \
    -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W -Q "SET NOCOUNT ON; $1" 2>&1)
  rc=$?
  if (( rc != 0 )); then
    printf '%s\n' "$out" >&2
    return "$rc"
  fi
  printf '%s\n' "$out"
}

mongo_sql() {
  docker exec datasync-mongodb-test mongosh --quiet --eval "$1"
}

read_mariadb_creds() {
  local row
  row=$(catalog_sql "SELECT username || '|' || password FROM cdc_catalog.connections WHERE alias = 'MARIADBTEST';")
  [[ -n "$row" ]] || fail "MARIADBTEST connection missing (run tools/bootstrap-dev-engines.sh)"
  MARIADB_USER="${row%%|*}"
  MARIADB_PASSWORD="${row#*|}"
}

# ---------------------------------------------------------------- fixtures

setup_mariadb_fixture() {
  mariadb_sql "
DROP TABLE IF EXISTS dlq_probe;
CREATE TABLE dlq_probe (
  id INT NOT NULL PRIMARY KEY,
  city VARCHAR(32) NOT NULL
);
INSERT INTO dlq_probe (id, city) VALUES (1, 'Guadalajara'), (2, 'Monterrey');
" >/dev/null
}

setup_mssql_fixture() {
  mssql_sql "
IF EXISTS (SELECT 1 FROM testdb.cdc.change_tables WHERE capture_instance = 'dbo_dlq_probe')
  EXEC testdb.sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'dlq_probe',
       @capture_instance = N'dbo_dlq_probe';
IF OBJECT_ID('testdb.dbo.dlq_probe', 'U') IS NOT NULL DROP TABLE testdb.dbo.dlq_probe;
GO
CREATE TABLE testdb.dbo.dlq_probe (
  id INT NOT NULL PRIMARY KEY,
  city NVARCHAR(32) NOT NULL
);
GO
INSERT INTO testdb.dbo.dlq_probe (id, city) VALUES (1, N'Guadalajara'), (2, N'Monterrey');
EXEC testdb.sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'dlq_probe',
     @role_name = NULL, @supports_net_changes = 0;
" >/dev/null
}

# The Mongo dev connection replicates the `mongotest` database, and the lake schema takes its
# name from it, so the fixture has to live there and not in the `testdb` the other engines use.
setup_mongo_fixture() {
  mongo_sql '
const target = globalThis.db.getSiblingDB("mongotest");
target.dlq_probe.drop();
target.dlq_probe.insertMany([
  { _id: 1, city: "Guadalajara" },
  { _id: 2, city: "Monterrey" },
]);
' >/dev/null
}

# ---------------------------------------------------------------- helpers

# Only the fixture reloads: any other table left pending would drag the whole conn into a
# full load and the assertions below could not tell which run touched what.
onboard_fixture() {
  local conn_id="$1"
  run_ds discover >/dev/null
  local catalog_id
  catalog_id=$(catalog_sql "
SELECT catalog_id FROM cdc_catalog.catalog
WHERE conn_id = '$conn_id' AND source_table = 'dlq_probe' LIMIT 1;
")
  [[ -n "$catalog_id" ]] || fail "$conn_id: discover did not register dlq_probe"
  # Rows parked by a previous run would be counted as this run's.
  catalog_sql "DELETE FROM cdc_catalog.apply_dlq WHERE catalog_id = $catalog_id;" >/dev/null
  catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false, status = 'success', updated_at = now()
WHERE conn_id = '$conn_id' AND catalog_id <> $catalog_id;
DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.apply_position
SET status = 'healthy', quarantined_at = NULL, quarantine_reason = NULL, last_error = NULL
WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.catalog
SET active = true, cdc_enabled = true, needs_full_load = true, status = 'pending',
    last_error = NULL, last_error_at = NULL, updated_at = now()
WHERE catalog_id = $catalog_id;
" >/dev/null
  run_ds full-load --conn-id "$conn_id" >/dev/null
  echo "$catalog_id"
}

# A one-shot `kafka-apply` is useless here: it gives up after three empty 100 ms polls, long
# before a cold consumer finishes joining its group, so it always reports idle_no_messages
# without reading anything. The daemon keeps the consumer alive, which is how CDC actually
# runs, so the smoke drives a real daemon and waits for the lake to converge.
DAEMON_NAME="datasync-dlq-daemon"

start_daemon() {
  docker rm -f "$DAEMON_NAME" >/dev/null 2>&1 || true
  docker run -d --name "$DAEMON_NAME" --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" daemon >/dev/null
  log "daemon started ($DAEMON_NAME)"
}

stop_daemon() {
  docker rm -f "$DAEMON_NAME" >/dev/null 2>&1 || true
}
trap stop_daemon EXIT

# Generous on purpose: the daemon runs capture slices of up to slice_max_seconds (300s by
# default), so a change landing just after a slice closed waits for the next one. Anything
# tighter fails on timing rather than on behaviour.
wait_for_catalog() {
  local what="$1" query="$2" expect="$3"
  for _ in $(seq 1 150); do
    [[ "$(catalog_sql "$query")" == "$expect" ]] && return 0
    sleep 5
  done
  fail "$what: expected '$expect', got '$(catalog_sql "$query")'"
}

wait_for_lake() {
  local what="$1" query="$2" expect="$3"
  for _ in $(seq 1 150); do
    [[ "$(lake_sql "$query")" == "$expect" ]] && return 0
    sleep 5
  done
  fail "$what: expected '$expect', got '$(lake_sql "$query")'"
}

# ---------------------------------------------------------------- engine run

run_engine_smoke() {
  local engine="$1"
  local conn_id lake_schema key_col
  case "$engine" in
    mariadb) conn_id=MARIADBTEST; lake_schema=testdb;     key_col=id;       setup_mariadb_fixture ;;
    mssql)   conn_id=MSSQLTEST;   lake_schema=testdb_dbo; key_col=id;       setup_mssql_fixture ;;
    mongo)   conn_id=MONGOTEST;   lake_schema=mongotest;  key_col=mongo_id; setup_mongo_fixture ;;
    *) fail "unknown engine: $engine (use mariadb|mssql|mongo|all)" ;;
  esac
  local mirror="${lake_schema}.dlq_probe"

  log "=== $engine dlq smoke (conn=$conn_id) ==="
  local catalog_id
  catalog_id=$(onboard_fixture "$conn_id")
  log "$engine: fixture onboarded catalog_id=$catalog_id"

  [[ "$(lake_sql "SELECT COUNT(*)::text FROM $mirror;")" == "2" ]] \
    || fail "$engine: full load did not land 2 rows"

  # The poison. A CHECK the mirror can hold today, so only the row that later violates it
  # fails — the rest of the batch is perfectly applicable, which is the whole point.
  lake_sql "ALTER TABLE $mirror DROP CONSTRAINT IF EXISTS dlq_probe_no_poison;" >/dev/null
  lake_sql "ALTER TABLE $mirror ADD CONSTRAINT dlq_probe_no_poison CHECK (city <> 'POISON');" >/dev/null

  start_daemon

  log "$engine: one row goes bad, another in the same batch is fine"
  case "$engine" in
    mariadb)
      mariadb_sql "UPDATE dlq_probe SET city = 'POISON' WHERE id = 1; UPDATE dlq_probe SET city = 'Puebla' WHERE id = 2;" >/dev/null ;;
    mssql)
      mssql_sql "UPDATE testdb.dbo.dlq_probe SET city = N'POISON' WHERE id = 1; UPDATE testdb.dbo.dlq_probe SET city = N'Puebla' WHERE id = 2;" >/dev/null ;;
    mongo)
      mongo_sql 'const t = globalThis.db.getSiblingDB("mongotest").dlq_probe;
t.updateOne({_id:1},{$set:{city:"POISON"}});
t.updateOne({_id:2},{$set:{city:"Puebla"}});' >/dev/null ;;
  esac

  log "$engine: the good row must still land"
  wait_for_lake "$engine good row applied" \
    "SELECT city FROM $mirror WHERE ${key_col}::text = '2';" "Puebla"

  log "$engine: the bad row must be parked, not lost"
  wait_for_catalog "$engine row parked" \
    "SELECT COUNT(*)::text FROM cdc_catalog.apply_dlq
     WHERE catalog_id = $catalog_id AND status = 'parked';" "1"

  # A parked row is only useful if it says what failed and carries the data.
  local sqlstate
  sqlstate=$(catalog_sql "
SELECT COALESCE(error_sqlstate, '') FROM cdc_catalog.apply_dlq
WHERE catalog_id = $catalog_id AND status = 'parked' LIMIT 1;")
  [[ "$sqlstate" == "23514" ]] \
    || fail "$engine: parked row has sqlstate '$sqlstate', expected 23514 (check violation)"
  local parked_city
  parked_city=$(catalog_sql "
SELECT payload->>'city' FROM cdc_catalog.apply_dlq
WHERE catalog_id = $catalog_id AND status = 'parked' LIMIT 1;")
  [[ "$parked_city" == "POISON" ]] \
    || fail "$engine: parked payload holds city '$parked_city', expected 'POISON'"

  # The regression this feature exists to kill: one bad row used to stop the whole table.
  local status
  status=$(catalog_sql "
SELECT COALESCE(status::text, 'none') FROM cdc_catalog.apply_position WHERE catalog_id = $catalog_id;")
  [[ "$status" != "quarantined" ]] \
    || fail "$engine: the table was quarantined over a single bad row"

  # And the mirror kept the last good value for the poisoned row rather than a partial write.
  [[ "$(lake_sql "SELECT city FROM $mirror WHERE ${key_col}::text = '1';")" == "Guadalajara" ]] \
    || fail "$engine: the poisoned row's mirror value changed despite never being applied"

  log "$engine: replication keeps flowing after the park"
  case "$engine" in
    mariadb) mariadb_sql "UPDATE dlq_probe SET city = 'Toluca' WHERE id = 2;" >/dev/null ;;
    mssql)   mssql_sql "UPDATE testdb.dbo.dlq_probe SET city = N'Toluca' WHERE id = 2;" >/dev/null ;;
    mongo)   mongo_sql 'globalThis.db.getSiblingDB("mongotest").dlq_probe.updateOne({_id:2},{$set:{city:"Toluca"}});' >/dev/null ;;
  esac
  wait_for_lake "$engine still replicating" \
    "SELECT city FROM $mirror WHERE ${key_col}::text = '2';" "Toluca"

  stop_daemon
  lake_sql "ALTER TABLE $mirror DROP CONSTRAINT IF EXISTS dlq_probe_no_poison;" >/dev/null
  log "$engine dlq smoke OK"
}

read_mariadb_creds

log "ensure dev engines up"
"$ROOT/tools/start-dev-engines.sh" >/dev/null

case "$TARGET" in
  mariadb) run_engine_smoke mariadb ;;
  mssql) run_engine_smoke mssql ;;
  mongo) run_engine_smoke mongo ;;
  all)
    run_engine_smoke mariadb
    run_engine_smoke mssql
    run_engine_smoke mongo
    ;;
  *) fail "usage: $0 [mariadb|mssql|mongo|all]" ;;
esac

log "dlq smoke OK ($TARGET)"
