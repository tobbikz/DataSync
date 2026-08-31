#!/usr/bin/env bash
# Smoke: SCD Type 2 opt-in (MariaDB + MSSQL + MongoDB).
# Enables scd2 on a dedicated fixture table, then walks a row through full load, update and
# delete asserting that <table>_history keeps one closed version per change while the mirror
# keeps holding only the current state.
# Usage: ./tools/smoke-scd2.sh [mariadb|mssql|mongo|all]
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
TARGET="${1:-all}"

log() { printf '[scd2-smoke] %s\n' "$*"; }
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
DROP TABLE IF EXISTS scd2_probe;
CREATE TABLE scd2_probe (
  id INT NOT NULL PRIMARY KEY,
  city VARCHAR(32) NOT NULL
);
INSERT INTO scd2_probe (id, city) VALUES (1, 'Guadalajara'), (2, 'Monterrey');
" >/dev/null
}

# SQL Server compiles the whole batch before running it, so DROP/CREATE/INSERT against the
# same name must be split with GO or the INSERT resolves against the table left by a previous run.
setup_mssql_fixture() {
  mssql_sql "
IF EXISTS (SELECT 1 FROM testdb.cdc.change_tables WHERE capture_instance = 'dbo_scd2_probe')
  EXEC testdb.sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'scd2_probe',
       @capture_instance = N'dbo_scd2_probe';
IF OBJECT_ID('testdb.dbo.scd2_probe', 'U') IS NOT NULL DROP TABLE testdb.dbo.scd2_probe;
GO
CREATE TABLE testdb.dbo.scd2_probe (
  id INT NOT NULL PRIMARY KEY,
  city NVARCHAR(32) NOT NULL
);
GO
INSERT INTO testdb.dbo.scd2_probe (id, city) VALUES (1, N'Guadalajara'), (2, N'Monterrey');
EXEC testdb.sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'scd2_probe',
     @role_name = NULL, @supports_net_changes = 0;
" >/dev/null
}

# The Mongo dev connection replicates the `mongotest` database, and the lake schema takes its
# name from it, so the fixture has to live there and not in the `testdb` the other engines use.
setup_mongo_fixture() {
  mongo_sql '
const target = globalThis.db.getSiblingDB("mongotest");
target.scd2_probe.drop();
target.scd2_probe.insertMany([
  { _id: 1, city: "Guadalajara" },
  { _id: 2, city: "Monterrey" },
]);
' >/dev/null
}

# ---------------------------------------------------------------- helpers

# Only the fixture reloads: any other table left pending would drag the whole conn into a
# full load and the assertions below could not tell which run touched what.
onboard_fixture() {
  local conn_id="$1" lake_schema="$2"
  # A reload deliberately keeps history (it closes the open versions and opens new ones), so
  # a re-run would start with the versions of the previous run and the counts below would
  # drift. The fixture recreates the source table from scratch; its history goes with it.
  lake_sql "DROP TABLE IF EXISTS ${lake_schema}.scd2_probe_history;" >/dev/null
  run_ds discover >/dev/null
  local catalog_id
  catalog_id=$(catalog_sql "
SELECT catalog_id FROM cdc_catalog.catalog
WHERE conn_id = '$conn_id' AND source_table = 'scd2_probe' LIMIT 1;
")
  [[ -n "$catalog_id" ]] || fail "$conn_id: discover did not register scd2_probe"
  catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false, status = 'success', updated_at = now()
WHERE conn_id = '$conn_id' AND catalog_id <> $catalog_id;
DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.catalog
SET active = true, cdc_enabled = true, needs_full_load = true, status = 'pending',
    scd2_enabled = true, last_error = NULL, last_error_at = NULL, updated_at = now()
WHERE catalog_id = $catalog_id;
" >/dev/null
  run_ds full-load --conn-id "$conn_id" >/dev/null
  echo "$catalog_id"
}

# A one-shot `kafka-apply` is useless here: it gives up after three empty 100 ms polls, long
# before a cold consumer finishes joining its group, so it always reports idle_no_messages
# without reading anything. The daemon keeps the consumer alive, which is how CDC actually
# runs, so the smoke drives a real daemon and waits for the lake to converge.
DAEMON_NAME="datasync-scd2-daemon"

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
wait_for() {
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
  local hist="${lake_schema}.scd2_probe_history"
  local mirror="${lake_schema}.scd2_probe"

  log "=== $engine scd2 smoke (conn=$conn_id) ==="
  local catalog_id
  catalog_id=$(onboard_fixture "$conn_id" "$lake_schema")
  log "$engine: fixture onboarded catalog_id=$catalog_id"

  log "$engine: the full load opens one version per row"
  [[ "$(lake_sql "
SELECT COUNT(*)::text FROM information_schema.tables
WHERE table_schema = '$lake_schema' AND table_name = 'scd2_probe_history';")" == "1" ]] \
    || fail "$engine: full load did not create the history table"
  [[ "$(lake_sql "SELECT COUNT(*)::text FROM $hist;")" == "2" ]] \
    || fail "$engine: expected 2 seeded versions, got $(lake_sql "SELECT COUNT(*)::text FROM $hist;")"
  [[ "$(lake_sql "SELECT COUNT(*)::text FROM $hist WHERE _scd_is_current AND _scd_valid_to IS NULL;")" == "2" ]] \
    || fail "$engine: seeded versions are not open"

  # Only now: a daemon running during the onboarding full load would race with it.
  start_daemon

  log "$engine: an update closes the old version and opens a new one"
  case "$engine" in
    mariadb) mariadb_sql "UPDATE scd2_probe SET city = 'Zapopan' WHERE id = 1;" >/dev/null ;;
    mssql)   mssql_sql "UPDATE testdb.dbo.scd2_probe SET city = N'Zapopan' WHERE id = 1;" >/dev/null ;;
    mongo)   mongo_sql 'globalThis.db.getSiblingDB("mongotest").scd2_probe.updateOne({_id:1},{$set:{city:"Zapopan"}});' >/dev/null ;;
  esac
  wait_for "$engine update" \
    "SELECT COUNT(*)::text FROM $hist WHERE $key_col::text = '1';" "2"

  # The whole point of Type 2: the value as of before the change is still queryable.
  local closed_city
  closed_city=$(lake_sql "
SELECT city FROM $hist WHERE $key_col::text = '1' AND NOT _scd_is_current;")
  [[ "$closed_city" == "Guadalajara" ]] \
    || fail "$engine: closed version holds city '$closed_city', expected 'Guadalajara'"
  local open_city
  open_city=$(lake_sql "SELECT city FROM $hist WHERE $key_col::text = '1' AND _scd_is_current;")
  [[ "$open_city" == "Zapopan" ]] \
    || fail "$engine: open version holds city '$open_city', expected 'Zapopan'"
  # An interval with a hole or an overlap makes every "as of" query wrong.
  [[ "$(lake_sql "
SELECT COUNT(*)::text FROM $hist WHERE $key_col::text = '1' AND NOT _scd_is_current
  AND _scd_valid_to = (SELECT _dl_load_timestamp FROM $hist
                       WHERE $key_col::text = '1' AND _scd_is_current);")" == "1" ]] \
    || fail "$engine: the closed version does not end where the new one starts"

  log "$engine: a delete closes the version and says it was deleted"
  case "$engine" in
    mariadb) mariadb_sql "DELETE FROM scd2_probe WHERE id = 2;" >/dev/null ;;
    mssql)   mssql_sql "DELETE FROM testdb.dbo.scd2_probe WHERE id = 2;" >/dev/null ;;
    mongo)   mongo_sql 'globalThis.db.getSiblingDB("mongotest").scd2_probe.deleteOne({_id:2});' >/dev/null ;;
  esac
  wait_for "$engine delete" \
    "SELECT COUNT(*)::text FROM $hist WHERE $key_col::text = '2' AND _scd_is_deleted;" "1"
  [[ "$(lake_sql "SELECT COUNT(*)::text FROM $hist WHERE $key_col::text = '2' AND _scd_is_current;")" == "0" ]] \
    || fail "$engine: the deleted row still has an open version"
  [[ "$(lake_sql "SELECT city FROM $hist WHERE $key_col::text = '2';")" == "Monterrey" ]] \
    || fail "$engine: the deleted row lost its last known value"

  # The mirror is untouched by all of this: it still answers "what is true now".
  [[ "$(lake_sql "SELECT COUNT(*)::text FROM $mirror;")" == "1" ]] \
    || fail "$engine: the mirror should hold exactly the one surviving row"
  [[ "$(lake_sql "SELECT city FROM $mirror;")" == "Zapopan" ]] \
    || fail "$engine: the mirror lost the current value"

  stop_daemon
  log "$engine scd2 smoke OK"
}

# A table that never opted in must not grow a history table: this is what makes it opt-in.
assert_optout_has_no_history() {
  local lake_schema="$1" table="$2"
  [[ "$(lake_sql "
SELECT COUNT(*)::text FROM information_schema.tables
WHERE table_schema = '$lake_schema' AND table_name = '${table}_history';")" == "0" ]] \
    || fail "opt-out: $lake_schema.$table grew a history table without being enabled"
  log "opt-out OK: $lake_schema.$table has no history table"
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

assert_optout_has_no_history testdb customers

log "scd2 smoke OK ($TARGET)"
