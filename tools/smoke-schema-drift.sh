#!/usr/bin/env bash
# Smoke: schema drift beyond ADD COLUMN (MariaDB + MSSQL).
# Adds, drops, renames and narrows a column in the source and asserts the lake either follows
# in place or asks for a full-load reboot, never diverging in silence.
# MongoDB is out of scope by design: without DDL a missing field is not a dropped field.
# Usage: ./tools/smoke-schema-drift.sh [mariadb|mssql|all]
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
TARGET="${1:-all}"

log() { printf '[drift-smoke] %s\n' "$*"; }
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

# sqlcmd reports errors on stdout, so a call site that redirects stdout away would swallow
# them and leave the smoke failing with no message. Capture, and re-emit on stderr if it broke.
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

read_mariadb_creds() {
  local row
  row=$(catalog_sql "SELECT username || '|' || password FROM cdc_catalog.connections WHERE alias = 'MARIADBTEST';")
  [[ -n "$row" ]] || fail "MARIADBTEST connection missing (run tools/bootstrap-dev-engines.sh)"
  MARIADB_USER="${row%%|*}"
  MARIADB_PASSWORD="${row#*|}"
}

# ---------------------------------------------------------------- fixtures

# wide_col holds 16 chars so a later narrowing to 8 cannot be absorbed by the lake in place.
setup_mariadb_fixture() {
  read_mariadb_creds
  mariadb_sql "
DROP TABLE IF EXISTS drift_probe;
CREATE TABLE drift_probe (
  id INT PRIMARY KEY,
  keep_col VARCHAR(32) NOT NULL,
  doomed_col VARCHAR(32) NULL,
  old_name VARCHAR(32) NULL,
  wide_col VARCHAR(64) NULL
);
INSERT INTO drift_probe (id, keep_col, doomed_col, old_name, wide_col) VALUES
  (1, 'keep1', 'doomed1', 'old1', '0123456789abcdef'),
  (2, 'keep2', 'doomed2', 'old2', 'fedcba9876543210'),
  (3, 'keep3', 'doomed3', 'old3', 'abcdefabcdefabcd');
" >/dev/null
}

setup_mssql_fixture() {
  mssql_sql "
IF EXISTS (SELECT 1 FROM testdb.cdc.change_tables WHERE capture_instance = 'dbo_drift_probe')
  EXEC testdb.sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'drift_probe',
       @capture_instance = N'dbo_drift_probe';
IF OBJECT_ID('testdb.dbo.drift_probe', 'U') IS NOT NULL DROP TABLE testdb.dbo.drift_probe;
GO
CREATE TABLE testdb.dbo.drift_probe (
  id INT NOT NULL PRIMARY KEY,
  keep_col NVARCHAR(32) NOT NULL,
  doomed_col NVARCHAR(32) NULL,
  old_name NVARCHAR(32) NULL,
  wide_col NVARCHAR(64) NULL
);
GO
INSERT INTO testdb.dbo.drift_probe (id, keep_col, doomed_col, old_name, wide_col) VALUES
  (1, N'keep1', N'doomed1', N'old1', N'0123456789abcdef'),
  (2, N'keep2', N'doomed2', N'old2', N'fedcba9876543210'),
  (3, N'keep3', N'doomed3', N'old3', N'abcdefabcdefabcd');
EXEC testdb.sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'drift_probe',
     @role_name = NULL, @supports_net_changes = 0;
" >/dev/null
}

# ---------------------------------------------------------------- helpers

# Only the fixture reloads: any other table left flagged would drag the whole conn into a
# full load and the assertions below could not tell which run touched what.
onboard_fixture() {
  local conn_id="$1"
  run_ds discover >/dev/null
  local catalog_id
  catalog_id=$(catalog_sql "
SELECT catalog_id FROM cdc_catalog.catalog
WHERE conn_id = '$conn_id' AND source_table = 'drift_probe' LIMIT 1;
")
  [[ -n "$catalog_id" ]] || fail "$conn_id: discover did not register drift_probe"
  catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false, status = 'success', updated_at = now()
WHERE conn_id = '$conn_id' AND catalog_id <> $catalog_id;
DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.catalog
SET active = true, cdc_enabled = true, needs_full_load = true, status = 'pending',
    last_error = NULL, last_error_at = NULL, updated_at = now()
WHERE catalog_id = $catalog_id;
" >/dev/null
  run_ds full-load --conn-id "$conn_id" >/dev/null
  echo "$catalog_id"
}

lake_has_column() {
  local schema="$1" table="$2" column="$3"
  [[ "$(lake_sql "
SELECT COUNT(*)::text FROM information_schema.columns
WHERE table_schema = '$schema' AND table_name = '$table' AND column_name = '$column';
")" == "1" ]]
}

assert_lake_column() {
  local schema="$1" table="$2" column="$3" phase="$4"
  lake_has_column "$schema" "$table" "$column" || fail "$phase: lake is missing column $column"
}

assert_no_lake_column() {
  local schema="$1" table="$2" column="$3" phase="$4"
  lake_has_column "$schema" "$table" "$column" && fail "$phase: lake still carries column $column"
  return 0
}

assert_not_flagged() {
  local catalog_id="$1" phase="$2"
  local flag
  flag=$(catalog_sql "SELECT needs_full_load::text FROM cdc_catalog.catalog WHERE catalog_id = $catalog_id;")
  [[ "$flag" == "false" ]] || fail "$phase: table was flagged for a reload it did not need"
}

assert_flagged() {
  local catalog_id="$1" phase="$2" expect="$3"
  local row
  row=$(catalog_sql "
SELECT needs_full_load::text || '|' || COALESCE(last_error, '')
FROM cdc_catalog.catalog WHERE catalog_id = $catalog_id;
")
  [[ "${row%%|*}" == "true" ]] || fail "$phase: table was not flagged for a full-load reboot"
  [[ "${row#*|}" == *"$expect"* ]] || fail "$phase: reason '${row#*|}' does not mention '$expect'"
  log "  reason: ${row#*|}"
}

reload_fixture() {
  local conn_id="$1" catalog_id="$2"
  catalog_sql "DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;" >/dev/null
  run_ds full-load --conn-id "$conn_id" >/dev/null
  catalog_sql "
SELECT status FROM cdc_catalog.catalog WHERE catalog_id = $catalog_id;
" | grep -qx success || fail "reload did not finish successfully for catalog $catalog_id"
}

# ---------------------------------------------------------------- engine run

run_engine_smoke() {
  local engine="$1"
  local conn_id lake_schema
  case "$engine" in
    mariadb)
      conn_id=MARIADBTEST
      lake_schema=testdb
      setup_mariadb_fixture
      ;;
    mssql)
      conn_id=MSSQLTEST
      lake_schema=testdb_dbo
      setup_mssql_fixture
      ;;
    *) fail "unknown engine: $engine (use mariadb|mssql|all)" ;;
  esac

  log "=== $engine schema drift smoke (conn=$conn_id) ==="
  local catalog_id
  catalog_id=$(onboard_fixture "$conn_id")
  log "$engine: fixture onboarded catalog_id=$catalog_id"

  local rows
  rows=$(lake_sql "SELECT COUNT(*)::text FROM ${lake_schema}.drift_probe;")
  [[ "$rows" == "3" ]] || fail "$engine: expected 3 rows in the lake after the load, got $rows"

  log "$engine: phase A — a new column lands in the lake"
  if [[ "$engine" == mariadb ]]; then
    mariadb_sql "ALTER TABLE drift_probe ADD COLUMN added_col VARCHAR(32) NULL;" >/dev/null
  else
    mssql_sql "ALTER TABLE testdb.dbo.drift_probe ADD added_col NVARCHAR(32) NULL;" >/dev/null
  fi
  run_ds ddl-sync --conn-id "$conn_id" --table drift_probe >/dev/null
  assert_lake_column "$lake_schema" drift_probe added_col "$engine phase A"
  assert_not_flagged "$catalog_id" "$engine phase A"

  log "$engine: phase B — a dropped column leaves the lake, no reload needed"
  if [[ "$engine" == mariadb ]]; then
    mariadb_sql "ALTER TABLE drift_probe DROP COLUMN doomed_col;" >/dev/null
  else
    mssql_sql "ALTER TABLE testdb.dbo.drift_probe DROP COLUMN doomed_col;" >/dev/null
  fi
  run_ds ddl-sync --conn-id "$conn_id" --table drift_probe >/dev/null
  assert_no_lake_column "$lake_schema" drift_probe doomed_col "$engine phase B"
  assert_lake_column "$lake_schema" drift_probe keep_col "$engine phase B"
  assert_not_flagged "$catalog_id" "$engine phase B"

  log "$engine: phase C — a rename is ambiguous, so the table reloads instead of guessing"
  if [[ "$engine" == mariadb ]]; then
    mariadb_sql "ALTER TABLE drift_probe CHANGE old_name new_name VARCHAR(32) NULL;" >/dev/null
  else
    # MSSQL refuses sp_rename on a captured column with "Msg 4928 ... is 'REPLICATED'", so the
    # only way to rename one is to drop the capture instance and recreate it afterwards.
    mssql_sql "
EXEC testdb.sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'drift_probe',
     @capture_instance = N'dbo_drift_probe';
EXEC testdb.sys.sp_rename N'dbo.drift_probe.old_name', N'new_name', N'COLUMN';
EXEC testdb.sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'drift_probe',
     @role_name = NULL, @supports_net_changes = 0;
" >/dev/null
  fi
  run_ds ddl-sync --conn-id "$conn_id" --table drift_probe >/dev/null
  assert_flagged "$catalog_id" "$engine phase C" "possible rename"
  # The old column keeps its data until the reload: dropping it here would destroy the only
  # copy of the values the renamed column is supposed to inherit.
  assert_lake_column "$lake_schema" drift_probe old_name "$engine phase C"

  reload_fixture "$conn_id" "$catalog_id"
  assert_lake_column "$lake_schema" drift_probe new_name "$engine phase C reload"
  assert_no_lake_column "$lake_schema" drift_probe old_name "$engine phase C reload"
  local renamed
  renamed=$(lake_sql "SELECT new_name FROM ${lake_schema}.drift_probe WHERE id = 1;")
  [[ "$renamed" == "old1" ]] || fail "$engine phase C reload: renamed column holds '$renamed', expected 'old1'"

  log "$engine: phase D — a narrowing the lake cannot absorb asks for a reload"
  # The source shrinks its own data first; the lake still holds the old values because no
  # capture/apply runs in between, which is exactly the state that makes the ALTER fail.
  # The DDL differs per engine because the lake type mapping does: MariaDB stores every
  # varchar as TEXT, so shortening a length is a no-op there and only a real type change
  # forces a migration. MSSQL keeps VARCHAR(n), so the length alone is enough.
  if [[ "$engine" == mariadb ]]; then
    mariadb_sql "
UPDATE drift_probe SET wide_col = id;
ALTER TABLE drift_probe MODIFY wide_col BIGINT NULL;
" >/dev/null
  else
    mssql_sql "
UPDATE testdb.dbo.drift_probe SET wide_col = LEFT(wide_col, 8);
ALTER TABLE testdb.dbo.drift_probe ALTER COLUMN wide_col NVARCHAR(8) NULL;
" >/dev/null
  fi
  run_ds ddl-sync --conn-id "$conn_id" --table drift_probe >/dev/null
  assert_flagged "$catalog_id" "$engine phase D" "cannot migrate in place"

  reload_fixture "$conn_id" "$catalog_id"
  # Convergence is asserted through a clean second pass rather than a type name, which differs
  # per engine mapping: no drift left means the lake matches the source shape.
  local report
  report=$(run_ds ddl-sync --conn-id "$conn_id" --table drift_probe | tail -n1)
  log "  post-reload ddl-sync: $report"
  python3 - "$report" <<'PY' || fail "post-reload ddl-sync still reports drift"
import json, sys
r = json.loads(sys.argv[1])
sys.exit(0 if r["tables_flagged_for_reload"] == 0 and r["tables_failed"] == 0 else 1)
PY
  assert_not_flagged "$catalog_id" "$engine phase D reload"

  log "$engine schema-drift smoke OK"
}

log "ensure dev engines up"
"$ROOT/tools/start-dev-engines.sh" >/dev/null

case "$TARGET" in
  mariadb) run_engine_smoke mariadb ;;
  mssql) run_engine_smoke mssql ;;
  all)
    run_engine_smoke mariadb
    run_engine_smoke mssql
    ;;
  *) fail "usage: $0 [mariadb|mssql|all]" ;;
esac

log "schema-drift smoke OK ($TARGET)"
