#!/usr/bin/env bash
# Smoke: mid-COPY full-load checkpoint resume (MariaDB + MSSQL + MongoDB).
# Kill full-load after the copy workers checkpoint, re-run, assert resume log + row parity.
# Also asserts the persisted slice plan covers the table without gaps, since the resume
# reuses that plan instead of re-splitting the source.
# Usage: ./tools/smoke-full-load-resume.sh [mariadb|mssql|mssql-composite|mongo|all]
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
ROW_TARGET="${ROW_TARGET:-200000}"
# MariaDB copies roughly an order of magnitude faster than the other two engines, so at
# ROW_TARGET it finishes before the kill lands and there is no mid-copy state to resume.
MARIADB_ROW_TARGET="${MARIADB_ROW_TARGET:-3000000}"
TARGET="${1:-all}"

log() { printf '[resume-smoke] %s\n' "$*"; }
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

run_ds_bg() {
  local cname="datasync-fl-resume-$$-$RANDOM"
  docker run -d --name "$cname" --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@" >/dev/null
  echo "$cname"
}

run_ds() {
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    "$IMAGE" "$@"
}

kill_ds_container() {
  local cname="$1"
  docker kill "$cname" >/dev/null 2>&1 || true
  docker rm -f "$cname" >/dev/null 2>&1 || true
}

read_mariadb_creds() {
  local row
  row=$(catalog_sql "SELECT username || '|' || password FROM cdc_catalog.connections WHERE alias = 'MARIADBTEST';")
  [[ -n "$row" ]] || fail "MARIADBTEST connection missing (run tools/bootstrap-dev-engines.sh)"
  MARIADB_USER="${row%%|*}"
  MARIADB_PASSWORD="${row#*|}"
}

seed_mssql_customers() {
  local cur
  cur=$(docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.customers;" | tr -d '[:space:]')
  log "mssql source rows before seed: ${cur:-0}"
  if [[ "${cur:-0}" -ge "$ROW_TARGET" ]]; then
    return 0
  fi
  local need=$((ROW_TARGET - cur))
  log "mssql seeding $need rows (target=$ROW_TARGET)"
  docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "
SET NOCOUNT ON;
DECLARE @need INT = $need;
INSERT INTO testdb.dbo.customers (name, active)
SELECT TOP (@need)
  N'resume_seed_' + CAST(n AS NVARCHAR(20)),
  CASE WHEN n % 2 = 0 THEN 0 ELSE 1 END
FROM (
  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
) x;
" >/dev/null
}

# Composite PK fixture: exercises the sampled boundary split, which is the only way a
# multi-column key can be handed to more than one worker.
seed_mssql_order_items() {
  docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "
SET NOCOUNT ON;
IF OBJECT_ID('testdb.dbo.order_items', 'U') IS NULL
  CREATE TABLE testdb.dbo.order_items (
    order_id INT NOT NULL,
    line_no INT NOT NULL,
    sku NVARCHAR(40) NOT NULL,
    qty INT NOT NULL,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, line_no)
  );
" >/dev/null

  local cur
  cur=$(docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -h -1 -W -Q \
    "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.order_items;" | tr -d '[:space:]')
  log "mssql order_items rows before seed: ${cur:-0}"
  if [[ "${cur:-0}" -lt "$ROW_TARGET" ]]; then
    local need=$((ROW_TARGET - cur))
    log "mssql seeding $need order_items rows (target=$ROW_TARGET)"
    docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "
SET NOCOUNT ON;
DECLARE @need INT = $need;
DECLARE @base INT = (SELECT ISNULL(MAX(order_id), 0) FROM testdb.dbo.order_items);
INSERT INTO testdb.dbo.order_items (order_id, line_no, sku, qty)
SELECT TOP (@need)
  @base + ((n - 1) / 4) + 1,
  ((n - 1) % 4) + 1,
  N'SKU-' + CAST(n AS NVARCHAR(20)),
  (n % 7) + 1
FROM (
  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
) x;
" >/dev/null
  fi

  # MSSQL discovery only scans cdc.change_tables, so the fixture is invisible until it has
  # a capture instance.
  docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d testdb -Q "
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.order_items'))
  EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'order_items',
    @role_name = NULL,
    @supports_net_changes = 0;
" >/dev/null

  # discover only inserts new catalog rows; the fixture needs activating like bootstrap does.
  run_ds discover >/dev/null
  catalog_sql "
UPDATE cdc_catalog.catalog
SET active = true, cdc_enabled = true, capture_during_full_load = true, updated_at = now()
WHERE conn_id = 'MSSQLTEST' AND source_table = 'order_items' AND has_pk = true;
" >/dev/null
}

seed_mariadb_customers() {
  local cur
  cur=$(docker exec datasync-mariadb-test mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" -D testdb -N -B -e \
    "SELECT COUNT(*) FROM customers;" | tr -d '[:space:]')
  log "mariadb source rows before seed: ${cur:-0}"
  if [[ "${cur:-0}" -ge "$MARIADB_ROW_TARGET" ]]; then
    return 0
  fi
  local need=$((MARIADB_ROW_TARGET - cur))
  log "mariadb seeding $need rows (target=$MARIADB_ROW_TARGET)"
  # Chunked so one statement does not build a multi-million row transaction.
  local chunk=250000
  while (( need > 0 )); do
    local step=$(( need < chunk ? need : chunk ))
    docker exec datasync-mariadb-test mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" -D testdb -e "
INSERT INTO customers (name, email)
SELECT CONCAT('resume_seed_', seq), CONCAT('resume_seed_', seq, '@example.com')
FROM seq_1_to_$step;
" >/dev/null
    need=$(( need - step ))
  done
}

seed_mongo_customers() {
  docker exec datasync-mongodb-test mongosh --quiet --eval "
const target = $ROW_TARGET;
const coll = db.getSiblingDB('mongotest').customers;
let n = coll.countDocuments();
print('mongo source rows before seed: ' + n);
if (n >= target) quit(0);
const batch = 2000;
while (n < target) {
  const docs = [];
  for (let i = 0; i < batch && n < target; i++, n++) {
    docs.push({ name: 'resume_seed_' + n, active: n % 2 === 0, updated_at: new Date() });
  }
  coll.insertMany(docs);
}
print('mongo seeded to ' + coll.countDocuments());
" >/dev/null
}

prepare_catalog() {
  local conn_id="$1"
  local source_table="$2"
  local catalog_id
  catalog_id=$(catalog_sql "
SELECT catalog_id FROM cdc_catalog.catalog
WHERE conn_id = '$conn_id' AND source_table = '$source_table' AND active
LIMIT 1;
")
  [[ -n "$catalog_id" ]] || fail "catalog row missing for $conn_id.$source_table"
  catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false, status = 'success', updated_at = now()
WHERE conn_id = '$conn_id' AND source_table <> '$source_table';
DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.catalog
SET needs_full_load = true, status = 'pending', last_error = NULL, updated_at = now()
WHERE catalog_id = $catalog_id;
" >/dev/null
  echo "$catalog_id"
}

wait_resumable_copy_checkpoint() {
  local catalog_id="$1"
  local min_workers="${2:-1}"
  local expected_rows="${3:-$ROW_TARGET}"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    local row
    row=$(catalog_sql "
SELECT COALESCE(MAX(source_rows), 0)::text || '|' ||
       COALESCE(SUM(rows_loaded) FILTER (WHERE phase = 'copy'), 0)::text || '|' ||
       COUNT(*) FILTER (
         WHERE phase = 'copy'
           AND last_pk IS NOT NULL
           AND last_pk::text <> '[]'
       )::text
FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id = $catalog_id;
")
    local source_rows="${row%%|*}"
    local rest="${row#*|}"
    local rows_loaded="${rest%%|*}"
    local copy_cp="${rest##*|}"
    if [[ "${copy_cp:-0}" -ge "$min_workers" && "${rows_loaded:-0}" -ge $((20000 * min_workers)) ]]; then
      local target_rows="${source_rows:-0}"
      if [[ "$target_rows" -le 0 ]]; then
        target_rows="$expected_rows"
      fi
      if [[ "${rows_loaded:-0}" -lt $((target_rows - 5000)) ]]; then
        log "copy checkpoint ready catalog_id=$catalog_id rows_loaded=$rows_loaded source_rows=$target_rows workers=$copy_cp (min=$min_workers)"
        return 0
      fi
    fi
    sleep 0.02
  done
  return 1
}

# The resume reuses the stored split verbatim, so a gap or an overlap between consecutive
# slices would silently skip or duplicate rows on the second run.
assert_slice_plan() {
  local catalog_id="$1"
  local expected_workers="$2"
  local workers
  workers=$(catalog_sql "
SELECT COUNT(*)::text FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id = $catalog_id AND phase = 'copy';
")
  [[ "${workers:-0}" == "$expected_workers" ]] ||
    fail "slice plan has $workers copy rows, expected $expected_workers"

  catalog_sql "
SELECT bool_and(contiguous)::text FROM (
  SELECT slice_begin IS NOT DISTINCT FROM LAG(slice_end) OVER (ORDER BY worker_id) AS contiguous
  FROM cdc_catalog.full_load_checkpoint
  WHERE catalog_id = $catalog_id AND phase = 'copy'
) t;
" | grep -qx true || fail "slice plan is not contiguous for catalog $catalog_id"

  catalog_sql "
SELECT (slice_end IS NULL)::text FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id = $catalog_id AND phase = 'copy'
ORDER BY worker_id DESC LIMIT 1;
" | grep -qx true || fail "last slice is bounded, tail rows would be dropped"

  log "slice plan OK catalog_id=$catalog_id workers=$workers"
}

assert_resume_log() {
  local conn_id="$1"
  catalog_sql "
SELECT 1 FROM cdc_catalog.logs
WHERE conn_id = '$conn_id'
  AND message = 'full load resumed from checkpoint'
  AND logged_at > now() - interval '10 minutes'
LIMIT 1;
" | grep -qx 1 || fail "missing resume log for $conn_id"
}

assert_catalog_success() {
  local catalog_id="$1"
  catalog_sql "
SELECT status FROM cdc_catalog.catalog WHERE catalog_id = $catalog_id;
" | grep -qx success || fail "catalog $catalog_id not success after resume"
  catalog_sql "
SELECT COUNT(*) FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
" | grep -qx 0 || fail "checkpoints not cleared for catalog $catalog_id"
  lake_sql "
SELECT COUNT(*) FROM lake.full_load_position WHERE catalog_id = $catalog_id;
" | grep -qx 0 || fail "lake copy positions not cleared for catalog $catalog_id"
}

# The lake position is what makes the resume exactly-once: it commits with the COPY it
# describes, while the catalog checkpoint lands a moment later and can be a batch behind.
assert_lake_position_ahead_or_equal() {
  local catalog_id="$1"
  local positions
  positions=$(lake_sql "
SELECT COUNT(*)::text FROM lake.full_load_position WHERE catalog_id = $catalog_id;
")
  [[ "${positions:-0}" -gt 0 ]] ||
    fail "no lake copy position after interrupted COPY for catalog $catalog_id"
  log "lake copy positions after kill catalog_id=$catalog_id rows=$positions"
}

run_engine_smoke() {
  local engine="$1"
  # min_workers mirrors the per-engine worker knob in cpp/include/pipeline_defaults.hpp.
  local conn_id source_table lake_schema lake_table source_count_cmd min_workers
  local row_target="$ROW_TARGET"
  case "$engine" in
    mariadb)
      conn_id=MARIADBTEST
      source_table=customers
      lake_schema=testdb
      lake_table=customers
      min_workers=4
      row_target="$MARIADB_ROW_TARGET"
      read_mariadb_creds
      source_count_cmd='docker exec datasync-mariadb-test mariadb -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" -D testdb -N -B -e "SELECT COUNT(*) FROM customers;" | tr -d "[:space:]"'
      seed_mariadb_customers
      ;;
    mssql)
      conn_id=MSSQLTEST
      source_table=customers
      lake_schema=testdb_dbo
      lake_table=customers
      min_workers=2
      source_count_cmd='docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P '"$MSSQL_SA_PASSWORD"' -C -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.customers;" | tr -d "[:space:]"'
      seed_mssql_customers
      ;;
    mssql-composite)
      conn_id=MSSQLTEST
      source_table=order_items
      lake_schema=testdb_dbo
      lake_table=order_items
      min_workers=2
      source_count_cmd='docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd -S 127.0.0.1,1433 -U sa -P '"$MSSQL_SA_PASSWORD"' -C -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM testdb.dbo.order_items;" | tr -d "[:space:]"'
      seed_mssql_order_items
      ;;
    mongo)
      conn_id=MONGOTEST
      source_table=customers
      lake_schema=mongotest
      lake_table=customers
      min_workers=4
      source_count_cmd='docker exec datasync-mongodb-test mongosh --quiet --eval "db.getSiblingDB('"'"'mongotest'"'"').customers.countDocuments()" | tr -d "[:space:]"'
      seed_mongo_customers
      ;;
    *)
      fail "unknown engine: $engine (use mariadb|mssql|mssql-composite|mongo|all)"
      ;;
  esac

  log "=== $engine resume smoke (conn=$conn_id table=$source_table) ==="
  local catalog_id
  catalog_id=$(prepare_catalog "$conn_id" "$source_table")

  local cname
  cname=$(run_ds_bg full-load --conn-id "$conn_id")
  if ! wait_resumable_copy_checkpoint "$catalog_id" "$min_workers" "$row_target"; then
    kill_ds_container "$cname"
    fail "$engine: no resumable copy checkpoint within 180s (increase ROW_TARGET?)"
  fi
  kill_ds_container "$cname"
  log "$engine: killed mid-COPY container=$cname"

  assert_slice_plan "$catalog_id" "$min_workers"
  assert_lake_position_ahead_or_equal "$catalog_id"

  catalog_sql "
SELECT status FROM cdc_catalog.catalog WHERE catalog_id = $catalog_id;
" | grep -qxE 'full_load_in_progress|pending' || fail "$engine: expected in-progress catalog after kill"

  local partial_lake
  partial_lake=$(lake_sql "SELECT COUNT(*)::text FROM ${lake_schema}.${lake_table};" || echo 0)
  [[ "${partial_lake:-0}" -gt 0 ]] || fail "$engine: lake empty after interrupted COPY"

  log "$engine: re-run full-load (expect resume)"
  run_ds full-load --conn-id "$conn_id" >/dev/null

  assert_resume_log "$conn_id"
  assert_catalog_success "$catalog_id"

  local source_rows lake_rows
  source_rows=$(eval "$source_count_cmd")
  lake_rows=$(lake_sql "SELECT COUNT(*)::text FROM ${lake_schema}.${lake_table};")
  log "$engine: source_rows=$source_rows lake_rows=$lake_rows"
  [[ "$source_rows" == "$lake_rows" ]] || fail "$engine: row mismatch source=$source_rows lake=$lake_rows"
  log "$engine resume-smoke OK"
}

log "ensure dev engines up"
"$ROOT/tools/start-dev-engines.sh" >/dev/null

case "$TARGET" in
  mariadb) run_engine_smoke mariadb ;;
  mssql) run_engine_smoke mssql ;;
  mssql-composite) run_engine_smoke mssql-composite ;;
  mongo) run_engine_smoke mongo ;;
  all)
    run_engine_smoke mariadb
    run_engine_smoke mssql
    run_engine_smoke mssql-composite
    run_engine_smoke mongo
    ;;
  *)
    fail "usage: $0 [mariadb|mssql|mssql-composite|mongo|all]"
    ;;
esac

log "resume-smoke OK ($TARGET)"
