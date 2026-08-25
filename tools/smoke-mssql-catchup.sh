#!/usr/bin/env bash
# Smoke: MSSQL catch-up merge post full-load.
# The COPY window is opened deterministically instead of by racing a live load: kill the
# full load mid-COPY, mutate rows it already committed to the lake, then let the resumed
# load finish. Those mutations only reach the lake if CDC replays from the LSN anchored
# before the COPY started, so this fails on the old "reset LSN to max after full load".
# Row counts converge either way — the assertions here are on values.
# Usage: ./tools/smoke-mssql-catchup.sh
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
CONN_ID="${CONN_ID:-MSSQLTEST}"
SOURCE_DB=testdb
SOURCE_SCHEMA=dbo
SOURCE_TABLE=customers
LAKE_SCHEMA=testdb_dbo
LAKE_TABLE=customers
ROW_TARGET="${ROW_TARGET:-200000}"
COPY_WORKERS="${COPY_WORKERS:-2}"
COPY_WAIT_SECS="${COPY_WAIT_SECS:-180}"
CONVERGE_SECS="${CONVERGE_SECS:-300}"

TAG="catchup_$(date +%Y%m%d_%H%M%S)_$RANDOM"
UPD_NAME="upd_${TAG}"
INS_NAME="ins_${TAG}"

log() { printf '[catchup-smoke] %s\n' "$*"; }
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

mssql_sql() {
  docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd \
    -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W -Q "SET NOCOUNT ON; $1"
}

mssql_scalar() {
  mssql_sql "$1" | tr -d '[:space:]'
}

# Values that may contain spaces: trim the row, keep the text intact.
mssql_text() {
  mssql_sql "$1" | head -n1 | sed -e 's/[[:space:]]*$//' -e 's/^[[:space:]]*//'
}

run_ds_bg() {
  local cname="datasync-catchup-$$-$RANDOM"
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

nudge_pipeline() {
  run_ds capture --conn-id "$CONN_ID" >/dev/null 2>&1 || true
  run_ds kafka-apply --conn-id "$CONN_ID" >/dev/null 2>&1 || true
}

stored_lsn() {
  catalog_sql "
SELECT COALESCE(encode(last_start_lsn, 'hex'), '') FROM cdc_catalog.cdc_mssql_lsn
WHERE conn_id = '$CONN_ID' AND database = '$SOURCE_DB'
  AND schema_name = '$SOURCE_SCHEMA' AND table_name = '$SOURCE_TABLE';
"
}

# fn_cdc_get_max_lsn only resolves inside the CDC-enabled database.
source_max_lsn() {
  docker exec datasync-mssql-test /opt/mssql-tools18/bin/sqlcmd \
    -S 127.0.0.1,1433 -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d "$SOURCE_DB" -h -1 -W \
    -Q "SET NOCOUNT ON; SELECT CONVERT(VARCHAR(42), sys.fn_cdc_get_max_lsn(), 2);" | tr -d '[:space:]'
}

seed_customers() {
  local cur
  cur=$(mssql_scalar "SELECT COUNT(*) FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE;")
  log "mssql source rows before seed: ${cur:-0}"
  if [[ "${cur:-0}" -ge "$ROW_TARGET" ]]; then
    return 0
  fi
  local need=$((ROW_TARGET - cur))
  log "seeding $need rows (target=$ROW_TARGET)"
  mssql_sql "
DECLARE @need INT = $need;
INSERT INTO $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE (name, active)
SELECT TOP (@need)
  N'catchup_seed_' + CAST(n AS NVARCHAR(20)),
  CASE WHEN n % 2 = 0 THEN 0 ELSE 1 END
FROM (
  SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
  FROM sys.all_objects a CROSS JOIN sys.all_objects b
) x;
" >/dev/null
}

prepare_catalog() {
  local catalog_id
  catalog_id=$(catalog_sql "
SELECT catalog_id FROM cdc_catalog.catalog
WHERE conn_id = '$CONN_ID' AND source_table = '$SOURCE_TABLE' AND active
LIMIT 1;
")
  [[ -n "$catalog_id" ]] || fail "catalog row missing for $CONN_ID.$SOURCE_TABLE (run tools/bootstrap-dev-engines.sh)"
  catalog_sql "
UPDATE cdc_catalog.catalog
SET needs_full_load = false, status = 'success', updated_at = now()
WHERE conn_id = '$CONN_ID' AND source_table <> '$SOURCE_TABLE';
DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $catalog_id;
UPDATE cdc_catalog.catalog
SET needs_full_load = true, status = 'pending', last_error = NULL, updated_at = now()
WHERE catalog_id = $catalog_id;
" >/dev/null
  echo "$catalog_id"
}

# Same gate as tools/smoke-full-load-resume.sh: enough committed copy batches to resume from,
# but short of the end so the load still has work left after the kill.
wait_resumable_copy_checkpoint() {
  local deadline=$((SECONDS + COPY_WAIT_SECS))
  while (( SECONDS < deadline )); do
    local row source_rows rest rows_loaded copy_cp
    row=$(catalog_sql "
SELECT COALESCE(MAX(source_rows), 0)::text || '|' ||
       COALESCE(SUM(rows_loaded) FILTER (WHERE phase = 'copy'), 0)::text || '|' ||
       COUNT(*) FILTER (
         WHERE phase = 'copy'
           AND last_pk IS NOT NULL
           AND last_pk::text <> '[]'
       )::text
FROM cdc_catalog.full_load_checkpoint
WHERE catalog_id = $CATALOG_ID;
")
    source_rows="${row%%|*}"
    rest="${row#*|}"
    rows_loaded="${rest%%|*}"
    copy_cp="${rest##*|}"
    if [[ "${copy_cp:-0}" -ge "$COPY_WORKERS" && "${rows_loaded:-0}" -ge $((20000 * COPY_WORKERS)) ]]; then
      local target_rows="${source_rows:-0}"
      [[ "$target_rows" -gt 0 ]] || target_rows="$ROW_TARGET"
      if [[ "${rows_loaded:-0}" -lt $((target_rows - 5000)) ]]; then
        log "copy checkpoint ready rows_loaded=$rows_loaded source_rows=$target_rows workers=$copy_cp"
        return 0
      fi
    fi
    sleep 0.02
  done
  return 1
}

converged() {
  local upd del ins
  upd=$(lake_sql "SELECT COALESCE(MAX(name), '') FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE id = $PROBE_UPD_ID;" 2>/dev/null || echo "")
  del=$(lake_sql "SELECT COUNT(*)::text FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE id = $PROBE_DEL_ID;" 2>/dev/null || echo 1)
  ins=$(lake_sql "SELECT COUNT(*)::text FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE name = '$INS_NAME';" 2>/dev/null || echo 0)
  [[ "$upd" == "$UPD_NAME" && "${del:-1}" == "0" && "${ins:-0}" -ge 1 ]]
}

log "=== MSSQL catch-up smoke (conn=$CONN_ID table=$SOURCE_TABLE tag=$TAG) ==="
log "ensure dev engines up"
"$ROOT/tools/start-dev-engines.sh" >/dev/null

seed_customers

CATALOG_ID=$(prepare_catalog)
PROBE_UPD_ID=$(mssql_scalar "SELECT MIN(id) FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE;")
PROBE_DEL_ID=$(mssql_scalar "SELECT MIN(id) FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE WHERE id > $PROBE_UPD_ID;")
[[ -n "$PROBE_UPD_ID" && -n "$PROBE_DEL_ID" ]] || fail "could not pick probe rows"
OLD_NAME=$(mssql_text "SELECT name FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE WHERE id = $PROBE_UPD_ID;")
log "catalog_id=$CATALOG_ID probes: update id=$PROBE_UPD_ID (name=$OLD_NAME) delete id=$PROBE_DEL_ID"

log "phase 1: start full load, kill mid-COPY to freeze the load window open"
CNAME=$(run_ds_bg full-load --conn-id "$CONN_ID")
trap 'docker rm -f "$CNAME" >/dev/null 2>&1 || true' EXIT
if ! wait_resumable_copy_checkpoint; then
  fail "no resumable copy checkpoint within ${COPY_WAIT_SECS}s (raise ROW_TARGET?)"
fi
docker rm -f "$CNAME" >/dev/null 2>&1 || true
log "killed full load mid-COPY"

catalog_sql "
SELECT 1 FROM cdc_catalog.logs
WHERE conn_id = '$CONN_ID'
  AND message = 'mssql LSN T0 anchored before full load copy'
  AND logged_at > now() - interval '30 minutes'
LIMIT 1;
" | grep -qx 1 || fail "missing 'mssql LSN T0 anchored before full load copy' log"

ANCHOR_LSN=$(stored_lsn)
[[ -n "$ANCHOR_LSN" ]] || fail "no cdc_mssql_lsn row after the anchor"
log "anchor lsn=$ANCHOR_LSN"

COPIED=$(lake_sql "
SELECT COUNT(*)::text FROM ${LAKE_SCHEMA}.${LAKE_TABLE}
WHERE id IN ($PROBE_UPD_ID, $PROBE_DEL_ID);
")
[[ "${COPIED:-0}" == "2" ]] || fail "probe rows not in lake after the kill (COPY had not reached them)"
LAKE_OLD_NAME=$(lake_sql "SELECT name FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE id = $PROBE_UPD_ID;")
[[ "$LAKE_OLD_NAME" == "$OLD_NAME" ]] || fail "lake probe has unexpected name '$LAKE_OLD_NAME' (expected '$OLD_NAME')"
log "probe rows committed to lake by the COPY with pre-mutation values"

log "phase 2: mutate the already-copied rows (update + delete + insert)"
mssql_sql "
UPDATE $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE SET name = N'$UPD_NAME' WHERE id = $PROBE_UPD_ID;
DELETE FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE WHERE id = $PROBE_DEL_ID;
INSERT INTO $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE (name, active) VALUES (N'$INS_NAME', 1);
" >/dev/null
MAX_LSN_AFTER=$(source_max_lsn)
log "source max lsn after mutations=$MAX_LSN_AFTER"

log "phase 3: resume full load to completion"
RESUME_LOG=$(mktemp)
if ! run_ds full-load --conn-id "$CONN_ID" >"$RESUME_LOG" 2>&1; then
  tail -20 "$RESUME_LOG" | sed 's/^/[full-load] /'
  fail "resumed full load exited non-zero"
fi

catalog_sql "
SELECT 1 FROM cdc_catalog.logs
WHERE conn_id = '$CONN_ID'
  AND message = 'full load resumed from checkpoint'
  AND logged_at > now() - interval '30 minutes'
LIMIT 1;
" | grep -qx 1 || fail "resume log missing — the second run was not a resume"

catalog_sql "SELECT status FROM cdc_catalog.catalog WHERE catalog_id = $CATALOG_ID;" \
  | grep -qxE 'success|cdc_in_progress' || fail "catalog $CATALOG_ID not success after resume"

AFTER_LSN=$(stored_lsn)
[[ "$AFTER_LSN" == "$ANCHOR_LSN" ]] \
  || fail "full load moved the CDC cursor from $ANCHOR_LSN to $AFTER_LSN — load window discarded"
log "cdc cursor still at the anchor after full load"

log "phase 4: replay the load window (capture + apply)"
DEADLINE=$((SECONDS + CONVERGE_SECS))
while (( SECONDS < DEADLINE )); do
  if converged; then
    break
  fi
  nudge_pipeline
  sleep 3
done

converged || {
  log "lake name for id=$PROBE_UPD_ID: $(lake_sql "SELECT COALESCE(MAX(name),'<missing>') FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE id = $PROBE_UPD_ID;")"
  log "lake rows for deleted id=$PROBE_DEL_ID: $(lake_sql "SELECT COUNT(*) FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE id = $PROBE_DEL_ID;")"
  log "lake rows for inserted name=$INS_NAME: $(lake_sql "SELECT COUNT(*) FROM ${LAKE_SCHEMA}.${LAKE_TABLE} WHERE name = '$INS_NAME';")"
  fail "lake did not converge within ${CONVERGE_SECS}s — load window was lost"
}
log "catch-up applied: update + delete + insert all reflected in lake"

SOURCE_ROWS=$(mssql_scalar "SELECT COUNT(*) FROM $SOURCE_DB.$SOURCE_SCHEMA.$SOURCE_TABLE;")
LAKE_ROWS=$(lake_sql "SELECT COUNT(*)::text FROM ${LAKE_SCHEMA}.${LAKE_TABLE};")
log "source_rows=$SOURCE_ROWS lake_rows=$LAKE_ROWS"
[[ "$SOURCE_ROWS" == "$LAKE_ROWS" ]] || fail "row mismatch source=$SOURCE_ROWS lake=$LAKE_ROWS"

log "catchup-smoke OK"
