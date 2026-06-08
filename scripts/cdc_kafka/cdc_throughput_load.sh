#!/usr/bin/env bash
# Sequential 1M-row CDC throughput load: MariaDB → MSSQL → MongoDB.
#
# Inserts only (does NOT wait for capture/apply). With DataSync daemon running,
# CDC will drain over multiple 60s slices — review apply_batch_stats afterward.
#
# Defaults match active catalog rows (bronze):
#   MARIADB_LOCAL  datasync_test.test
#   MSSQL_LOCAL    testing.user.costumers
#   MONGO_LOCAL    cdc_test.users
#
# Usage:
#   export PGPASSWORD=Yucaquemada1 MARIADB_PASSWORD=Yucaquemada1
#   # DataSync daemon must be running:
#   sudo systemctl status DataSync
#
#   ROW_COUNT=1000000 bash scripts/cdc_kafka/cdc_throughput_load.sh
#
# Smaller smoke:
#   ROW_COUNT=10000 bash scripts/cdc_kafka/cdc_throughput_load.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${ROOT}/scripts/cdc_kafka/cdc_throughput_load.py"

export ROW_COUNT="${ROW_COUNT:-1000000}"
export BATCH_SIZE="${BATCH_SIZE:-5000}"
export STRESS_RUN_ID="${STRESS_RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
export ENGINES="${ENGINES:-mariadb,mssql,mongo}"

export MARIADB_HOST="${MARIADB_HOST:-127.0.0.1}"
export MARIADB_PORT="${MARIADB_PORT:-3306}"
export MARIADB_USER="${MARIADB_USER:-tomy.berrios}"
export MARIADB_PASSWORD="${MARIADB_PASSWORD:-Yucaquemada1}"
export MARIADB_SCHEMA="${MARIADB_SCHEMA:-datasync_test}"
export MARIADB_TABLE="${MARIADB_TABLE:-test}"

export MSSQL_MODE="${MSSQL_MODE:-docker}"
export MSSQL_CONTAINER="${MSSQL_CONTAINER:-datalake-mssql}"
export MSSQL_USER="${MSSQL_USER:-sa}"
export MSSQL_PASSWORD="${MSSQL_PASSWORD:-Yucaquemada1}"
export MSSQL_DB="${MSSQL_DB:-testing}"
export MSSQL_SCHEMA="${MSSQL_SCHEMA:-user}"
export MSSQL_TABLE="${MSSQL_TABLE:-costumers}"

export MONGO_URI="${MONGO_URI:-mongodb://localhost:27017/?replicaSet=rs0}"
export MONGO_DB="${MONGO_DB:-cdc_test}"
export MONGO_COLL="${MONGO_COLL:-users}"

echo "=============================================="
echo " CDC throughput load (sequential by engine)"
echo " ROW_COUNT=${ROW_COUNT}  STRESS_RUN_ID=${STRESS_RUN_ID}"
echo "=============================================="
echo ""
echo "Prereqs:"
echo "  - DataSync daemon running (capture + apply per tier)"
echo "  - Catalog tables active + cdc_enabled (bronze defaults above)"
echo "  - Kafka + sources up (MariaDB GTID binlog, MSSQL CDC, Mongo replica set)"
echo ""

if ! command -v mariadb >/dev/null 2>&1; then
  echo "ERROR: mariadb client not found"
  exit 1
fi
if ! command -v mongosh >/dev/null 2>&1; then
  echo "ERROR: mongosh not found"
  exit 1
fi
if [[ "${MSSQL_MODE}" == "docker" ]] && ! docker ps --format '{{.Names}}' | grep -qx "${MSSQL_CONTAINER}"; then
  echo "WARN: MSSQL container '${MSSQL_CONTAINER}' not running — start docker-compose.mssql or set MSSQL_MODE=host"
fi

python3 "${PY}" \
  --row-count "${ROW_COUNT}" \
  --batch-size "${BATCH_SIZE}" \
  --run-id "${STRESS_RUN_ID}" \
  --engines "${ENGINES}"

echo ""
echo "=============================================="
echo " Insert phase complete. CDC catch-up is async."
echo "=============================================="
echo ""
echo "Lake row counts (parent tables — includes prior data):"
export PGPASSWORD="${PGPASSWORD:-Yucaquemada1}"
psql -h localhost -U "${PGUSER:-tomy.berrios}" -d "${PGDATABASE:-DataLake}" -c "
SELECT 'mariadb' AS engine, count(*) AS lake_rows FROM ${MARIADB_SCHEMA}.${MARIADB_TABLE}
UNION ALL
SELECT 'mssql', count(*) FROM testing_user.${MSSQL_TABLE}
UNION ALL
SELECT 'mongo', count(*) FROM ${MONGO_DB}.${MONGO_COLL};
" 2>/dev/null || echo "(skip lake counts if schemas differ)"

echo ""
echo "apply_batch_stats — paste results after daemon catches up:"
echo ""
cat <<EOF
SELECT conn_id, source_schema, source_table, batch_id,
       events_inserts, events_updates, events_deletes, events_total,
       events_per_minute, duration_ms, capture_lag_seconds, kafka_consumer_lag,
       reconciliation_rag, semaphore, is_stale, is_starving, logged_at
FROM cdc_catalog.apply_batch_stats
WHERE conn_id IN ('MARIADB_LOCAL','MSSQL_LOCAL','MONGO_LOCAL')
  AND logged_at > now() - interval '2 hours'
ORDER BY logged_at DESC
LIMIT 30;
EOF

echo ""
echo "Stress rows filter (source):"
echo "  MariaDB: name LIKE 'stress-${STRESS_RUN_ID}-m-%'"
echo "  MSSQL:   name LIKE 'stress-${STRESS_RUN_ID}-s-%'"
echo "  Mongo:   { stress_run: '${STRESS_RUN_ID}' }"
echo ""
echo "Re-run one engine only:"
echo "  ENGINES=mariadb ROW_COUNT=1000000 bash scripts/cdc_kafka/cdc_throughput_load.sh"
