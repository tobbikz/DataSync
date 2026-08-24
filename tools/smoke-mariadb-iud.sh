#!/usr/bin/env bash
# Smoke E2E: MariaDB INSERT / UPDATE / DELETE → capture → lake.
# Docs: Obsidian DataSync/Testing Strategy.md
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
CONN_ID="${CONN_ID:-MARIADBTEST}"
MARIADB_HOST="${MARIADB_HOST:-127.0.0.1}"
MARIADB_PORT="${MARIADB_PORT:-3306}"
MARIADB_DB="${MARIADB_DB:-testdb}"
MARIADB_USER="${MARIADB_USER:-}"
MARIADB_PASSWORD="${MARIADB_PASSWORD:-}"
POLL_SECS="${POLL_SECS:-120}"
POLL_INTERVAL="${POLL_INTERVAL:-5}"
NUDGE="${NUDGE:-1}"

TAG=""
EMAIL=""
SKU=""
UPD_NAME=""
DS_HOST="" DS_PORT="" DS_DB="" DS_USER="" DS_PASS=""
DL_HOST="" DL_PORT="" DL_DB="" DL_USER="" DL_PASS=""

log() { printf '[smoke] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

read_config() {
  python3 - "$CONFIG" <<'PY'
import json, sys
c = json.load(open(sys.argv[1]))
ds, dl = c["datasync"], c["datalake"]
print(ds["host"], ds["port"], ds["database"], ds["user"], ds["password"])
print(dl["host"], dl["port"], dl["database"], dl["user"], dl["password"])
PY
}

mariadb_sql() {
  docker run --rm --network host mariadb:11 mariadb \
    -h"$MARIADB_HOST" -P"$MARIADB_PORT" -u"$MARIADB_USER" -p"$MARIADB_PASSWORD" "$MARIADB_DB" -N -e "$1"
}

lake_sql() {
  PGPASSWORD="$DL_PASS" psql -h "$DL_HOST" -p "$DL_PORT" -U "$DL_USER" -d "$DL_DB" -tA -c "$1"
}

catalog_sql() {
  PGPASSWORD="$DS_PASS" psql -h "$DS_HOST" -p "$DS_PORT" -U "$DS_USER" -d "$DS_DB" -tA -c "$1"
}

nudge_pipeline() {
  log "nudge capture + apply..."
  local bin=(/usr/local/bin/DataSync capture --conn-id "$CONN_ID")
  local apply=(/usr/local/bin/DataSync kafka-apply --conn-id "$CONN_ID")
  if docker ps --format '{{.Names}}' | grep -qx 'datasync-datasync-1'; then
    docker exec datasync-datasync-1 "${bin[@]}" >/dev/null 2>&1 || true
    docker exec datasync-datasync-1 "${apply[@]}" >/dev/null 2>&1 || true
    return 0
  fi
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    datasync:local capture --conn-id "$CONN_ID" >/dev/null 2>&1 || true
  docker run --rm --network host \
    -v "$CONFIG:/app/config.json:ro" \
    -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
    -e DATASYNC_CONFIG=/app/config.json \
    datasync:local kafka-apply --conn-id "$CONN_ID" >/dev/null 2>&1 || true
}

check_customer_updated() {
  local row
  row=$(lake_sql "SELECT name FROM testdb.customers WHERE email='$EMAIL' LIMIT 1" 2>/dev/null || true)
  [[ "$row" == "$UPD_NAME" ]]
}

check_order_present() {
  local n
  n=$(lake_sql "SELECT count(*)::text FROM testdb.orders_probe WHERE sku='$SKU'" 2>/dev/null || echo 0)
  [[ "${n:-0}" -ge 1 ]]
}

check_order_absent() {
  local n
  n=$(lake_sql "SELECT count(*)::text FROM testdb.orders_probe WHERE sku='$SKU'" 2>/dev/null || echo 1)
  [[ "${n:-1}" == "0" ]]
}

check_apply_stats() {
  local stats ins upd del
  stats=$(catalog_sql "
    SELECT COALESCE(SUM(events_inserts),0)||','||COALESCE(SUM(events_updates),0)||','||COALESCE(SUM(events_deletes),0)
    FROM cdc_catalog.apply_batch_stats
    WHERE conn_id='$CONN_ID' AND logged_at > now() - interval '15 minutes'
  " 2>/dev/null || echo "0,0,0")
  IFS=, read -r ins upd del <<< "$stats"
  [[ "${ins:-0}" -ge 1 && "${upd:-0}" -ge 1 && "${del:-0}" -ge 1 ]]
}

poll_until() {
  local desc=$1 max=$2
  shift 2
  local deadline=$((SECONDS + max))
  while (( SECONDS < deadline )); do
    if "$@"; then
      log "ok: $desc"
      return 0
    fi
    if [[ "$NUDGE" == "1" ]]; then
      nudge_pipeline || true
    fi
    sleep "$POLL_INTERVAL"
  done
  fail "timeout waiting for $desc (${max}s)"
}

mapfile -t DS < <(read_config | sed -n '1p')
mapfile -t DL < <(read_config | sed -n '2p')
read -r DS_HOST DS_PORT DS_DB DS_USER DS_PASS <<< "${DS[0]}"
read -r DL_HOST DL_PORT DL_DB DL_USER DL_PASS <<< "${DL[0]}"

if [[ -z "$MARIADB_USER" ]]; then
  MARIADB_USER=$(catalog_sql "SELECT username FROM cdc_catalog.connections WHERE alias='$CONN_ID' LIMIT 1")
  MARIADB_PASSWORD=$(catalog_sql "SELECT password FROM cdc_catalog.connections WHERE alias='$CONN_ID' LIMIT 1")
fi

TAG="smoke_$(date +%Y%m%d_%H%M%S)_$RANDOM"
EMAIL="smoke.${TAG}@test.local"
SKU="SMOKE-${TAG}"
UPD_NAME="Smoke upd $TAG"

log "tag=$TAG conn_id=$CONN_ID"

command -v docker >/dev/null || fail "docker required"
catalog_sql 'SELECT 1' >/dev/null || fail "datasync PG down"
lake_sql 'SELECT 1' >/dev/null || fail "datalake PG down"
python3 - <<'PY' || fail "Kafka :9092 down"
import socket
s = socket.socket()
s.settimeout(2)
s.connect(("127.0.0.1", 9092))
s.close()
PY

CAP=$(catalog_sql "SELECT COALESCE(status::text,'missing') FROM cdc_catalog.capture_position WHERE conn_id='$CONN_ID' LIMIT 1")
[[ "$CAP" == "healthy" ]] || log "WARN capture_position status=$CAP"

log "phase 1: INSERT customer, UPDATE, INSERT order..."
mariadb_sql "
INSERT INTO customers (name, email) VALUES ('Smoke $TAG', '$EMAIL');
UPDATE customers SET name='$UPD_NAME' WHERE email='$EMAIL';
INSERT INTO orders_probe (sku, qty, amount) VALUES ('$SKU', 3, 33.33);
" >/dev/null

poll_until "customer UPDATE in lake" "$POLL_SECS" check_customer_updated
poll_until "order INSERT in lake" "$POLL_SECS" check_order_present

log "phase 2: DELETE order..."
mariadb_sql "DELETE FROM orders_probe WHERE sku='$SKU';" >/dev/null

poll_until "order DELETE in lake" "$POLL_SECS" check_order_absent
poll_until "apply_batch_stats I/U/D" 60 check_apply_stats

log "PASS I/U/D — customer='$UPD_NAME' order sku=$SKU deleted from lake"
log "UI: http://127.0.0.1:3000/dashboard/ops → Events → Apply slices"
