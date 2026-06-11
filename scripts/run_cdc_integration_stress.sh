#!/usr/bin/env bash
# Full integration stress: unit tests → max runtime_config → full-load → daemon + MariaDB txn simulator.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONN_ID="${CONN_ID:-MARIADB_LOCAL}"
TIER="${TIER:-bronze}"
TXN_DURATION_SEC="${TXN_DURATION_SEC:-120}"
TXN_INTERVAL_SEC="${TXN_INTERVAL_SEC:-1}"
SKIP_FULL_LOAD="${SKIP_FULL_LOAD:-0}"
DATASYNC_BIN="${DATASYNC_BIN:-$ROOT/cpp/build/DataSync}"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"

log() { printf '==> %s\n' "$*"; }
die() { printf '✖ %s\n' "$*" >&2; exit 1; }

[[ -f "$CONFIG" ]] || die "missing $CONFIG"

pg_py() {
  python3 - "$CONFIG" <<'PY' "$@"
import json, sys
import psycopg2
cfg = json.load(open(sys.argv[1]))
pg = cfg["datasync"]
sql = sys.argv[2]
args = sys.argv[3:]
with psycopg2.connect(
    host=pg["host"], port=pg.get("port", 5432), user=pg["user"],
    password=pg["password"], dbname=pg["database"],
) as conn:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        if cur.description:
            for row in cur.fetchall():
                print("\t".join("" if v is None else str(v) for v in row))
        else:
            conn.commit()
PY
}

fix_mariadb_port() {
  local port
  port="$(pg_py "$CONFIG" "SELECT port FROM cdc_catalog.connections WHERE alias=%s" "$CONN_ID" | head -1)"
  if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', int('$port'))); s.close()" 2>/dev/null; then
    log "MariaDB reachable on catalog port $port"
    return
  fi
  if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 3306)); s.close()" 2>/dev/null; then
    log "Port $port down — overriding to 3306 for local stress"
    export MARIADB_PORT_OVERRIDE=3306
    pg_py "$CONFIG" "UPDATE cdc_catalog.connections SET port=3306, updated_at=now() WHERE alias=%s" "$CONN_ID"
  else
    die "MariaDB not reachable on $port or 3306"
  fi
}

apply_max_runtime() {
  log "Applying max-throughput runtime_config (capture/apply/full_load)"
  pg_py "$CONFIG" "
    INSERT INTO cdc_catalog.runtime_config (config_key, component, conn_id, config_value, description)
    VALUES
      ('full_load_batch_size', 'mariadb_load', '', '50000'::jsonb, 'stress test'),
      ('full_load_workers', 'mariadb_load', '', '4'::jsonb, 'stress test'),
      ('full_load_parallel_tables', 'mariadb_load', '', '4'::jsonb, 'stress test'),
      ('capture_max_seconds', 'cdc_kafka_capture', '', '60'::jsonb, 'stress test'),
      ('capture_max_events', 'cdc_kafka_capture', '', '500000'::jsonb, 'stress test'),
      ('apply_max_seconds', 'cdc_kafka_apply', '', '60'::jsonb, 'stress test'),
      ('apply_max_events', 'cdc_kafka_apply', '', '500000'::jsonb, 'stress test'),
      ('apply_batch_size', 'cdc_kafka_apply', '', '5000'::jsonb, 'stress test')
    ON CONFLICT (config_key, component, conn_id)
    DO UPDATE SET config_value = EXCLUDED.config_value, updated_at = now()
  "
}

verify_results() {
  log "Verification — apply_batch_stats (last 5 min)"
  pg_py "$CONFIG" "
    SELECT logged_at, source_schema, source_table, events_total, events_inserts, events_updates, events_deletes
    FROM cdc_catalog.apply_batch_stats
    WHERE conn_id = %s AND service_tier::text = lower(%s)
      AND logged_at > now() - interval '5 minutes'
    ORDER BY logged_at DESC LIMIT 10
  " "$CONN_ID" "$TIER" || true

  log "Verification — logs errors (last 5 min)"
  pg_py "$CONFIG" "
    SELECT logged_at, component, message
    FROM cdc_catalog.logs
    WHERE conn_id = %s AND level = 'error' AND created_at > now() - interval '5 minutes'
    ORDER BY created_at DESC LIMIT 10
  " "$CONN_ID" || true

  local applied
  applied="$(pg_py "$CONFIG" "
    SELECT coalesce(sum(events_total), 0)
    FROM cdc_catalog.apply_batch_stats
    WHERE conn_id = %s AND service_tier::text = lower(%s)
      AND logged_at > now() - interval '5 minutes'
  " "$CONN_ID" "$TIER" | head -1)"
  if [[ "${applied:-0}" -gt 0 ]]; then
    log "✔ Stress OK — events applied in window: $applied"
  else
    die "No apply_batch_stats activity in last 5 minutes"
  fi
}

log "Step 1/7 — C++ unit tests"
"$ROOT/scripts/run_unit_tests.sh"

log "Step 2/7 — Build DataSync binary"
cmake -S "$ROOT/cpp" -B "$ROOT/cpp/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "$ROOT/cpp/build" --target DataSync -j"$(nproc)"
DATASYNC_BIN="$ROOT/cpp/build/DataSync"

fix_mariadb_port

log "Step 3/7 — Bootstrap MariaDB stress table"
python3 "$ROOT/scripts/mariadb_txn_simulator.py" \
  --conn-id "$CONN_ID" --bootstrap-only

apply_max_runtime

log "Step 4/7 — Discover catalog"
"$DATASYNC_BIN" --config "$CONFIG" discover

if [[ "$SKIP_FULL_LOAD" != "1" ]]; then
  log "Step 5/7 — Full-load MAX (re-load smoke table)"
  pg_py "$CONFIG" "
    UPDATE cdc_catalog.catalog
    SET needs_full_load = true, status = 'success', last_error = NULL,
        engine_meta = coalesce(engine_meta, '{}'::jsonb) - 'full_load_fail_count'
    WHERE conn_id = %s AND service_tier::text = lower(%s) AND active
  " "$CONN_ID" "$TIER"
  "$DATASYNC_BIN" --config "$CONFIG" full-load --tier "$TIER" --conn-id "$CONN_ID" || true
else
  log "Step 5/7 — SKIP full-load (SKIP_FULL_LOAD=1)"
fi

log "Step 6/7 — CDC stress: txn simulator + daemon (${TXN_DURATION_SEC}s)"
python3 "$ROOT/scripts/mariadb_txn_simulator.py" \
  --conn-id "$CONN_ID" \
  --duration "$TXN_DURATION_SEC" \
  --interval "$TXN_INTERVAL_SEC" &
SIM_PID=$!
trap 'kill $SIM_PID 2>/dev/null || true' EXIT

DAEMON_END=$((SECONDS + TXN_DURATION_SEC))
while (( SECONDS < DAEMON_END )); do
  "$DATASYNC_BIN" --config "$CONFIG" daemon --once || true
  sleep 2
done
wait "$SIM_PID" 2>/dev/null || true
trap - EXIT

log "Step 7/7 — Verify"
verify_results
log "Integration stress complete"
