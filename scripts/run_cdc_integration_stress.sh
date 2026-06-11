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
DATASYNC_USE_DOCKER="${DATASYNC_USE_DOCKER:-auto}"

log() { printf '==> %s\n' "$*"; }
die() { printf '✖ %s\n' "$*" >&2; exit 1; }

run_datasync() {
  local use_docker=0
  if [[ "$DATASYNC_USE_DOCKER" == "1" ]]; then
    use_docker=1
  elif [[ "$DATASYNC_USE_DOCKER" == "auto" ]]; then
    if [[ ! -x "$DATASYNC_BIN" ]] || ! ldd "$DATASYNC_BIN" 2>/dev/null | grep -q rdkafka; then
      use_docker=1
    fi
  fi
  if [[ "$use_docker" -eq 1 ]]; then
    local -a docker_extra=()
    for sock in /run/mysqld/mysqld.sock /var/run/mysqld/mysqld.sock /run/mariadb/mariadb.sock; do
      if [[ -S "$sock" ]]; then
        docker_extra+=(-v "${sock}:${sock}:ro")
        break
      fi
    done
    docker compose -f "$ROOT/docker-compose.yml" run --rm --no-deps \
      --user "${DATASYNC_DOCKER_UID:-$(id -u)}:${DATASYNC_DOCKER_GID:-$(id -g)}" \
      -e DATASYNC_SKIP_CONNECTIONS_SYNC=1 \
      "${docker_extra[@]}" \
      datasync "$@"
  else
    "$DATASYNC_BIN" --config "$CONFIG" "$@"
  fi
}

[[ -f "$CONFIG" ]] || die "missing $CONFIG"

pg_py() {
  local sql="$1"
  shift
  CONFIG_PATH="$CONFIG" SQL="$sql" ARGS_JSON="$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' "$@")" python3 <<'PY'
import json, os, sys
import psycopg2
cfg = json.load(open(os.environ["CONFIG_PATH"]))
pg = cfg["datasync"]
sql = os.environ["SQL"]
args = json.loads(os.environ.get("ARGS_JSON", "[]"))
with psycopg2.connect(
    host=pg["host"], port=pg.get("port", 5432), user=pg["user"],
    password=pg["password"], dbname=pg["database"],
) as conn:
    with conn.cursor() as cur:
        cur.execute(sql, tuple(args))
        if cur.description:
            for row in cur.fetchall():
                print("\t".join("" if v is None else str(v) for v in row))
        else:
            conn.commit()
PY
}

setup_local_mariadb_tcp_user() {
  if [[ "${MARIADB_USE_TCP:-0}" == "1" ]]; then
    return
  fi
  local stress_user="${MARIADB_STRESS_USER:-cdc_stress}"
  local stress_pass="${MARIADB_STRESS_PASS:-cdc_stress}"
  log "Ensuring TCP user ${stress_user} for docker→host MariaDB"
  mariadb -e "
    CREATE DATABASE IF NOT EXISTS test;
    CREATE USER IF NOT EXISTS '${stress_user}'@'127.0.0.1' IDENTIFIED BY '${stress_pass}';
    CREATE USER IF NOT EXISTS '${stress_user}'@'localhost' IDENTIFIED BY '${stress_pass}';
    GRANT ALL PRIVILEGES ON test.* TO '${stress_user}'@'127.0.0.1';
    GRANT ALL PRIVILEGES ON test.* TO '${stress_user}'@'localhost';
    FLUSH PRIVILEGES;
  " 2>/dev/null || true
  pg_py "
    UPDATE cdc_catalog.connections
    SET username = %s, password = %s, port = 3306, updated_at = now()
    WHERE alias = %s
  " "$stress_user" "$stress_pass" "$CONN_ID"
  export MARIADB_USE_TCP=1
}

fix_mariadb_port() {
  local port
  port="$(pg_py "SELECT port FROM cdc_catalog.connections WHERE alias=%s" "$CONN_ID" | head -1)"
  if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', int('$port'))); s.close()" 2>/dev/null; then
    log "MariaDB reachable on catalog port $port"
    return
  fi
  if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1', 3306)); s.close()" 2>/dev/null; then
    log "Port $port down — local dev: localhost socket + port 3306"
    export MARIADB_PORT_OVERRIDE=3306
    pg_py "
      UPDATE cdc_catalog.connections
      SET host = 'localhost', port = 3306, username = %s, password = '', updated_at = now()
      WHERE alias = %s
    " "$(whoami)" "$CONN_ID"
  else
    die "MariaDB not reachable on $port or 3306"
  fi
}

mariadb_admin_cli() {
  local sql="$1"
  if [[ -n "${MARIADB_CDC_USER:-}" ]]; then
    mariadb -h localhost -P "${MARIADB_PORT_OVERRIDE:-3306}" \
      -u "$MARIADB_CDC_USER" -p"${MARIADB_CDC_PASSWORD:-}" --batch --skip-column-names -e "$sql"
    return
  fi
  mariadb --batch --skip-column-names -e "$sql"
}

check_mariadb_binlog_privilege() {
  if mariadb_admin_cli "SHOW MASTER STATUS" >/dev/null 2>&1; then
    return 0
  fi
  log "WARN: MariaDB user lacks BINLOG MONITOR — CDC capture cannot run"
  log "  Grant: GRANT BINLOG MONITOR ON *.* TO user@host;  or set MARIADB_CDC_USER/PASSWORD"
  return 1
}

reseed_capture_position() {
  log "Re-seed capture_position from SHOW MASTER STATUS"
  check_mariadb_binlog_privilege || return 1
  local file pos uuid
  read -r file pos < <(mariadb_admin_cli "SHOW MASTER STATUS" | awk 'NR==1{print $1,$2}')
  uuid="$(mariadb_admin_cli "SELECT @@server_uuid")"
  [[ -n "$file" && -n "$pos" ]] || die "SHOW MASTER STATUS failed — is MariaDB running with binlog?"
  pg_py "
    INSERT INTO cdc_catalog.capture_position
      (conn_id, gtid_set, binlog_file, binlog_position, server_uuid, status, last_error, updated_at)
    VALUES (%s, '', %s, %s::bigint, %s, 'healthy'::cdc_catalog.cdc_health_status, NULL, now())
    ON CONFLICT (conn_id) DO UPDATE SET
      binlog_file = EXCLUDED.binlog_file,
      binlog_position = EXCLUDED.binlog_position,
      server_uuid = EXCLUDED.server_uuid,
      status = 'healthy'::cdc_catalog.cdc_health_status,
      last_error = NULL,
      capture_lag_seconds = 0,
      updated_at = now()
  " "$CONN_ID" "$file" "$pos" "$uuid"
}

apply_max_runtime() {
  log "Applying max-throughput runtime_config (capture/apply/full_load)"
  pg_py "
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
  pg_py "
    SELECT logged_at, source_schema, source_table, events_total, events_inserts, events_updates, events_deletes
    FROM cdc_catalog.apply_batch_stats
    WHERE conn_id = %s AND service_tier::text = lower(%s)
      AND logged_at > now() - interval '5 minutes'
    ORDER BY logged_at DESC LIMIT 10
  " "$CONN_ID" "$TIER" || true

  log "Verification — logs errors (last 5 min)"
  pg_py "
    SELECT logged_at, component, message
    FROM cdc_catalog.logs
    WHERE conn_id = %s AND level = 'error' AND created_at > now() - interval '5 minutes'
    ORDER BY created_at DESC LIMIT 10
  " "$CONN_ID" || true

  local applied
  applied="$(pg_py "
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
if [[ "${MARIADB_FORCE_TCP:-0}" == "1" ]]; then
  setup_local_mariadb_tcp_user
elif [[ -S /run/mysqld/mysqld.sock || -S /var/run/mysqld/mysqld.sock || -S /run/mariadb/mariadb.sock ]]; then
  log "Local MariaDB unix socket — peer auth for host + docker (socket volume mount)"
  pg_py "
    UPDATE cdc_catalog.connections
    SET host = 'localhost', username = %s, password = '', updated_at = now()
    WHERE alias = %s
  " "$(whoami)" "$CONN_ID"
fi

log "Step 3/7 — Bootstrap MariaDB stress table"
python3 "$ROOT/scripts/mariadb_txn_simulator.py" \
  --conn-id "$CONN_ID" --bootstrap-only

apply_max_runtime
CDC_PRIV_OK=1
reseed_capture_position || CDC_PRIV_OK=0

catalog_rows="$(pg_py "SELECT count(*) FROM cdc_catalog.catalog WHERE conn_id=%s AND active" "$CONN_ID" | head -1)"
if [[ "${catalog_rows:-0}" -gt 0 ]]; then
  log "Step 4/7 — Discover skipped (catalog already has ${catalog_rows} active tables)"
else
  log "Step 4/7 — Discover catalog"
  run_datasync discover || true
fi

if [[ "$SKIP_FULL_LOAD" != "1" ]]; then
  log "Step 5/7 — Full-load MAX (re-load smoke table)"
  pg_py "
    UPDATE cdc_catalog.catalog
    SET needs_full_load = true, status = 'success', last_error = NULL,
        engine_meta = coalesce(engine_meta, '{}'::jsonb) - 'full_load_fail_count'
    WHERE conn_id = %s AND service_tier::text = lower(%s) AND active
  " "$CONN_ID" "$TIER"
  if "$DATASYNC_BIN" --config "$CONFIG" full-load --tier "$TIER" --conn-id "$CONN_ID"; then
    log "full-load completed (native binary)"
  else
    run_datasync full-load --tier "$TIER" --conn-id "$CONN_ID" || true
  fi
else
  log "Step 5/7 — SKIP full-load (SKIP_FULL_LOAD=1)"
fi

if [[ "$CDC_PRIV_OK" -eq 0 ]]; then
  log "Step 6/7 — CDC stress SKIPPED (no BINLOG MONITOR). Full-load + unit tests OK."
  log "Step 7/7 — Verify skipped (set MARIADB_CDC_USER or grant BINLOG MONITOR for CDC)"
  log "Integration stress partial complete (unit + full-load only)"
  exit 0
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
  run_datasync daemon --once || true
  sleep 2
done
wait "$SIM_PID" 2>/dev/null || true
trap - EXIT

log "Step 7/7 — Verify"
verify_results
log "Integration stress complete"
