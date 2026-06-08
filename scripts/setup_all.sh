#!/usr/bin/env bash
# One-shot: install + GTID + Kafka + C++ build + unit tests + E2E
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PGPASSWORD="${PGPASSWORD:-Yucaquemada1}"
export MARIADB_PASSWORD="${MARIADB_PASSWORD:-Yucaquemada1}"

SUDO_OK=0
if sudo -n true 2>/dev/null; then
  SUDO_OK=1
elif [[ -t 0 ]] && sudo true 2>/dev/null; then
  SUDO_OK=1
fi

have_cmd() { command -v "$1" >/dev/null 2>&1; }

install_pacman_if_missing() {
  local -a pkgs=()
  local pkg
  for pkg in "$@"; do
    if ! pacman -Q "$pkg" >/dev/null 2>&1; then
      pkgs+=("$pkg")
    fi
  done
  if ((${#pkgs[@]} == 0)); then
    return 0
  fi
  echo "Instalando paquetes faltantes: ${pkgs[*]}"
  if ! sudo pacman -S --needed --noconfirm "${pkgs[@]}"; then
    echo "WARN: pacman falló (conflictos de versión en el sistema)."
    echo "      Si cmake/mariadb/docker ya funcionan, el setup continúa."
    return 1
  fi
}

docker_cmd() {
  if docker info >/dev/null 2>&1; then
    docker "$@"
  else
    sudo docker "$@"
  fi
}

docker_compose() {
  if docker info >/dev/null 2>&1; then
    docker compose "$@"
  else
    sudo docker compose "$@"
  fi
}

ensure_test_mariadb() {
  if mariadb -h 127.0.0.1 -P 3307 -u tomy.berrios -p"$MARIADB_PASSWORD" -e "SELECT 1" >/dev/null 2>&1; then
    echo "MariaDB test :3307 OK"
    return 0
  fi
  echo "=== Starting MariaDB test instance :3307 ==="
  local test_dir="/tmp/datalake-mariadb-test"
  mkdir -p "$test_dir"/{data,run,tmp,binlog}
  if [[ ! -f "$test_dir/my.cnf" ]]; then
    cat >"$test_dir/my.cnf" <<EOF
[mysqld]
datadir = $test_dir/data
port = 3307
socket = $test_dir/run/mysqld.sock
pid-file = $test_dir/run/mysqld.pid
tmpdir = $test_dir/tmp
log_error = $test_dir/mysqld.log
log_bin = $test_dir/binlog/mysql-bin
binlog_format = ROW
server_id = 2
gtid_strict_mode = ON
bind-address = 127.0.0.1

[client]
socket = $test_dir/run/mysqld.sock
port = 3307
EOF
  fi
  if [[ ! -d "$test_dir/data/mysql" ]]; then
    mariadb-install-db --user="$USER" --datadir="$test_dir/data" --skip-test-db 2>/dev/null \
      || mysql_install_db --user="$USER" --datadir="$test_dir/data" 2>/dev/null \
      || true
  fi
  local sock="$test_dir/run/mysqld.sock"
  bootstrap_test_mariadb_users() {
    mariadb --socket="$sock" -u "$USER" -e "
      CREATE USER IF NOT EXISTS 'tomy.berrios'@'%' IDENTIFIED BY '${MARIADB_PASSWORD}';
      CREATE USER IF NOT EXISTS 'tomy.berrios'@'localhost' IDENTIFIED BY '${MARIADB_PASSWORD}';
      GRANT ALL ON *.* TO 'tomy.berrios'@'%' WITH GRANT OPTION;
      GRANT ALL ON *.* TO 'tomy.berrios'@'localhost' WITH GRANT OPTION;
      FLUSH PRIVILEGES;
    "
  }
  if [[ -f "$test_dir/run/mysqld.pid" ]]; then
    kill "$(cat "$test_dir/run/mysqld.pid")" 2>/dev/null || true
    sleep 1
  fi
  rm -f "$test_dir/run/mysqld.sock" "$test_dir/run/mysqld.pid"
  nohup mariadbd --defaults-file="$test_dir/my.cnf" >>"$test_dir/mysqld.log" 2>&1 &
  local ready=0
  for i in $(seq 1 30); do
    if mariadb --socket="$sock" -u "$USER" -e "SELECT 1" >/dev/null 2>&1; then
      bootstrap_test_mariadb_users 2>/dev/null && ready=1 && break
    fi
    sleep 1
  done
  if [[ "$ready" -eq 0 ]]; then
    echo "Reinicializando datadir test (auth root incompatible)..."
    kill "$(cat "$test_dir/run/mysqld.pid" 2>/dev/null)" 2>/dev/null || true
    sleep 1
    rm -rf "$test_dir/data"
    mariadb-install-db --user="$USER" --datadir="$test_dir/data" --skip-test-db
    rm -f "$test_dir/run/mysqld.sock" "$test_dir/run/mysqld.pid"
    nohup mariadbd --defaults-file="$test_dir/my.cnf" >>"$test_dir/mysqld.log" 2>&1 &
    for i in $(seq 1 30); do
      if mariadb --socket="$sock" -u "$USER" -e "SELECT 1" >/dev/null 2>&1; then
        bootstrap_test_mariadb_users
        ready=1
        break
      fi
      sleep 1
    done
  fi
  mariadb -h 127.0.0.1 -P 3307 -u tomy.berrios -p"$MARIADB_PASSWORD" -e "SELECT 1"
  echo "MariaDB test :3307 started"
}

echo "=========================================="
echo " CDC stack — setup completo"
echo "=========================================="

echo "=== 1/7 Paquetes sistema (sudo) ==="
if [[ "$SUDO_OK" -eq 1 ]]; then
  if command -v pacman >/dev/null 2>&1; then
    # Solo instalar lo que falta — no forzar upgrade de gcc/mariadb (rompe libgccjit, etc.)
    install_pacman_if_missing docker docker-compose python python-pip jq curl cmake pkgconf \
      postgresql-libs nlohmann-json || true
    if ! have_cmd mariadb; then
      install_pacman_if_missing mariadb-clients || true
    fi
    if ! have_cmd g++; then
      install_pacman_if_missing gcc || true
    fi
  fi
  for cmd in cmake g++ mariadb psql python3 docker; do
    if have_cmd "$cmd"; then
      echo "  OK $cmd"
    else
      echo "  FALTA $cmd — instálalo manualmente"
    fi
  done
  sudo systemctl enable --now docker 2>/dev/null || true
  sudo usermod -aG docker "$USER" 2>/dev/null || true
elif [[ "$SUDO_OK" -eq 0 ]]; then
  echo "WARN: omitiendo pacman/docker (ejecuta este script en tu terminal para sudo interactivo)"
fi

echo "=== 2/7 SQL migrations (DataSync + DataLake) ==="
psql -h localhost -U tomy.berrios -d DataSync -v ON_ERROR_STOP=1 \
  -f "$ROOT/sql/007_cdc_kafka_control_plane.sql" \
  -f "$ROOT/sql/008_cdc_apply_runtime_config.sql" \
  -f "$ROOT/sql/009_cdc_health_views.sql" \
  -f "$ROOT/sql/010_cdc_native_capture.sql" \
  -f "$ROOT/sql/011_cdc_scale_runtime.sql" \
  -f "$ROOT/sql/012_lake_partitions_workers.sql" \
  -f "$ROOT/sql/013_cdc_daemon_runtime.sql" \
  -f "$ROOT/sql/014_cdc_reconciliation.sql" \
  -f "$ROOT/sql/015_cdc_reconcile_schedule.sql" \
  -f "$ROOT/sql/031_cdc_fast_parallel_daemon.sql" \
  -f "$ROOT/sql/032_drop_service_tiers.sql" \
  -f "$ROOT/sql/034_drop_legacy_tables_apply_op_counts_tiers.sql" \
  -f "$ROOT/sql/035_drop_cdc_binlog_position.sql" \
  -f "$ROOT/sql/036_apply_batch_stats.sql" \
  -f "$ROOT/sql/037_apply_batch_stats_health.sql" \
  -f "$ROOT/sql/038_apply_batch_stats_metrics.sql" \
  -f "$ROOT/sql/039_consolidate_lake_observability.sql" \
  -f "$ROOT/sql/023_cdc_mssql_lsn.sql" \
  -f "$ROOT/sql/024_cdc_mssql_apply_runtime.sql" \
  -f "$ROOT/sql/025_mssql_load_runtime.sql" \
  -f "$ROOT/sql/040_mssql_mariadb_parity.sql" \
  -f "$ROOT/sql/041_capture_conn_parity.sql" \
  -f "$ROOT/sql/042_reconcile_pipeline.sql" \
  -f "$ROOT/sql/043_apply_batch_stats_host_metrics.sql" \
  -f "$ROOT/sql/044_reconcile_auto_4h_full.sql" \
  -f "$ROOT/sql/045_apply_batch_stats_process_mem.sql"
mariadb -h 127.0.0.1 -P 3306 -u tomy.berrios -p"$MARIADB_PASSWORD" < "$ROOT/sql/029_mariadb_cdc_meta.sql" 2>/dev/null || true

echo "=== 3/7 MariaDB GTID + binlog prod :3306 (sudo) ==="
if [[ "$SUDO_OK" -eq 1 ]]; then
  sudo "$ROOT/scripts/enable_mariadb_gtid.sh"
else
  echo "WARN: omitiendo GTID/binlog — corre: sudo $ROOT/scripts/enable_mariadb_gtid.sh"
fi

echo "=== 4/7 Kafka (Docker) ==="
KAFKA_OK=0
if docker_compose -f "$ROOT/scripts/cdc_kafka/docker/docker-compose.kafka.yml" up -d; then
  for i in $(seq 1 30); do
    if docker_compose -f "$ROOT/scripts/cdc_kafka/docker/docker-compose.kafka.yml" exec -T kafka \
      kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
      KAFKA_OK=1
      break
    fi
    sleep 2
  done
fi
if [[ "$KAFKA_OK" -eq 0 ]]; then
  echo "WARN: Kafka no disponible — build continúa"
  echo "      Levanta: docker compose -f scripts/cdc_kafka/docker/docker-compose.kafka.yml up -d"
fi

echo "=== 5/5 Build C++ DataSync ==="
cmake -S "$ROOT/cpp" -B "$ROOT/cpp/build" -q 2>/dev/null || cmake -S "$ROOT/cpp" -B "$ROOT/cpp/build"
cmake --build "$ROOT/cpp/build" -j"$(nproc)"

echo ""
echo "=========================================="
echo " SETUP COMPLETO"
echo "=========================================="
echo "CDC stress: [[CDC Kafka Architecture]] (Obsidian) — stress-load --setup + daemon"
