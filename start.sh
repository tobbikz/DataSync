#!/usr/bin/env bash
# DataSync — bootstrap + build + migrations in one shot.
#
# Usage (from anywhere):
#   ./start.sh
#   ./start.sh --skip-docker
#   ./start.sh --systemd          # also install + enable DataSync.service (sudo)
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

SKIP_DOCKER=0
INSTALL_SYSTEMD=0
for arg in "$@"; do
  case "$arg" in
    --skip-docker) SKIP_DOCKER=1 ;;
    --systemd) INSTALL_SYSTEMD=1 ;;
    -h|--help)
      sed -n '2,8p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

docker_compose() {
  if docker info >/dev/null 2>&1; then
    docker compose "$@"
  else
    sudo docker compose "$@"
  fi
}

read_pg_from_config() {
  python3 - "$ROOT/config.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    cfg = json.load(f)
ds = cfg["datasync"]
for k in ("host", "port", "database", "user", "password"):
    if k not in ds or ds[k] in ("", "CHANGE_ME"):
        raise SystemExit(f"config.json: datasync.{k} missing or CHANGE_ME")
print(ds["host"])
print(ds["port"])
print(ds["database"])
print(ds["user"])
print(ds["password"])
PY
}

ensure_config() {
  if [[ ! -f "$ROOT/config.json" ]]; then
    if [[ -f "$ROOT/config.json.example" ]]; then
      log "Creating config.json from config.json.example"
      cp "$ROOT/config.json.example" "$ROOT/config.json"
      warn "Edit $ROOT/config.json (passwords) and run ./start.sh again"
      exit 1
    fi
    echo "Missing config.json — copy config.json.example" >&2
    exit 1
  fi
  if grep -q 'CHANGE_ME' "$ROOT/config.json" 2>/dev/null; then
    warn "config.json still has CHANGE_ME — edit credentials first"
    exit 1
  fi
}

ensure_build_deps() {
  log "Checking build dependencies"
  local -a missing=()
  for cmd in cmake g++ pkg-config psql python3; do
    have_cmd "$cmd" || missing+=("$cmd")
  done
  if ((${#missing[@]})); then
    warn "Missing: ${missing[*]} — install with pacman and re-run"
    exit 1
  fi
  if ! pkg-config --exists libpq; then
    warn "libpq dev headers missing (postgresql-libs)"
    exit 1
  fi
  if ! pkg-config --exists libmariadb; then
    warn "mariadb client dev missing (mariadb-libs)"
    exit 1
  fi
}

maybe_install_freetds() {
  if pkg-config --exists freetds 2>/dev/null || pacman -Q freetds &>/dev/null 2>&1; then
    return 0
  fi
  if sudo -n true 2>/dev/null; then
    log "Installing freetds (MSSQL support)"
    sudo pacman -S --needed --noconfirm freetds || warn "freetds install failed — MSSQL capture disabled"
  else
    warn "freetds not installed — run: sudo pacman -S freetds"
  fi
}

run_migrations() {
  log "SQL migrations → PostgreSQL (from config.json datasync)"
  mapfile -t pg_cfg < <(read_pg_from_config)
  local pg_host="${pg_cfg[0]}" pg_port="${pg_cfg[1]}" pg_db="${pg_cfg[2]}"
  local pg_user="${pg_cfg[3]}" pg_pass="${pg_cfg[4]}"
  export PGPASSWORD="$pg_pass"

  local -a sql_files=()
  local f
  for f in "$ROOT"/sql/[0-9][0-9][0-9]_*.sql; do
    [[ -f "$f" ]] && sql_files+=("$f")
  done
  IFS=$'\n' sql_files=($(printf '%s\n' "${sql_files[@]}" | sort))
  unset IFS

  for f in "${sql_files[@]}"; do
    echo "  $(basename "$f")"
    psql -h "$pg_host" -p "$pg_port" -U "$pg_user" -d "$pg_db" \
      -v ON_ERROR_STOP=0 -f "$f" >/dev/null 2>&1 || true
  done

  # MariaDB CDC meta (source side)
  if have_cmd mariadb && [[ -n "${MARIADB_PASSWORD:-}" ]]; then
    mariadb -h 127.0.0.1 -P 3306 -u tomy.berrios -p"$MARIADB_PASSWORD" \
      < "$ROOT/sql/029_mariadb_cdc_meta.sql" >/dev/null 2>&1 || true
  fi
}

start_kafka() {
  [[ "$SKIP_DOCKER" -eq 1 ]] && { log "Skipping Kafka (--skip-docker)"; return 0; }
  if nc -z localhost 9092 2>/dev/null || (command -v curl >/dev/null && curl -sf http://localhost:8080 >/dev/null 2>&1); then
    log "Kafka already reachable on :9092"
    return 0
  fi
  local compose="$ROOT/scripts/cdc_kafka/docker/docker-compose.kafka.yml"
  if [[ ! -f "$compose" ]]; then
    warn "Kafka compose not found"
    return 0
  fi
  log "Starting Kafka (Docker)"
  if ! docker_compose -f "$compose" up -d; then
    warn "Kafka start failed — CDC capture/apply need Kafka"
    return 0
  fi
  local i
  for i in $(seq 1 30); do
    if docker_compose -f "$compose" exec -T kafka \
      kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
      log "Kafka ready"
      return 0
    fi
    sleep 2
  done
  warn "Kafka not ready yet — check: docker compose -f $compose ps"
}

build_datasync() {
  log "Building DataSync (Release)"
  cmake -S "$ROOT/cpp" -B "$ROOT/cpp/build" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$ROOT/cpp/build" --target DataSync -j"$(nproc)"
  log "Binary: $ROOT/cpp/build/DataSync"
}

smoke_discover() {
  log "Smoke: DataSync discover"
  cd "$ROOT"
  if "$ROOT/cpp/build/DataSync" discover; then
    log "Discover OK"
  else
    warn "Discover failed — check config.json and cdc_catalog.connections"
    return 1
  fi
}

maybe_systemd() {
  [[ "$INSTALL_SYSTEMD" -eq 1 ]] || return 0
  log "Installing systemd unit (sudo)"
  sudo "$ROOT/deploy/systemd/install-systemd.sh"
  sudo systemctl enable --now DataSync
}

echo "=========================================="
echo " DataSync start — $(date '+%Y-%m-%d %H:%M')"
echo "=========================================="

ensure_config
ensure_build_deps
maybe_install_freetds
run_migrations
start_kafka
build_datasync
smoke_discover || true
maybe_systemd

echo ""
echo "=========================================="
echo " READY"
echo "=========================================="
echo "  Config:     $ROOT/config.json"
echo "  Binary:     $ROOT/cpp/build/DataSync"
echo "  CLI:        deploy/systemd/datasync-cli.sh discover"
echo "  Full load:  deploy/systemd/datasync-cli.sh full-load --tier platinum"
echo "  Daemon:     deploy/systemd/datasync-cli.sh daemon --once"
echo "  Production: sudo ./start.sh --systemd"
echo "              journalctl -u DataSync -f"
echo ""
