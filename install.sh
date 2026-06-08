#!/usr/bin/env bash
# DataSync — Docker-only install (Kafka + DataSync daemon).
#
# Does everything: config bootstrap, build, schema, daemon, discover, log tail.
# Does NOT provision PostgreSQL, MariaDB, MSSQL, or MongoDB — use config.json.
#
# Requires: Docker + Docker Compose v2
#
# Usage:
#   ./install.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      sed -n '2,11p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }

docker_compose() {
  if docker info >/dev/null 2>&1; then
    docker compose "$@"
  else
    sudo docker compose "$@"
  fi
}

ensure_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required — install Docker Engine and re-run ./install.sh" >&2
    exit 1
  fi
  if ! docker compose version >/dev/null 2>&1 && ! sudo docker compose version >/dev/null 2>&1; then
    echo "docker compose v2 plugin is required" >&2
    exit 1
  fi
}

ensure_config() {
  if [[ ! -f "$ROOT/config.json" ]]; then
    if [[ -f "$ROOT/config.json.example" ]]; then
      log "Creating config.json from config.json.example"
      cp "$ROOT/config.json.example" "$ROOT/config.json"
      warn "Edit $ROOT/config.json (password) and re-run ./install.sh"
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

wait_service() {
  local svc="$1" max="${2:-60}"
  log "Waiting for $svc"
  local i
  for i in $(seq 1 "$max"); do
    if docker_compose ps --status running "$svc" 2>/dev/null | grep -q "$svc"; then
      local health
      health=$(docker_compose ps "$svc" 2>/dev/null | awk -v s="$svc" '$1 == s {print $4}' || true)
      if [[ "$health" == *"(healthy)"* ]] || [[ "$health" != *"health"* ]]; then
        log "$svc ready"
        return 0
      fi
    fi
    sleep 2
  done
  warn "$svc not ready — check: docker compose ps"
  return 1
}

apply_catalog_schema() {
  log "Applying catalog schema (Docker, idempotent)"
  docker_compose run --rm --no-deps \
    -e DATASYNC_RUN_MIGRATIONS=1 \
    -e DATASYNC_HOST_NETWORK=1 \
    -e KAFKA_BOOTSTRAP=localhost:9092 \
    datasync schema-only
}

run_discover() {
  log "Running catalog discover"
  if docker_compose run --rm \
      -e DATASYNC_HOST_NETWORK=1 \
      -e KAFKA_BOOTSTRAP=localhost:9092 \
      datasync discover; then
    log "Discover completed"
  else
    warn "Discover finished with errors — seed cdc_catalog.connections if empty, then re-run ./install.sh"
  fi
}

show_daemon_logs() {
  log "DataSync daemon logs (recent — follow with: docker compose logs -f datasync)"
  docker_compose logs datasync --tail 40
}

install_systemd_units() {
  if [[ "${SKIP_SYSTEMD:-0}" == "1" ]]; then
    warn "SKIP_SYSTEMD=1 — skipping systemd install"
    return 0
  fi
  if [[ ! -x "$ROOT/deploy/systemd/install-systemd.sh" ]]; then
    warn "deploy/systemd/install-systemd.sh missing — skip systemd"
    return 0
  fi
  log "Installing systemd units (requires root — rebuild on every restart)"
  if [[ "${EUID}" -eq 0 ]]; then
    "$ROOT/deploy/systemd/install-systemd.sh"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$ROOT/deploy/systemd/install-systemd.sh"
  else
    warn "Run manually: sudo deploy/systemd/install-systemd.sh"
    return 0
  fi
  log "Optional: sudo systemctl enable --now DataSync DataSync-reconcile.timer"
}

echo "=========================================="
echo " DataSync install (Docker) — $(date '+%Y-%m-%d %H:%M')"
echo "=========================================="

ensure_docker
ensure_config

log "Building images"
docker_compose build datasync

log "Starting Kafka stack (Zookeeper + Kafka)"
docker_compose up -d zookeeper kafka

wait_service kafka 90 || true

apply_catalog_schema

log "Starting DataSync daemon"
docker_compose up -d datasync

sleep 2
run_discover
show_daemon_logs
install_systemd_units

echo ""
echo "=========================================="
echo " READY"
echo "=========================================="
echo "  Config:      $ROOT/config.json"
echo "  Catalog DDL: sql/backup/cdc_catalog_schema_structure.sql"
echo "  Kafka:       localhost:9092"
echo ""
echo "  Databases (PG, MariaDB, MSSQL, Mongo) are external — not created by Docker."
echo "  PG in config.json: localhost:5432 when on the same host (network_mode: host)."
echo ""
echo "  Follow daemon:"
echo "    docker compose logs -f datasync"
echo ""
echo "  systemd (rebuild + restart):"
echo "    sudo systemctl enable --now DataSync"
echo "    sudo systemctl restart DataSync   # rebuilds image/binary first"
echo "    sudo systemctl enable --now DataSync-reconcile.timer"
echo ""
