#!/usr/bin/env bash
# DataSync — Kafka + daemon in Podman. Prod: systemd user datalake.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

for arg in "$@"; do
  case "$arg" in
    -h|--help)
      sed -n '2,8p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

warn() { printf '✖ %s\n' "$*" >&2; }

# shellcheck source=scripts/container-compose.sh
source "$ROOT/scripts/container-compose.sh"

run_quiet() {
  local label="$1"; shift
  local err
  err="$(mktemp)"
  if "$@" >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ %s\n' "$label"
    return 0
  fi
  printf '✖ %s\n' "$label" >&2
  tail -20 "$err" >&2 || true
  rm -f "$err"
  return 1
}

ensure_container_engine() {
  if ensure_container_runtime; then
    printf '✔ Container runtime (%s)\n' "$(container_runtime_label)"
    return 0
  fi
  warn "podman or docker required — rootless: systemctl --user start podman.socket"
  exit 1
}

ensure_config() {
  if [[ ! -f "$ROOT/config.json" ]]; then
    if [[ -f "$ROOT/config.json.example" ]]; then
      cp "$ROOT/config.json.example" "$ROOT/config.json"
      warn "created config.json from example — edit passwords and re-run"
      exit 1
    fi
    warn "missing config.json"
    exit 1
  fi
}

build_image() {
  local t0=$SECONDS err
  err="$(mktemp)"
  if docker_compose build --quiet datasync >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ Image datasync built %ss\n' "$((SECONDS - t0))"
    return 0
  fi
  if docker_compose build datasync >"$err" 2>&1; then
    rm -f "$err"
    printf '✔ Image datasync built %ss\n' "$((SECONDS - t0))"
    return 0
  fi
  printf '✖ Image datasync build failed\n' >&2
  tail -30 "$err" >&2 || true
  rm -f "$err"
  return 1
}

apply_catalog_schema() {
  [[ -f "$ROOT/sql/backup/cdc_catalog_schema_structure.sql" ]] || { warn "missing catalog schema sql"; exit 1; }
  local err rc
  err="$(mktemp)"
  docker_compose run --rm --no-deps --remove-orphans \
    -e DATASYNC_RUN_MIGRATIONS=1 \
    -e DATASYNC_INSTALL_QUIET=1 \
    -e DATASYNC_HOST_NETWORK=1 \
    -e KAFKA_BOOTSTRAP=localhost:9092 \
    datasync schema-only >"$err" 2>&1
  rc=$?
  grep -vE 'Container datasync-datasync-run|^$' "$err" || true
  rm -f "$err"
  if [[ "$rc" -ne 0 ]]; then
    exit "$rc"
  fi
}

kafka_tcp_ok() {
  (echo >/dev/tcp/localhost/9092) 2>/dev/null
}

wait_kafka_tcp() {
  local i
  for i in $(seq 1 90); do
    kafka_tcp_ok && return 0
    sleep 2
  done
  return 1
}

start_kafka_dev() {
  if systemctl is-enabled --quiet DataSync-kafka.service 2>/dev/null; then
    return 0
  fi
  docker_compose stop zookeeper 2>/dev/null || true
  docker_compose up -d --remove-orphans kafka
  wait_kafka_tcp
}

start_datasync_dev() {
  if systemctl is-enabled --quiet DataSync.service 2>/dev/null; then
    printf '✔ DataSync managed by systemd\n'
    return 0
  fi
  docker_compose up -d --force-recreate datasync
}

post_install_health() {
  local err rc
  err="$(mktemp)"
  docker_compose run --rm --no-deps --remove-orphans \
    -e DATASYNC_INSTALL_QUIET=1 \
    -e DATASYNC_HOST_NETWORK=1 \
    -e KAFKA_BOOTSTRAP=localhost:9092 \
    datasync health-only >"$err" 2>&1
  rc=$?
  grep -vE 'Container datasync-datasync-run|^$' "$err" || true
  rm -f "$err"
  if [[ "$rc" -eq 0 ]]; then
    printf '✔ Post-install health\n'
    return 0
  fi
  warn "post-install health failed"
  return 1
}

sync_systemd_prod() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 1
  [[ -x "$ROOT/deploy/systemd/install-systemd.sh" ]] || return 1

  printf '→ Syncing prod systemd (Podman Kafka + DataSync)...\n'
  if [[ "${EUID}" -eq 0 ]]; then
    "$ROOT/deploy/systemd/install-systemd.sh"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo "$ROOT/deploy/systemd/install-systemd.sh"
    return 0
  fi
  warn "sudo required for prod — see deploy/PROD.md"
  return 1
}

run_discover() {
  printf '✔ Discover skipped (run manually after onboarding sources)\n'
  return 0
}

print_dev_hint() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 0
  systemctl is-enabled --quiet DataSync.service 2>/dev/null && return 0
  cat <<EOF

── Dev mode (no systemd) ──
  podman compose ps
  podman compose logs -f kafka datasync

Prod once: sudo $ROOT/deploy/systemd/install-systemd.sh
See: $ROOT/deploy/PROD.md

EOF
}

ensure_container_engine
ensure_config

build_image
apply_catalog_schema

if sync_systemd_prod; then
  printf '✔ Prod stack ready (systemd + Podman as datalake)\n'
else
  run_quiet "Kafka (compose)" start_kafka_dev
  run_quiet "DataSync daemon" start_datasync_dev
fi

sleep 2
run_quiet "Post-install health" post_install_health
run_discover
print_dev_hint

printf '✔ Install complete\n'
