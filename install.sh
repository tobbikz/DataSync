#!/usr/bin/env bash
# DataSync — Docker install (Kafka + daemon). Idempotent schema bootstrap.
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
  warn "podman or docker required — start rootless podman: systemctl --user start podman.socket"
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

wait_kafka() {
  local i h state
  for i in $(seq 1 90); do
    state=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
    h=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$state" == "running" && "$h" == "healthy" ]]; then
      printf '✔ Kafka ready\n'
      return 0
    fi
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      break
    fi
    sleep 2
  done
  warn "Kafka not ready — check: $(container_runtime_label) compose logs kafka --tail 40"
  return 1
}

start_kafka() {
  docker_compose stop zookeeper 2>/dev/null || true
  docker_compose rm -f zookeeper 2>/dev/null || true

  local kstate khealth
  kstate=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
  khealth=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)

  if [[ "$kstate" == "running" && "$khealth" == "healthy" ]]; then
    docker_compose up -d kafka
    wait_kafka
    return $?
  fi

  docker_compose up -d --force-recreate --remove-orphans kafka
  wait_kafka
}

run_discover() {
  printf '✔ Discover skipped (run manually after onboarding sources)\n'
  return 0
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

print_systemd_instructions() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 0
  [[ -x "$ROOT/deploy/systemd/install-systemd.sh" ]] || return 0

  cat <<EOF

── systemd (optional, run once with sudo) ──
  sudo $ROOT/deploy/systemd/install-systemd.sh

  Installs + enables DataSync (CDC daemon) and reconcile timer (auto light/full every 4h).
  Uses your user ($USER) for rootless podman — no port 9092 conflict.

  After git pull or config change:
  sudo systemctl restart DataSync

  Status:
  systemctl status DataSync DataSync-reconcile.timer

EOF
}

ensure_container_engine
ensure_config

build_image
apply_catalog_schema

run_quiet "Kafka starting" start_kafka

run_quiet "DataSync daemon" docker_compose up -d --no-recreate datasync

sleep 2
run_quiet "Post-install health" post_install_health
run_discover
print_systemd_instructions

printf '✔ Install complete — status: %s compose ps | discover: %s compose run --rm datasync discover\n' \
  "$(container_runtime_label)" "$(container_runtime_label)"
