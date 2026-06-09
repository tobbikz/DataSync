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
  for i in $(seq 1 120); do
    state=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
    h=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$state" == "running" ]]; then
      if (echo >/dev/tcp/localhost/9092) 2>/dev/null; then
        return 0
      fi
      if [[ "$h" == *healthy* ]]; then
        return 0
      fi
    fi
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      break
    fi
    sleep 2
  done
  return 1
}

start_kafka() {
  if systemctl is-enabled --quiet DataSync-kafka.service 2>/dev/null; then
    printf '✔ Kafka managed by systemd (skip compose up)\n'
    return 0
  fi

  local clean="${ROOT}/deploy/systemd/kafka-force-clean.sh"
  if [[ -f "$clean" ]]; then
    /bin/bash "$clean" 2>/dev/null || true
  fi

  docker_compose stop zookeeper 2>/dev/null || true
  docker_compose rm -f zookeeper 2>/dev/null || true

  local kstate khealth
  kstate=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
  khealth=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)

  if [[ "$kstate" == "running" ]]; then
    if (echo >/dev/tcp/localhost/9092) 2>/dev/null || [[ "$khealth" == *healthy* ]]; then
      docker_compose up -d kafka
      wait_kafka
      return $?
    fi
  fi

  docker_compose up -d --force-recreate --remove-orphans kafka
  wait_kafka
}

run_discover() {
  printf '✔ Discover skipped (run manually after onboarding sources)\n'
  return 0
}

stop_host_native_datasync() {
  local pid cmd
  while read -r pid; do
    [[ -z "${pid}" ]] && continue
    cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
    [[ "${cmd}" == *"DataSync daemon"* ]] || continue
    kill "${pid}" 2>/dev/null || true
  done < <(pgrep -f 'DataSync daemon' 2>/dev/null || true)
}

start_datasync_daemon() {
  if systemctl is-enabled --quiet DataSync.service 2>/dev/null; then
    printf '✔ DataSync daemon managed by systemd (skip compose up)\n'
    return 0
  fi
  stop_host_native_datasync
  docker_compose up -d --no-recreate datasync
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

systemd_units_installed() {
  [[ -f /etc/systemd/system/DataSync.service && -f /etc/systemd/system/DataSync-kafka.service ]]
}

install_systemd_if_missing() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 1
  [[ -x "$ROOT/deploy/systemd/install-systemd.sh" ]] || return 1
  if systemd_units_installed; then
    return 0
  fi

  printf '→ Installing systemd units (DataSync-kafka + DataSync)...\n'
  if [[ "${EUID}" -eq 0 ]]; then
    "$ROOT/deploy/systemd/install-systemd.sh"
    return 0
  fi
  if command -v sudo >/dev/null 2>&1; then
    sudo "$ROOT/deploy/systemd/install-systemd.sh"
    return 0
  fi
  warn "sudo required — run: sudo $ROOT/deploy/systemd/install-systemd.sh"
  return 1
}

restart_systemd_stack() {
  if ! systemctl is-enabled --quiet DataSync.service 2>/dev/null; then
    return 1
  fi
  local clean="${ROOT}/deploy/systemd/kafka-force-clean.sh"
  if [[ -x "$clean" ]]; then
    if [[ "${EUID}" -eq 0 ]]; then
      /bin/bash "$clean"
    elif command -v sudo >/dev/null 2>&1; then
      sudo /bin/bash "$clean"
    fi
  fi
  if [[ "${EUID}" -eq 0 ]]; then
    systemctl restart DataSync-kafka.service DataSync.service
  elif command -v sudo >/dev/null 2>&1; then
    sudo systemctl restart DataSync-kafka.service DataSync.service
  else
    warn "sudo required to restart systemd units"
    return 1
  fi
}

print_systemd_hint() {
  [[ "${SKIP_SYSTEMD:-0}" == "1" ]] && return 0
  systemd_units_installed && return 0
  [[ -x "$ROOT/deploy/systemd/install-systemd.sh" ]] || return 0

  cat <<EOF

── systemd (not installed — run once with sudo) ──
  sudo $ROOT/deploy/systemd/install-systemd.sh

EOF
}

ensure_container_engine
ensure_config

build_image
apply_catalog_schema

install_systemd_if_missing || true

if restart_systemd_stack; then
  printf '✔ Systemd stack restarted (DataSync-kafka + DataSync)\n'
elif systemctl is-enabled --quiet DataSync-kafka.service 2>/dev/null; then
  run_quiet "Kafka starting" start_kafka
  run_quiet "DataSync daemon" start_datasync_daemon
else
  run_quiet "Kafka starting" start_kafka
  run_quiet "DataSync daemon" start_datasync_daemon
fi

sleep 2
run_quiet "Post-install health" post_install_health
run_discover
print_systemd_hint

printf '✔ Install complete — status: %s compose ps | discover: %s compose run --rm datasync discover\n' \
  "$(container_runtime_label)" "$(container_runtime_label)"
