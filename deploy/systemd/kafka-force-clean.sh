#!/usr/bin/env bash
# Stop every Kafka on :9092 and remove compose kafka containers (clean slate before compose up).
# Root: all podman users + fuser. Non-root: current user's podman/compose only.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASYNC_ROOT="${DATASYNC_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
DATASYNC_USER="${DATASYNC_RUN_USER:-datalake}"

ENV_FILE="${DATASYNC_ENV_FILE:-/etc/datasync/datasync.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

# shellcheck source=scripts/container-compose.sh
source "${DATASYNC_ROOT}/scripts/container-compose.sh"

kafka_port_busy() {
  (echo >/dev/tcp/127.0.0.1/"${KAFKA_PORT}") 2>/dev/null
}

compose_stop_kafka() {
  local root="$1" user="$2" runtime="$3" dbus="$4"
  [[ -d "$root" ]] || return 0
  runuser -u "$user" -- \
    env XDG_RUNTIME_DIR="${runtime}" \
        DBUS_SESSION_BUS_ADDRESS="${dbus}" \
        DOCKER_HOST="unix://${runtime}/podman/podman.sock" \
        DATASYNC_ROOT="${root}" \
    bash -c "cd '${root}' && podman compose stop kafka datasync 2>/dev/null; \
      podman compose rm -f -s kafka 2>/dev/null; true" 2>/dev/null || true
}

podman_rm_kafka_publishers() {
  local user="$1" runtime="$2" dbus="$3"
  runuser -u "$user" -- \
    env XDG_RUNTIME_DIR="${runtime}" DBUS_SESSION_BUS_ADDRESS="${dbus}" \
    bash -c "
      command -v podman >/dev/null 2>&1 || exit 0
      podman ps -aq --filter publish=${KAFKA_PORT} 2>/dev/null | xargs -r podman rm -f 2>/dev/null || true
      podman ps -aq --filter name=kafka 2>/dev/null | xargs -r podman rm -f 2>/dev/null || true
    " 2>/dev/null || true
}

local_compose_clean() {
  ensure_container_runtime || return 0
  cd "${DATASYNC_ROOT}"
  docker_compose stop datasync kafka 2>/dev/null || true
  docker_compose rm -f -s kafka 2>/dev/null || true
  if command -v podman >/dev/null 2>&1; then
    podman ps -aq --filter "publish=${KAFKA_PORT}" 2>/dev/null | xargs -r podman rm -f 2>/dev/null || true
    podman ps -aq --filter "name=kafka" 2>/dev/null | xargs -r podman rm -f 2>/dev/null || true
  fi
}

wait_port_free() {
  local i
  for i in $(seq 1 20); do
    kafka_port_busy || return 0
    sleep 1
  done
  return 1
}

stop_native_kafka() {
  systemctl stop DataSync-kafka.service 2>/dev/null || true
  if kafka_port_busy && command -v fuser >/dev/null 2>&1; then
    echo "Stopping processes on TCP ${KAFKA_PORT}..." >&2
    fuser -k "${KAFKA_PORT}/tcp" 2>/dev/null || true
    sleep 2
  fi
}

root_clean_all() {
  stop_native_kafka
  local u uid runtime dbus repo_owner
  repo_owner="$(stat -c '%U' "${DATASYNC_ROOT}" 2>/dev/null || true)"

  for u in "${DATASYNC_USER}" "${repo_owner}"; do
    [[ -n "$u" ]] || continue
    id "$u" &>/dev/null || continue
    uid="$(id -u "$u")"
    runtime="/run/user/${uid}"
    dbus="unix:path=${runtime}/bus"
    compose_stop_kafka "${DATASYNC_ROOT}" "$u" "$runtime" "$dbus"
    podman_rm_kafka_publishers "$u" "$runtime" "$dbus"
  done

  if kafka_port_busy && command -v fuser >/dev/null 2>&1; then
    echo "Freeing TCP ${KAFKA_PORT} (fuser)..." >&2
    fuser -k "${KAFKA_PORT}/tcp" 2>/dev/null || true
    sleep 2
  fi
}

main() {
  if [[ "${EUID}" -eq 0 ]]; then
    root_clean_all
  else
    local_compose_clean
  fi

  if wait_port_free; then
    echo "Port ${KAFKA_PORT} free — ready for compose up" >&2
    return 0
  fi

  echo "ERROR: port ${KAFKA_PORT} still in use after cleanup" >&2
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Run: sudo $0" >&2
  fi
  ss -Htn "sport = :${KAFKA_PORT}" 2>/dev/null || true
  return 1
}

main "$@"
