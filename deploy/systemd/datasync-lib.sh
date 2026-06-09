#!/usr/bin/env bash
# Shared helpers for DataSync systemd wrappers.
set -euo pipefail

DATASYNC_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASYNC_ROOT="${DATASYNC_ROOT:-$(cd "${DATASYNC_LIB_DIR}/../.." && pwd)}"

ENV_FILE="${DATASYNC_ENV_FILE:-/etc/datasync/datasync.env}"
if [[ -f "${ENV_FILE}" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "${ENV_FILE}"
  set +a
fi

DATASYNC_ROOT="${DATASYNC_ROOT:-$(cd "${DATASYNC_LIB_DIR}/../.." && pwd)}"

# shellcheck source=scripts/container-compose.sh
source "${DATASYNC_ROOT}/scripts/container-compose.sh"

ensure_podman_ready() {
  local uid="${DATASYNC_UID:-$(id -u)}"
  export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/${uid}}"
  export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${XDG_RUNTIME_DIR}/bus}"

  if ensure_container_runtime; then
    return 0
  fi

  if command -v systemctl >/dev/null 2>&1; then
    systemctl --user start podman.socket 2>/dev/null || true
    sleep 2
  fi

  ensure_container_runtime
}

wait_kafka() {
  local i h state
  for i in $(seq 1 90); do
    state=$(docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true)
    h=$(docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$state" == "running" && "$h" == "healthy" ]]; then
      return 0
    fi
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      return 1
    fi
    sleep 2
  done
  return 1
}

datasync_build() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"
  docker_compose build datasync
}

stop_compose_datasync() {
  ensure_podman_ready || return 0
  cd "${DATASYNC_ROOT}"
  docker_compose stop datasync 2>/dev/null || true
}
