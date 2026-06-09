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

kafka_tcp_ok() {
  local bootstrap="${KAFKA_BOOTSTRAP:-localhost:9092}"
  local host="${bootstrap%%:*}"
  local port="${bootstrap##*:}"
  [[ -n "$host" && -n "$port" ]] || return 1
  (echo >/dev/tcp/"$host"/"$port") 2>/dev/null
}

kafka_compose_state() {
  docker_compose ps kafka --format '{{.State}}' 2>/dev/null | head -1 || true
}

kafka_compose_health() {
  docker_compose ps kafka --format '{{.Health}}' 2>/dev/null | head -1 || true
}

kafka_is_ready() {
  if ! kafka_tcp_ok; then
    return 1
  fi
  local state
  state="$(kafka_compose_state)"
  [[ -z "$state" || "$state" == "running" ]]
}

wait_kafka() {
  local i state
  for i in $(seq 1 120); do
    state="$(kafka_compose_state)"
    if [[ "$state" == "running" ]] && kafka_tcp_ok; then
      return 0
    fi
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      return 1
    fi
    sleep 2
  done
  [[ "$(kafka_compose_state)" == "running" ]] && kafka_tcp_ok
}

reset_kafka_stack() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"
  if [[ -x "${DATASYNC_ROOT}/deploy/systemd/kafka-force-clean.sh" ]]; then
    /bin/bash "${DATASYNC_ROOT}/deploy/systemd/kafka-force-clean.sh"
    return $?
  fi
  docker_compose stop kafka datasync 2>/dev/null || true
  docker_compose rm -f -s kafka 2>/dev/null || true
  ! kafka_tcp_ok
}

start_kafka_compose() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"
  docker_compose stop zookeeper 2>/dev/null || true
  docker_compose rm -f zookeeper 2>/dev/null || true
  docker_compose up -d --remove-orphans kafka
}

ensure_kafka_ready() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"

  echo "Resetting Kafka stack (stop old containers, free :9092)..." >&2
  reset_kafka_stack || {
    echo "Port 9092 still in use — run: sudo ${DATASYNC_ROOT}/deploy/systemd/kafka-force-clean.sh" >&2
    return 1
  }

  echo "Starting Kafka via compose..." >&2
  start_kafka_compose || return 1

  if wait_kafka; then
    echo "Kafka ready on ${KAFKA_BOOTSTRAP:-localhost:9092}" >&2
    return 0
  fi

  echo "Kafka not ready — logs:" >&2
  docker_compose logs kafka --tail 40 >&2 || true
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
