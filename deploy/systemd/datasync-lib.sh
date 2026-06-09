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
  # TCP wins — Kafka may run in another user's rootless podman on the same host.
  if kafka_tcp_ok; then
    return 0
  fi

  local state health
  state="$(kafka_compose_state)"
  health="$(kafka_compose_health | tr '()' ' ')"

  [[ "$state" == "running" ]] && [[ "$health" == *healthy* ]]
}

wait_kafka() {
  local i state
  for i in $(seq 1 120); do
    if kafka_is_ready; then
      return 0
    fi
    state="$(kafka_compose_state)"
    if [[ "$state" == "exited" || "$state" == "dead" ]]; then
      return 1
    fi
    sleep 2
  done
  kafka_tcp_ok
}

start_kafka_compose() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"

  if kafka_tcp_ok; then
    echo "Kafka already listening on ${KAFKA_BOOTSTRAP:-localhost:9092} — skip compose up" >&2
    return 0
  fi

  docker_compose stop zookeeper 2>/dev/null || true
  docker_compose rm -f zookeeper 2>/dev/null || true
  if docker_compose up -d --remove-orphans kafka; then
    return 0
  fi

  if kafka_tcp_ok; then
    echo "Port 9092 in use by existing Kafka — continuing (shared host broker)" >&2
    return 0
  fi

  echo "Kafka compose up failed and :9092 is not reachable" >&2
  return 1
}

ensure_kafka_ready() {
  ensure_podman_ready
  cd "${DATASYNC_ROOT}"

  if kafka_tcp_ok; then
    echo "Kafka reachable at ${KAFKA_BOOTSTRAP:-localhost:9092}" >&2
    return 0
  fi

  if wait_kafka; then
    return 0
  fi

  echo "Kafka not ready — starting compose kafka service..." >&2
  start_kafka_compose || return 1

  if wait_kafka; then
    return 0
  fi

  if kafka_tcp_ok; then
    return 0
  fi

  echo "Kafka still not ready — logs:" >&2
  docker_compose logs kafka --tail 30 >&2 || true
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
