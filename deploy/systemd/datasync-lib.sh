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
DATASYNC_DEPLOY_MODE="${DATASYNC_DEPLOY_MODE:-docker}"

# shellcheck source=scripts/container-compose.sh
source "${DATASYNC_ROOT}/scripts/container-compose.sh"

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

native_build() {
  local build_dir="${DATASYNC_ROOT}/cpp/build"
  cmake -S "${DATASYNC_ROOT}/cpp" -B "${build_dir}" -DCMAKE_BUILD_TYPE=Release
  cmake --build "${build_dir}" --target DataSync -j"$(nproc)"
}

datasync_build() {
  case "${DATASYNC_DEPLOY_MODE}" in
    native)
      native_build
      ;;
    docker|*)
      cd "${DATASYNC_ROOT}"
      docker_compose build datasync
      ;;
  esac
}

# Host-native DataSync daemon (not the /usr/local/bin binary inside the compose container).
stop_host_native_datasync() {
  local pid cmd
  while read -r pid; do
    [[ -z "${pid}" ]] && continue
    cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
    [[ "${cmd}" == *"DataSync daemon"* ]] || continue
    [[ "${cmd}" == *"/usr/local/bin/DataSync"* ]] && continue
    kill "${pid}" 2>/dev/null || true
  done < <(pgrep -f 'DataSync daemon' 2>/dev/null || true)
}

stop_compose_datasync() {
  cd "${DATASYNC_ROOT}"
  docker_compose stop datasync 2>/dev/null || true
}

# Exactly one CDC daemon: docker container OR native binary — never both on Kafka.
ensure_single_daemon() {
  case "${DATASYNC_DEPLOY_MODE}" in
    native)
      stop_compose_datasync
      ;;
    docker|*)
      stop_host_native_datasync
      ;;
  esac
}
