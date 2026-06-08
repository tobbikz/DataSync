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

docker_compose() {
  if docker info >/dev/null 2>&1; then
    docker compose "$@"
  else
    sudo docker compose "$@"
  fi
}

wait_zookeeper() {
  local i h
  for i in $(seq 1 30); do
    h=$(docker_compose ps zookeeper --format '{{.Health}}' 2>/dev/null | head -1 || true)
    if [[ "$h" == "healthy" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_kafka() {
  local i h state
  for i in $(seq 1 60); do
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
