#!/usr/bin/env bash
# Start DataSync (Docker daemon container or native binary foreground).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"

case "${DATASYNC_DEPLOY_MODE}" in
  native)
    BIN="${DATASYNC_BIN:-${DATASYNC_ROOT}/cpp/build/DataSync}"
    CONFIG="${DATASYNC_CONFIG:-${DATASYNC_ROOT}/config.json}"
    export DATASYNC_CONFIG
    exec "${BIN}" daemon
    ;;
  docker|*)
    docker_compose up -d --remove-orphans zookeeper
    wait_zookeeper || {
      echo "Zookeeper not ready — check: docker compose logs zookeeper --tail 30" >&2
      exit 1
    }
    docker_compose up -d kafka
    wait_kafka || {
      echo "Kafka not ready — check: docker compose logs kafka --tail 40" >&2
      exit 1
    }
    docker_compose up -d --force-recreate datasync
    ;;
esac
