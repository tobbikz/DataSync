#!/usr/bin/env bash
# Start DataSync (Docker daemon container or native binary foreground).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_single_daemon

case "${DATASYNC_DEPLOY_MODE}" in
  native)
    BIN="${DATASYNC_BIN:-${DATASYNC_ROOT}/cpp/build/DataSync}"
    CONFIG="${DATASYNC_CONFIG:-${DATASYNC_ROOT}/config.json}"
    export DATASYNC_CONFIG
    exec "${BIN}" daemon
    ;;
  docker|*)
    docker_compose stop zookeeper 2>/dev/null || true
    docker_compose rm -f zookeeper 2>/dev/null || true
    docker_compose up -d --remove-orphans kafka
    wait_kafka || {
      echo "Kafka not ready — check: docker compose logs kafka --tail 40" >&2
      exit 1
    }
    docker_compose up -d --force-recreate datasync
    ;;
esac
