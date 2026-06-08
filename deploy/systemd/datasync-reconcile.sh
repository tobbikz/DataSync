#!/usr/bin/env bash
# One-shot reconcile loop (timer or manual).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"

case "${DATASYNC_DEPLOY_MODE}" in
  native)
    BIN="${DATASYNC_BIN:-${DATASYNC_ROOT}/cpp/build/DataSync}"
    export DATASYNC_CONFIG="${DATASYNC_CONFIG:-${DATASYNC_ROOT}/config.json}"
    exec "${BIN}" reconcile-loop --once
    ;;
  docker|*)
    docker_compose run --rm \
      -e DATASYNC_HOST_NETWORK=1 \
      -e KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" \
      datasync reconcile-loop --once
    ;;
esac
