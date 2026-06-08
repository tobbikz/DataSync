#!/usr/bin/env bash
# Stop DataSync daemon (Docker container or SIGTERM to native process via systemd).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"

case "${DATASYNC_DEPLOY_MODE}" in
  native)
    # systemd sends SIGTERM to the main process (Type=simple).
    ;;
  docker|*)
    docker_compose stop datasync || true
    ;;
esac
