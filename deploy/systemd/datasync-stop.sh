#!/usr/bin/env bash
# Stop DataSync daemon container (systemd DataSync.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready || exit 0
docker_compose stop datasync || true
