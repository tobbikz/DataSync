#!/usr/bin/env bash
# Stop DataSync stack (Kafka + daemon containers).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready || exit 0
stop_compose_all
