#!/usr/bin/env bash
# Stop Kafka (systemd DataSync-kafka.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
docker_compose stop kafka || true
