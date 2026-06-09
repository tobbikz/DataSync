#!/usr/bin/env bash
# Start DataSync daemon container (Kafka ensured by datasync-ensure-kafka.sh / unit deps).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready
stop_compose_datasync
ensure_kafka_ready
docker_compose up -d --force-recreate datasync
