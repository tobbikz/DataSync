#!/usr/bin/env bash
# Start Kafka container (systemd DataSync-kafka.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready
docker_compose stop zookeeper 2>/dev/null || true
docker_compose rm -f zookeeper 2>/dev/null || true
docker_compose up -d --remove-orphans kafka
wait_kafka_compose
