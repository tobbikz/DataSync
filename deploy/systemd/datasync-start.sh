#!/usr/bin/env bash
# Start DataSync stack: Kafka + CDC daemon (single compose project).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready
stop_compose_all
docker_compose stop zookeeper 2>/dev/null || true
docker_compose rm -f zookeeper 2>/dev/null || true
docker_compose up -d --remove-orphans kafka
wait_kafka_compose
docker_compose up -d --force-recreate datasync
