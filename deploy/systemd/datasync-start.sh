#!/usr/bin/env bash
# Start DataSync daemon container only (Kafka via DataSync-kafka.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready
stop_compose_datasync
wait_kafka || {
  echo "Kafka not ready — start DataSync-kafka.service first" >&2
  exit 1
}
docker_compose up -d --force-recreate datasync
