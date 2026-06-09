#!/usr/bin/env bash
# Stop Kafka container owned by this compose project (skip if Kafka runs elsewhere).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"
ensure_podman_ready || exit 0

state="$(kafka_compose_state)"
if [[ -z "$state" || "$state" == "unknown" ]]; then
  exit 0
fi

docker_compose stop kafka || true
