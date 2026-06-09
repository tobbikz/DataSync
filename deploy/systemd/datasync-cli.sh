#!/usr/bin/env bash
# Run DataSync CLI via Docker compose.
# Usage: deploy/systemd/datasync-cli.sh discover
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

cd "${DATASYNC_ROOT}"

if [[ ! -f "${DATASYNC_CONFIG:-${DATASYNC_ROOT}/config.json}" ]]; then
  echo "Missing config.json — copy from config.json.example" >&2
  exit 1
fi

ensure_podman_ready

docker_compose run --rm \
  -e DATASYNC_HOST_NETWORK=1 \
  -e KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}" \
  datasync "$@"
