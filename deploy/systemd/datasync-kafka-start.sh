#!/usr/bin/env bash
# Start Kafka only (systemd DataSync-kafka.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

ensure_kafka_ready
