#!/usr/bin/env bash
# Ensure Kafka is up before build/start (ExecStartPre on DataSync.service).
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"

ensure_kafka_ready
