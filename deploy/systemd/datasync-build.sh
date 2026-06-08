#!/usr/bin/env bash
# Rebuild DataSync (Docker image or native binary). Used as systemd ExecStartPre.
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"
datasync_build
