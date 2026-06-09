#!/usr/bin/env bash
# Rebuild DataSync Docker image. Used as systemd ExecStartPre.
set -euo pipefail
# shellcheck source=deploy/systemd/datasync-lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/datasync-lib.sh"
datasync_build
