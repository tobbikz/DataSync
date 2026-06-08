#!/usr/bin/env bash
# Install CDC stack — delega en setup_all.sh (un solo comando)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec "$ROOT/scripts/setup_all.sh" "$@"
