#!/usr/bin/env bash
# Run DataSync CLI (reads config.json from project root).
# Usage: deploy/systemd/datasync-cli.sh discover
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${DATASYNC_BIN:-$ROOT/cpp/build/DataSync}"

if [[ ! -f "$ROOT/config.json" ]]; then
  echo "Missing $ROOT/config.json — copy from config.json.example" >&2
  exit 1
fi

cd "$ROOT"
exec "$BIN" "$@"
