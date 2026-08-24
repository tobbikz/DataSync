#!/usr/bin/env bash
# Dry-run or apply MariaDB type-coercion audit (BIT/boolean, datetime/timestamptz).
# Docs: Obsidian DataSync/Type Coercion Reload.md
set -euo pipefail

ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
CONN_ID="${CONN_ID:-}"
IMAGE="${DATASYNC_IMAGE:-datasync:local}"
APPLY="${APPLY:-0}"

log() { printf '[coercion-audit] %s\n' "$*"; }

args=(coercion-audit)
if [[ -n "$CONN_ID" ]]; then
  args+=(--conn-id "$CONN_ID")
fi
if [[ "$APPLY" == "1" || "$APPLY" == "true" || "$APPLY" == "yes" ]]; then
  args+=(--apply)
  log "apply mode: stale tables will be flagged needs_full_load=true"
else
  log "dry-run (set APPLY=1 to flag catalog rows)"
fi

docker run --rm --network host \
  -v "$CONFIG:/app/config.json:ro" \
  -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
  -e DATASYNC_CONFIG=/app/config.json \
  "$IMAGE" "${args[@]}"
