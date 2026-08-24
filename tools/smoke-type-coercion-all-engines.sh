#!/usr/bin/env bash
# Assert coercion-audit scans all three engines (requires bootstrap-dev-engines.sh).
set -euo pipefail
ROOT="${DATASYNC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"
log() { printf '[coercion-smoke] %s\n' "$*"; }
TMP=$(mktemp)
./tools/audit-type-coercion.sh 2>/dev/null >"$TMP"
python3 - "$TMP" <<'PY'
import json, sys
raw = open(sys.argv[1]).read()
start = raw.find("{")
if start < 0:
    sys.exit("no JSON from coercion-audit")
r = json.loads(raw[start:])
for eng in ("mariadb", "mssql", "mongodb"):
    b = r[eng]
    print(f"{eng}: scanned={b['scanned']} stale={b['stale']}")
    if b["scanned"] < 1:
        sys.exit(f"FAIL: {eng} scanned=0")
print("coercion-smoke OK")
PY
rm -f "$TMP"
log "done"
