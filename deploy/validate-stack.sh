#!/usr/bin/env bash
# Validate DataSync compose stack (run on prod as datalake or via install-systemd).
# Usage: ./deploy/validate-stack.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# shellcheck source=deploy/container-compose.sh
source "$ROOT/deploy/container-compose.sh"

failures=0
ok_msg() { printf '✔ %s\n' "$*"; }
fail_msg() { printf '✖ %s\n' "$*" >&2; failures=$((failures + 1)); }

check_file() {
  local path="$1" label="$2"
  if [[ -f "$path" ]]; then
    ok_msg "$label"
  else
    fail_msg "$label missing: $path"
  fi
}

check_json_keys() {
  python3 - "$ROOT/config.json" <<'PY' || return 1
import json, sys
path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    cfg = json.load(f)
for section in ("datasync", "datalake"):
    sec = cfg.get(section, {})
    if not isinstance(sec, dict):
        raise SystemExit(f"missing or invalid {section}")
    for key in ("host", "port", "database", "user", "password"):
        if not sec.get(key):
            raise SystemExit(f"{section}.{key} empty")
cdc = cfg.get("cdc", {})
tiers = cdc.get("tiers", []) if isinstance(cdc, dict) else []
if not tiers:
    raise SystemExit("cdc.tiers empty — daemon will exit")
sources = cfg.get("sources", [])
legacy = any(cfg.get(k) for k in ("mariadb", "mssql", "mongodb"))
if not sources and not legacy:
    print("WARN: no sources[] in config.json — connections must exist in cdc_catalog.connections")
PY
}

ensure_container_runtime || fail_msg "podman/docker not available"
check_file "$ROOT/config.json" "config.json"
check_file "$ROOT/sql/backup/cdc_catalog_schema_structure.sql" "catalog schema SQL"
check_file "$ROOT/sql/backup/datalake_lake_schema.sql" "lake schema SQL"

if [[ -f "$ROOT/config.json" ]]; then
  if check_json_keys; then
    ok_msg "config.json structure"
  else
    fail_msg "config.json structure invalid"
  fi
  if [[ ! -r "$ROOT/config.json" ]]; then
    fail_msg "config.json not readable by $(id -un)"
  fi
fi

if (echo >/dev/tcp/127.0.0.1/9092) 2>/dev/null; then
  ok_msg "Kafka TCP 127.0.0.1:9092"
else
  fail_msg "Kafka TCP 127.0.0.1:9092 unreachable"
fi

kafka_status="$(docker_compose ps kafka --format '{{.Status}}' 2>/dev/null | head -1 || true)"
if [[ "$kafka_status" == *healthy* ]]; then
  ok_msg "kafka container healthy"
elif [[ "$kafka_status" == *running* ]]; then
  fail_msg "kafka running but not healthy yet ($kafka_status)"
else
  fail_msg "kafka container not running"
fi

ds_state="$(docker_compose ps datasync --format '{{.Status}}' 2>/dev/null | head -1 || true)"
if [[ "$ds_state" == *running* ]] && [[ "$ds_state" != *Exited* ]]; then
  ok_msg "datasync container running"
elif [[ -z "$ds_state" ]]; then
  fail_msg "datasync container not created — run: sudo systemctl restart DataSync"
else
  fail_msg "datasync container state: ${ds_state}"
fi

if docker_compose run --rm --no-deps --remove-orphans \
  -e DATASYNC_INSTALL_QUIET=1 \
  -e DATASYNC_HOST_NETWORK=1 \
  -e KAFKA_BOOTSTRAP=127.0.0.1:9092 \
  datasync health-only >/tmp/datasync-health-$$.log 2>&1; then
  ok_msg "health-only"
  grep '^✔' /tmp/datasync-health-$$.log || true
else
  fail_msg "health-only failed"
  tail -15 /tmp/datasync-health-$$.log >&2 || true
fi
rm -f /tmp/datasync-health-$$.log

if [[ "$failures" -gt 0 ]]; then
  echo ""
  echo "Validation failed ($failures issue(s)). See deploy/PROD.md"
  exit 1
fi

echo ""
echo "Stack OK"
