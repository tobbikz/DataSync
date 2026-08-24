#!/usr/bin/env bash
# Docker/Podman ENTRYPOINT for the datasync image — not for host use.
# Host ops: ./install.sh initial | start | stop
set -euo pipefail

CONFIG="${DATASYNC_CONFIG:-/app/config.json}"
ROOT="${DATASYNC_ROOT:-/app}"
BIN="${DATASYNC_BIN:-/usr/local/bin/DataSync}"

fail() { printf '✖ %s\n' "$*" >&2; exit 1; }

read_pg_cfg() {
  python3 - "$CONFIG" <<'PY'
import json, sys, os
with open(sys.argv[1]) as f:
    ds = json.load(f)["datasync"]
host = ds["host"]
if (os.path.exists("/.dockerenv")
        and os.environ.get("DATASYNC_HOST_NETWORK") != "1"
        and host in ("localhost", "127.0.0.1")):
    host = "host.docker.internal"
sslmode = ds.get("sslmode", os.environ.get("DATASYNC_PG_SSLMODE", ""))
for val in (host, ds["port"], ds["database"], ds["user"], ds["password"], sslmode):
    print(val)
PY
}

read_datalake_cfg() {
  python3 - "$CONFIG" <<'PY'
import json, sys, os
with open(sys.argv[1]) as f:
    root = json.load(f)
ds = root["datasync"]
if root.get("datalake") and isinstance(root["datalake"], dict):
    dl = root["datalake"]
    host = dl.get("host", ds["host"])
    port = dl.get("port", ds["port"])
    database = dl.get("database", ds["database"])
    user = dl.get("user", ds["user"])
    password = dl.get("password", ds["password"])
    sslmode = dl.get("sslmode", ds.get("sslmode", ""))
else:
    host = ds["host"]
    port = ds["port"]
    database = ds.get("database", "DataLake")
    user = ds["user"]
    password = ds["password"]
    sslmode = ds.get("sslmode", "")
if (os.path.exists("/.dockerenv")
        and os.environ.get("DATASYNC_HOST_NETWORK") != "1"
        and host in ("localhost", "127.0.0.1")):
    host = "host.docker.internal"
if not sslmode:
    sslmode = os.environ.get("DATALAKE_PG_SSLMODE", os.environ.get("DATASYNC_PG_SSLMODE", ""))
for val in (host, port, database, user, password, sslmode):
    print(val)
PY
}

apply_pg_sslmode() {
  local sslmode="${1:-}"
  if [[ -n "$sslmode" ]]; then
    export PGSSLMODE="$sslmode"
  else
    unset PGSSLMODE 2>/dev/null || true
  fi
}

apply_lake_schema() {
  "$BIN" lake-schema-only
}

kafka_tcp_ok() {
  [[ -n "${KAFKA_BOOTSTRAP:-}" ]] || return 0
  local khost="${KAFKA_BOOTSTRAP%%:*}"
  local kport="${KAFKA_BOOTSTRAP##*:}"
  (echo >/dev/tcp/"$khost"/"$kport") 2>/dev/null
}

if [[ -f "$CONFIG" ]]; then
  if mapfile -t pg < <(read_pg_cfg); then
    export PGPASSWORD="${pg[4]}"
    apply_pg_sslmode "${pg[5]:-}"
    for _ in $(seq 1 30); do
      if (echo >/dev/tcp/"${pg[0]}"/"${pg[1]}") 2>/dev/null; then
        break
      fi
      sleep 2
    done
  fi
fi

if [[ -n "${KAFKA_BOOTSTRAP:-}" ]]; then
  for _ in $(seq 1 30); do
    if kafka_tcp_ok; then
      break
    fi
    sleep 2
  done
  if ! kafka_tcp_ok; then
    fail "Kafka unreachable at ${KAFKA_BOOTSTRAP} — start stack first (./install.sh start)"
  fi
fi

if mapfile -t dl < <(read_datalake_cfg 2>/dev/null); then
  export PGPASSWORD="${dl[4]}"
  apply_pg_sslmode "${dl[5]:-}"
  apply_lake_schema
fi

cd "$ROOT"
exec "$BIN" "$@"
