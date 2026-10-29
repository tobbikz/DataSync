#!/usr/bin/env bash
# Bring up local lab DBs + Kafka, bootstrap, run unit + smoke suite.
# Windows: Git Bash. Requires Docker Desktop engine up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'
export DATASYNC_ROOT="$ROOT"
export DATASYNC_CONFIG="${DATASYNC_CONFIG:-$ROOT/config.json}"
export DATASYNC_IMAGE="${DATASYNC_IMAGE:-datasync:local}"
export MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"
export PATH="$ROOT/tools/lab-bin:$PATH"

BASH_BIN="${BASH_BIN:-}"
log() { printf '[lab-tests] %s\n' "$*"; }
fail() { log "FAIL: $*"; exit 1; }

[[ -f "$DATASYNC_CONFIG" ]] || fail "missing $DATASYNC_CONFIG"

RESULTS=()
record() {
  local name="$1" rc="$2"
  if [[ "$rc" -eq 0 ]]; then
    RESULTS+=("PASS  $name")
    log "PASS  $name"
  else
    RESULTS+=("FAIL  $name (rc=$rc)")
    log "FAIL  $name (rc=$rc)"
  fi
}

log "1/6 unit tests image (datasync-tests:local)"
if docker build --target test -t datasync-tests:local --build-arg PARALLEL_JOBS=2 .; then
  record "unit-ctest" 0
else
  record "unit-ctest" 1
fi

log "2/6 build runtime image datasync:local (PARALLEL_JOBS=2)"
if docker build -t datasync:local --build-arg PARALLEL_JOBS=2 .; then
  record "image-datasync:local" 0
else
  record "image-datasync:local" 1
  fail "cannot continue without datasync:local"
fi

log "3/6 start Kafka"
docker compose up -d kafka
for i in $(seq 1 60); do
  if docker compose ps kafka 2>/dev/null | grep -qi healthy; then
    break
  fi
  # cp-kafka healthcheck may lag; broker API is enough
  if docker run --rm --network host confluentinc/cp-kafka:7.6.1 \
      kafka-broker-api-versions --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
record "kafka-up" 0

log "4/6 start + bootstrap OLTP / PG lab"
"$ROOT/tools/start-dev-engines.sh"
"$ROOT/tools/bootstrap-dev-engines.sh"
record "bootstrap-dev" 0

log "5/6 smoke suite"
SMOKES=(
  smoke-schema-embedded.sh
  smoke-test-connection.sh
  smoke-mariadb-iud.sh
  smoke-apply-dlq.sh
  smoke-scd2.sh
  smoke-schema-drift.sh
  smoke-full-load-resume.sh
  smoke-gap-playbook.sh
  smoke-mssql-catchup.sh
  smoke-type-coercion-all-engines.sh
)
for s in "${SMOKES[@]}"; do
  script="$ROOT/tools/$s"
  [[ -x "$script" || -f "$script" ]] || { record "$s" 1; continue; }
  log "---- $s ----"
  set +e
  bash "$script"
  rc=$?
  set -e
  record "$s" "$rc"
done

log "6/6 summary"
printf '\n=== LAB TEST RESULTS ===\n'
fail_count=0
for line in "${RESULTS[@]}"; do
  printf '%s\n' "$line"
  [[ "$line" == FAIL* ]] && fail_count=$((fail_count + 1))
done
printf '========================\n'
[[ "$fail_count" -eq 0 ]] || exit 1
log "all lab checks passed"
