#!/usr/bin/env bash
# Run unit ctest image + smoke suite against already-up lab DBs (no engine rebuild).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'
export PATH="$ROOT/tools/lab-bin:$PATH"
export DATASYNC_IMAGE="${DATASYNC_IMAGE:-datasync:local}"
export MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-DataSync_Dev1!}"

# shellcheck source=lab-common.sh
source "$ROOT/tools/lab-common.sh"
# Prefer Windows-native config path for docker.exe + CPython.
export DATASYNC_CONFIG="$(host_path "${DATASYNC_CONFIG:-$ROOT/config.json}")"

log() { printf '[lab-smokes] %s\n' "$*"; }
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

log "config=$DATASYNC_CONFIG image=$DATASYNC_IMAGE"

log "unit ctest (datasync-tests:local)"
set +e
docker build --target test -t datasync-tests:local --build-arg PARALLEL_JOBS=2 .
record "unit-ctest" $?
set -e

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

# Patch python3 → lab_py and CONFIG mount for this process tree via env wrapper.
run_smoke() {
  local script="$1"
  # Inject lab-common for scripts that still call python3 with Unix paths.
  bash -c '
    set -euo pipefail
    ROOT="'"$ROOT"'"
    # shellcheck source=/dev/null
    source "$ROOT/tools/lab-common.sh"
    export DATASYNC_CONFIG="'"$DATASYNC_CONFIG"'"
    export DATASYNC_IMAGE="'"$DATASYNC_IMAGE"'"
    export PATH="'"$PATH"'"
    export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*"
    export MSSQL_SA_PASSWORD="'"$MSSQL_SA_PASSWORD"'"
    # Prefer Windows python; alias python3 for smoke scripts.
    if command -v python >/dev/null 2>&1; then
      python3() { python "$@"; }
      export -f python3
    fi
    bash "'"$script"'"
  '
}

for s in "${SMOKES[@]}"; do
  script="$ROOT/tools/$s"
  [[ -f "$script" ]] || { record "$s" 1; continue; }
  log "---- $s ----"
  set +e
  run_smoke "$script"
  rc=$?
  set -e
  record "$s" "$rc"
done

log "summary"
printf '\n=== SMOKE RESULTS ===\n'
fail_count=0
for line in "${RESULTS[@]}"; do
  printf '%s\n' "$line"
  [[ "$line" == FAIL* ]] && fail_count=$((fail_count + 1))
done
printf '=====================\n'
[[ "$fail_count" -eq 0 ]] || exit 1
log "all smokes passed"
