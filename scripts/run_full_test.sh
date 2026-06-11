#!/usr/bin/env bash
# Full test suite: C++ unit tests + optional CDC integration stress.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUN_INTEGRATION="${RUN_INTEGRATION:-1}"
TXN_DURATION_SEC="${TXN_DURATION_SEC:-120}"
TXN_INTERVAL_SEC="${TXN_INTERVAL_SEC:-1}"

echo "=== Unit tests (C++) ==="
"$ROOT/scripts/run_unit_tests.sh"

if [[ "$RUN_INTEGRATION" == "1" ]]; then
  echo "=== Integration stress (full-load + daemon + MariaDB txn/s) ==="
  TXN_DURATION_SEC="$TXN_DURATION_SEC" TXN_INTERVAL_SEC="$TXN_INTERVAL_SEC" \
    "$ROOT/scripts/run_cdc_integration_stress.sh"
else
  echo "=== Integration skipped (RUN_INTEGRATION=0) ==="
fi

echo "=== Full test complete ==="
