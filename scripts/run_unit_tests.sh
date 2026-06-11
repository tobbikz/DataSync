#!/usr/bin/env bash
# Build and run C++ unit tests (Catch2) from repo root.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/cpp"

CATCH2_TARBALL="$ROOT/cpp/deps/Catch2-v3.5.4.tar.gz"
if [[ ! -f "$CATCH2_TARBALL" ]]; then
  mkdir -p "$ROOT/cpp/deps"
  curl -fsSL -o "$CATCH2_TARBALL" \
    https://github.com/catchorg/Catch2/archive/refs/tags/v3.5.4.tar.gz
fi

BUILD_DIR="${BUILD_DIR:-build/unit-test}"
cmake -B "$BUILD_DIR" -DBUILD_TESTING=ON -Wno-dev
cmake --build "$BUILD_DIR" --target datasync_unit_tests -j"${NPROC:-$(nproc)}"
ctest --test-dir "$BUILD_DIR" --output-on-failure
