#!/usr/bin/env bash
# Enable MongoDB C++ full-load (libmongoc) and rebuild datalake-catalog.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! pacman -Qi libmongoc-1.0 &>/dev/null && ! pacman -Qi libmongoc &>/dev/null; then
  echo "Installing libmongoc (dev headers + pkg-config)..."
  sudo pacman -S --needed --noconfirm libmongoc
fi

cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build -j"$(nproc)"

if strings cpp/build/datalake-catalog | grep -q HAVE_MONGOC 2>/dev/null || \
   pkg-config --exists libmongoc-1.0; then
  echo "OK: datalake-catalog rebuilt; verify mongoc with: pkg-config --modversion libmongoc-1.0"
else
  echo "WARN: binary built; if full-load skips Mongo, re-run cmake after libmongoc install."
fi
