#!/usr/bin/env bash
# Enable MSSQL C++ full-load (FreeTDS) and rebuild datalake-catalog.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! pacman -Qi freetds &>/dev/null; then
  echo "Installing freetds..."
  sudo pacman -S --needed --noconfirm freetds
fi

cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build -j"$(nproc)"

if cpp/build/datalake-catalog full-load --help >/dev/null 2>&1; then
  echo "OK: datalake-catalog rebuilt with MSSQL full-load support (HAVE_FREETDS)."
else
  echo "WARN: binary built; verify FreeTDS with: strings cpp/build/datalake-catalog | grep -i freetds || true"
fi
