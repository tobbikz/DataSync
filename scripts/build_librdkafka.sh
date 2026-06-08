#!/usr/bin/env bash
# Build librdkafka into cpp/deps/rdkafka (no root required)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX="$ROOT/cpp/deps/rdkafka"
BUILD="$ROOT/cpp/build-rdkafka/src"
VER="${RDKAFKA_VERSION:-v2.6.1}"

if [[ -f "$PREFIX/lib/librdkafka.so" || -f "$PREFIX/lib/librdkafka.a" ]]; then
  echo "librdkafka already built at $PREFIX"
  exit 0
fi

rm -rf "$ROOT/cpp/build-rdkafka"
mkdir -p "$ROOT/cpp/build-rdkafka"
git clone --depth 1 --branch "$VER" https://github.com/confluentinc/librdkafka.git "$BUILD"
cd "$BUILD"
./configure --prefix="$PREFIX" --disable-sasl --disable-ssl 2>&1 | tail -5
make -j"$(nproc)" 2>&1 | tail -5
make install 2>&1 | tail -3
echo "Installed librdkafka to $PREFIX"
