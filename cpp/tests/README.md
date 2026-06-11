# DataSync C++ unit tests

Catch2 v3.5.4 via CMake `FetchContent` (local tarball at `cpp/deps/Catch2-v3.5.4.tar.gz`, auto-downloaded by `scripts/run_unit_tests.sh`). Test sources: `test_*.cpp` (auto-discovered).

## Run

From repo root:

```bash
./scripts/run_unit_tests.sh
```

Or manually:

```bash
cd cpp
# ensure Catch2 tarball (or run ./scripts/run_unit_tests.sh from repo root)
test -f deps/Catch2-v3.5.4.tar.gz || curl -fsSL -o deps/Catch2-v3.5.4.tar.gz \
  https://github.com/catchorg/Catch2/archive/refs/tags/v3.5.4.tar.gz
cmake -B build -DBUILD_TESTING=ON
cmake --build build --target datasync_unit_tests
ctest --test-dir build --output-on-failure
```

## Add tests

1. Add `test_<feature>.cpp` in this directory.
2. If the test needs production code, append the `.cpp` path to `DATASYNC_TEST_LIB_SOURCES` in `CMakeLists.txt` (prefer pure helpers that do not open DB connections).
3. Rebuild and run `ctest`.

## Linking production units

`DATASYNC_TEST_LIB_SOURCES` lists individual `src/*.cpp` files compiled into the test binary. Start with stateless helpers (e.g. `kafka_topics.cpp`, `cdc_envelope.cpp`) before pulling in connection-heavy modules.
