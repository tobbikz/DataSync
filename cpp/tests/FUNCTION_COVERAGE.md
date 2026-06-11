# C++ function test coverage map

Policy: every **public** function in `cpp/include/*.hpp` should have at least one unit test.
Integration (daemon + `scripts/mariadb_txn_simulator.py`) covers end-to-end CDC/full-load.

Run unit tests: `./scripts/run_unit_tests.sh`  
Run integration stress: `./scripts/run_cdc_integration_stress.sh`

| Header / module | Test file | Status |
|-----------------|-----------|--------|
| `mariadb_datetime.hpp` | `test_mariadb_datetime.cpp` | covered |
| `mariadb_schema.hpp` | `test_mariadb_schema.cpp` | covered |
| `mariadb_copy_format.hpp` | `test_mariadb_copy_format.cpp` | covered |
| `mariadb_conn.hpp` | `test_mariadb_conn.cpp` | partial (transient helpers) |
| `mariadb_binlog.hpp` | `test_mariadb_binlog.cpp` | partial (pure cursor math) |
| `kafka_topics.hpp` | `test_kafka_topics.cpp` | covered |
| `cdc_envelope.hpp` | `test_cdc_envelope.cpp` | covered |
| `kafka_apply_detail.hpp` | `test_kafka_apply_parse.cpp` | partial (parse helpers) |
| `capture_common.hpp` | `test_capture_common.cpp` | partial (topic prefix) |
| `mssql_lake.hpp`, `mongo_lake.hpp`, `mssql_schema.hpp`, `mssql_conn.hpp` | `test_lake_naming.cpp` | covered |
| `obs_log.hpp` | `test_obs_log.cpp` | partial (`make_batch_id`, `make_log`) |
| `runtime_config.hpp` | — | TODO (needs PG mock) |
| `pipeline_health.hpp` | — | TODO (needs PG mock) |
| DB/network entrypoints (`*_capture`, `full_load`, `daemon`) | `run_cdc_integration_stress.sh` | integration |

Add a row when a new public API lands; add `test_<module>.cpp` before merging behavior changes.
