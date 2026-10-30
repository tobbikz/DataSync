cmake_minimum_required(VERSION 3.16)
project(DataSync LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(nlohmann_json QUIET)

pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(LIBPQ REQUIRED libpq)

set(DATASYNC_SOURCES
    src/main.cpp
    src/config.cpp
    src/connections.cpp
    src/connection_test.cpp
    src/catalog_sync.cpp
    src/pg_conn.cpp
    src/mariadb_conn.cpp
    src/mariadb_preflight.cpp
    src/mariadb_schema.cpp
    src/mariadb_datetime.cpp
    src/mariadb_copy_format.cpp
    src/mariadb_boolean.cpp
    src/mariadb_ddl_sync.cpp
    src/mariadb_ddl_sync_run.cpp
    src/schema_drift.cpp
    src/apply_dlq.cpp
    src/lake_history.cpp
    src/full_load_common.cpp
    src/full_load_checkpoint.cpp
    src/full_load_slice.cpp
    src/mariadb_full_load.cpp
    src/mariadb_binlog.cpp
    src/mariadb_binlog_cli.cpp
    src/kafka_apply.cpp
    src/kafka_apply_parse.cpp
    src/lake_apply_index.cpp
    src/kafka_topics.cpp
    src/kafka_apply_native.cpp
    src/kafka_lag.cpp
    src/kafka_table_lag.cpp
    src/kafka_producer.cpp
    src/cdc_envelope.cpp
    src/cdc_tx.cpp
    src/capture_common.cpp
    src/mariadb_kafka_capture.cpp
    src/mssql_kafka_capture.cpp
    src/mongo_kafka_capture.cpp
    src/cdc_pre_apply.cpp
    src/cdc_daemon.cpp
    src/daemon_full_load.cpp
    src/mssql_lake.cpp
    src/mongo_lake.cpp
    src/mssql_full_load.cpp
    src/mongo_full_load.cpp
    src/obs_log.cpp
    src/retention_maintenance.cpp
    src/schema_migrate.cpp
    src/cdc_gap.cpp
    src/type_coercion.cpp
    src/snapshot_signal.cpp
    src/column_policy.cpp
    src/mariadb_signal.cpp
    src/control_plane.cpp
    src/mariadb_binlog_native.cpp
    src/mariadb_binlog_read.cpp
    src/mariadb_reselect.cpp
    src/cdc_routing.cpp
)

# S09: optional Connector/C replication API (mariadb_rpl.h).
find_path(MARIADB_RPL_INCLUDE_DIR NAMES mariadb_rpl.h
    PATH_SUFFIXES mysql mariadb)
if(MARIADB_RPL_INCLUDE_DIR)
    message(STATUS "mariadb_rpl.h found: ${MARIADB_RPL_INCLUDE_DIR}")
    set(HAVE_MARIADB_RPL TRUE)
else()
    message(STATUS "mariadb_rpl.h not found — native binlog path disabled (CLI fallback)")
endif()

find_path(FREETDS_INCLUDE_DIR NAMES sybdb.h)
find_library(FREETDS_LIBRARY NAMES sybdb)
if(FREETDS_INCLUDE_DIR AND FREETDS_LIBRARY)
    message(STATUS "FreeTDS found: ${FREETDS_LIBRARY}")
    list(APPEND DATASYNC_SOURCES src/mssql_conn.cpp src/mssql_schema.cpp src/mssql_ddl_sync.cpp src/mssql_preflight.cpp)
    set(HAVE_FREETDS TRUE)
else()
    message(WARNING "FreeTDS not found — MSSQL C++ full-load disabled. Install: pacman -S freetds")
endif()

pkg_check_modules(MONGOC libmongoc-1.0)
if(MONGOC_FOUND)
    message(STATUS "libmongoc found: ${MONGOC_LIBRARIES}")
    list(APPEND DATASYNC_SOURCES src/mongo_conn.cpp src/mongo_preflight.cpp)
    set(HAVE_MONGOC TRUE)
else()
    message(WARNING "libmongoc not found — MongoDB C++ full-load disabled. Install: pacman -S libmongoc")
endif()

add_executable(DataSync ${DATASYNC_SOURCES})

target_include_directories(DataSync PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${MARIADB_INCLUDE_DIRS}
    ${LIBPQ_INCLUDE_DIRS}
)

target_link_libraries(DataSync PRIVATE
    ${MARIADB_LIBRARIES}
    ${LIBPQ_LIBRARIES}
    crypto
)

if(nlohmann_json_FOUND)
    target_link_libraries(DataSync PRIVATE nlohmann_json::nlohmann_json)
endif()

# librdkafka: Docker stages vendor to ${CMAKE_BINARY_DIR}/deps/rdkafka; native dev uses pkg-config.
set(RDKAFKA_PREFIX "${CMAKE_BINARY_DIR}/deps/rdkafka")
set(HAVE_RDKAFKA OFF)
if(EXISTS "${RDKAFKA_PREFIX}/lib/librdkafka.so" OR EXISTS "${RDKAFKA_PREFIX}/lib/librdkafka.a")
    message(STATUS "Using vendored librdkafka: ${RDKAFKA_PREFIX}")
    set(HAVE_RDKAFKA ON)
    target_include_directories(DataSync PRIVATE "${RDKAFKA_PREFIX}/include")
    if(EXISTS "${RDKAFKA_PREFIX}/lib/librdkafka.so")
        target_link_libraries(DataSync PRIVATE "${RDKAFKA_PREFIX}/lib/librdkafka.so")
    else()
        target_link_libraries(DataSync PRIVATE "${RDKAFKA_PREFIX}/lib/librdkafka.a")
    endif()
else()
    pkg_check_modules(RDKAFKA QUIET rdkafka)
    if(RDKAFKA_FOUND)
        message(STATUS "Using system librdkafka via pkg-config")
        set(HAVE_RDKAFKA ON)
        target_include_directories(DataSync PRIVATE ${RDKAFKA_INCLUDE_DIRS})
        target_link_libraries(DataSync PRIVATE ${RDKAFKA_LIBRARIES})
    else()
        message(WARNING "librdkafka not found — Kafka capture/apply disabled. Use Docker build.")
    endif()
endif()

if(HAVE_RDKAFKA)
    target_compile_definitions(DataSync PRIVATE HAVE_RDKAFKA)
endif()

if(HAVE_MARIADB_RPL)
    target_compile_definitions(DataSync PRIVATE HAVE_MARIADB_RPL)
    target_include_directories(DataSync PRIVATE "${MARIADB_RPL_INCLUDE_DIR}")
endif()

if(HAVE_FREETDS)
    target_compile_definitions(DataSync PRIVATE HAVE_FREETDS)
    target_include_directories(DataSync PRIVATE "${FREETDS_INCLUDE_DIR}")
    target_link_libraries(DataSync PRIVATE "${FREETDS_LIBRARY}")
endif()

if(HAVE_MONGOC)
    target_compile_definitions(DataSync PRIVATE HAVE_MONGOC)
    target_include_directories(DataSync PRIVATE ${MONGOC_INCLUDE_DIRS})
    target_link_libraries(DataSync PRIVATE ${MONGOC_LIBRARIES})
endif()

target_compile_options(DataSync PRIVATE -Wall -Wextra)

option(BUILD_TESTING "Build DataSync unit tests" ON)
if(BUILD_TESTING)
    enable_testing()
    # Headers under include/ often pull libpq-fe.h; keep includes consistent across unit tests.
    set(DATASYNC_TEST_INCLUDES
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${LIBPQ_INCLUDE_DIRS}
        ${MARIADB_INCLUDE_DIRS}
    )
    add_executable(datasync_boolean_tests tests/boolean_tests.cpp src/mariadb_boolean.cpp)
    target_include_directories(datasync_boolean_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_compile_options(datasync_boolean_tests PRIVATE -Wall -Wextra)
    add_test(NAME boolean_tests COMMAND datasync_boolean_tests)

    add_executable(datasync_envelope_tests
        tests/envelope_tests.cpp
        src/cdc_envelope.cpp
        src/mariadb_schema.cpp
        src/mariadb_boolean.cpp
        src/mariadb_datetime.cpp
        src/pg_conn.cpp
    )
    target_include_directories(datasync_envelope_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_link_libraries(datasync_envelope_tests PRIVATE ${MARIADB_LIBRARIES} ${LIBPQ_LIBRARIES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_envelope_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_envelope_tests PRIVATE -Wall -Wextra)
    add_test(NAME envelope_tests COMMAND datasync_envelope_tests)

    add_executable(datasync_tx_id_tests tests/tx_id_tests.cpp src/cdc_tx.cpp)
    target_include_directories(datasync_tx_id_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_compile_options(datasync_tx_id_tests PRIVATE -Wall -Wextra)
    add_test(NAME tx_id_tests COMMAND datasync_tx_id_tests)

    add_executable(datasync_gap_kind_tests tests/gap_kind_tests.cpp)
    target_include_directories(datasync_gap_kind_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_compile_options(datasync_gap_kind_tests PRIVATE -Wall -Wextra)
    add_test(NAME gap_kind_tests COMMAND datasync_gap_kind_tests)

    add_executable(datasync_coercion_sensitive_tests tests/coercion_sensitive_tests.cpp)
    target_include_directories(datasync_coercion_sensitive_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_coercion_sensitive_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_coercion_sensitive_tests PRIVATE -Wall -Wextra)
    add_test(NAME coercion_sensitive_tests COMMAND datasync_coercion_sensitive_tests)

    add_executable(datasync_full_load_slice_tests
        tests/full_load_slice_tests.cpp
        src/full_load_slice.cpp
    )
    target_include_directories(datasync_full_load_slice_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_full_load_slice_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_full_load_slice_tests PRIVATE -Wall -Wextra)
    add_test(NAME full_load_slice_tests COMMAND datasync_full_load_slice_tests)

    add_executable(datasync_quarantine_hold_tests tests/quarantine_hold_tests.cpp)
    target_include_directories(datasync_quarantine_hold_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_quarantine_hold_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_quarantine_hold_tests PRIVATE -Wall -Wextra)
    add_test(NAME quarantine_hold_tests COMMAND datasync_quarantine_hold_tests)

    add_executable(datasync_column_policy_tests
        tests/column_policy_tests.cpp
        src/column_policy.cpp
    )
    target_include_directories(datasync_column_policy_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_link_libraries(datasync_column_policy_tests PRIVATE crypto)
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_column_policy_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_column_policy_tests PRIVATE -Wall -Wextra)
    add_test(NAME column_policy_tests COMMAND datasync_column_policy_tests)

    add_executable(datasync_snapshot_signal_tests tests/snapshot_signal_tests.cpp)
    target_include_directories(datasync_snapshot_signal_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_snapshot_signal_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_snapshot_signal_tests PRIVATE -Wall -Wextra)
    add_test(NAME snapshot_signal_tests COMMAND datasync_snapshot_signal_tests)

    add_executable(datasync_control_plane_tests
        tests/control_plane_tests.cpp
        tests/shutdown_stub.cpp
        src/control_plane.cpp
        src/config.cpp
        src/pg_conn.cpp
        src/snapshot_signal.cpp
    )
    target_include_directories(datasync_control_plane_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    target_link_libraries(datasync_control_plane_tests PRIVATE ${LIBPQ_LIBRARIES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_control_plane_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_control_plane_tests PRIVATE -Wall -Wextra)
    add_test(NAME control_plane_tests COMMAND datasync_control_plane_tests)

    # Lightweight: no libpq — verifies CdcConfig defaults used by P2 wiring.
    add_executable(datasync_cdc_config_tests tests/cdc_config_tests.cpp src/config.cpp)
    target_include_directories(datasync_cdc_config_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_cdc_config_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_cdc_config_tests PRIVATE -Wall -Wextra)
    add_test(NAME cdc_config_tests COMMAND datasync_cdc_config_tests)

    add_executable(datasync_cdc_routing_tests
        tests/cdc_routing_tests.cpp
        src/cdc_routing.cpp
    )
    target_include_directories(datasync_cdc_routing_tests PRIVATE ${DATASYNC_TEST_INCLUDES})
    if(nlohmann_json_FOUND)
        target_link_libraries(datasync_cdc_routing_tests PRIVATE nlohmann_json::nlohmann_json)
    endif()
    target_compile_options(datasync_cdc_routing_tests PRIVATE -Wall -Wextra)
    add_test(NAME cdc_routing_tests COMMAND datasync_cdc_routing_tests)
endif()
