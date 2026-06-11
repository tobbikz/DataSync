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
    src/service_tiers.cpp
    src/catalog_sync.cpp
    src/mariadb_conn.cpp
    src/mariadb_preflight.cpp
    src/mariadb_schema.cpp
    src/mariadb_datetime.cpp
    src/mariadb_copy_format.cpp
    src/mariadb_ddl_sync.cpp
    src/mariadb_ddl_sync_run.cpp
    src/mariadb_full_load.cpp
    src/mariadb_binlog.cpp
    src/mariadb_binlog_cli.cpp
    src/mariadb_cdc.cpp
    src/kafka_apply.cpp
    src/kafka_apply_parse.cpp
    src/kafka_topics.cpp
    src/kafka_apply_native.cpp
    src/kafka_lag.cpp
    src/host_metrics.cpp
    src/kafka_producer.cpp
    src/cdc_envelope.cpp
    src/capture_common.cpp
    src/mariadb_kafka_capture.cpp
    src/mssql_kafka_capture.cpp
    src/mongo_kafka_capture.cpp
    src/cdc_pre_apply.cpp
    src/cdc_catchup.cpp
    src/cdc_reconcile.cpp
    src/cdc_daemon.cpp
    src/daemon_full_load.cpp
    src/mssql_lake.cpp
    src/mongo_lake.cpp
    src/mssql_full_load.cpp
    src/mongo_full_load.cpp
    src/runtime_config.cpp
    src/obs_log.cpp
    src/pipeline_health.cpp
)

find_path(FREETDS_INCLUDE_DIR NAMES sybdb.h)
find_library(FREETDS_LIBRARY NAMES sybdb)
if(FREETDS_INCLUDE_DIR AND FREETDS_LIBRARY)
    message(STATUS "FreeTDS found: ${FREETDS_LIBRARY}")
    list(APPEND DATASYNC_SOURCES src/mssql_conn.cpp src/mssql_schema.cpp src/mssql_ddl_sync.cpp)
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

# librdkafka: Docker build stages under cpp/deps/rdkafka; native dev may use pkg-config.
set(RDKAFKA_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/deps/rdkafka")
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

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
