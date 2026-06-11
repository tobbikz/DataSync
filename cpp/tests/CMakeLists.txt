include(FetchContent)

set(CATCH2_TARBALL "${CMAKE_SOURCE_DIR}/deps/Catch2-v3.5.4.tar.gz")
if(NOT EXISTS "${CATCH2_TARBALL}")
    message(FATAL_ERROR "Missing ${CATCH2_TARBALL} — run scripts/run_unit_tests.sh (downloads on first use)")
endif()
FetchContent_Declare(
    Catch2
    URL "file://${CATCH2_TARBALL}"
    URL_HASH SHA256=b7754b711242c167d8f60b890695347f90a1ebc95949a045385114165d606dbb
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(CATCH_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(CATCH_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(CATCH_BUILD_EXTRA_TESTS OFF CACHE BOOL "" FORCE)
set(CATCH_DEVELOPMENT_BUILD OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(Catch2)

find_package(PkgConfig REQUIRED)
pkg_check_modules(MARIADB REQUIRED mariadb)
pkg_check_modules(LIBPQ REQUIRED libpq)

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/test_*.cpp")

# Production .cpp units under test — extend as new test_*.cpp need them.
set(DATASYNC_TEST_LIB_SOURCES
    ${CMAKE_SOURCE_DIR}/src/mariadb_schema.cpp
    ${CMAKE_SOURCE_DIR}/src/mariadb_datetime.cpp
    ${CMAKE_SOURCE_DIR}/src/mariadb_binlog.cpp
    ${CMAKE_SOURCE_DIR}/src/mariadb_conn.cpp
    ${CMAKE_SOURCE_DIR}/src/mariadb_copy_format.cpp
    ${CMAKE_SOURCE_DIR}/src/kafka_apply_parse.cpp
    ${CMAKE_SOURCE_DIR}/src/obs_log.cpp
    ${CMAKE_SOURCE_DIR}/src/kafka_topics.cpp
    ${CMAKE_SOURCE_DIR}/src/cdc_envelope.cpp
    ${CMAKE_SOURCE_DIR}/src/mssql_lake.cpp
    ${CMAKE_SOURCE_DIR}/src/mongo_lake.cpp
    ${CMAKE_SOURCE_DIR}/src/runtime_config.cpp
    ${CMAKE_SOURCE_DIR}/src/mssql_schema.cpp
    ${CMAKE_SOURCE_DIR}/src/mssql_conn.cpp
)

add_executable(datasync_unit_tests ${TEST_SOURCES} ${DATASYNC_TEST_LIB_SOURCES})

target_include_directories(datasync_unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MARIADB_INCLUDE_DIRS}
    ${LIBPQ_INCLUDE_DIRS}
)

target_link_libraries(datasync_unit_tests PRIVATE
    Catch2::Catch2WithMain
    ${MARIADB_LIBRARIES}
    ${LIBPQ_LIBRARIES}
    crypto
)

if(nlohmann_json_FOUND)
    target_link_libraries(datasync_unit_tests PRIVATE nlohmann_json::nlohmann_json)
endif()

find_path(FREETDS_INCLUDE_DIR NAMES sybdb.h)
find_library(FREETDS_LIBRARY NAMES sybdb)
if(FREETDS_INCLUDE_DIR AND FREETDS_LIBRARY)
    target_compile_definitions(datasync_unit_tests PRIVATE HAVE_FREETDS)
    target_include_directories(datasync_unit_tests PRIVATE "${FREETDS_INCLUDE_DIR}")
    target_link_libraries(datasync_unit_tests PRIVATE "${FREETDS_LIBRARY}")
endif()

target_compile_options(datasync_unit_tests PRIVATE -Wall -Wextra)

include(Catch)
catch_discover_tests(datasync_unit_tests)
