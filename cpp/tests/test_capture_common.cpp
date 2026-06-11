#include <catch2/catch_test_macros.hpp>

#include "capture_common.hpp"

TEST_CASE("topic_prefix_for_conn", "[capture_common]") {
    CHECK(topic_prefix_for_conn("MARIADB_LOCAL") == "MARIADB_LOCAL");
    CHECK(topic_prefix_for_conn("") == "UNKNOWN_CONN");
}

TEST_CASE("runtime_topic_prefix ignores runtime and pg", "[capture_common]") {
    RuntimeConfig runtime;
    CHECK(runtime_topic_prefix(runtime, nullptr, "MARIADB_PROD", "mariadb") == "MARIADB_PROD");
    CHECK(runtime_topic_prefix(runtime, nullptr, "", "mssql") == "UNKNOWN_CONN");
}
