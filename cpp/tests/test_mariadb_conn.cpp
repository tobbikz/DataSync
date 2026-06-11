#include <catch2/catch_test_macros.hpp>

#include "mariadb_conn.hpp"

TEST_CASE("mariadb_error_is_transient", "[mariadb_conn]") {
    CHECK(mariadb_error_is_transient("Lost connection to MySQL server"));
    CHECK(mariadb_error_is_transient("Server has gone away"));
    CHECK(mariadb_error_is_transient("Can't connect to MySQL server"));
    CHECK(mariadb_error_is_transient("Connection refused"));
    CHECK_FALSE(mariadb_error_is_transient(""));
    CHECK_FALSE(mariadb_error_is_transient("duplicate key value"));
}

TEST_CASE("mariadb_errno_is_transient", "[mariadb_conn]") {
    CHECK(mariadb_errno_is_transient(2002));
    CHECK(mariadb_errno_is_transient(2006));
    CHECK(mariadb_errno_is_transient(2013));
    CHECK_FALSE(mariadb_errno_is_transient(1062));
}
