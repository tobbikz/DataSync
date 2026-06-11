#include <catch2/catch_test_macros.hpp>

#include "mariadb_copy_format.hpp"

#include <cstring>

namespace {

MariaDbColumn col(const std::string& pg_type, bool nullable = true, const std::string& mysql_type = "text") {
    MariaDbColumn c;
    c.pg_type = pg_type;
    c.mysql_type = mysql_type;
    c.is_nullable = nullable;
    return c;
}

}  // namespace

TEST_CASE("mariadb_format_copy_cell NULL nullable TIMESTAMPTZ", "[mariadb_copy_format]") {
    const auto c = col("TIMESTAMPTZ", true, "datetime");
    const std::string out = mariadb_format_copy_cell(nullptr, 0, c);
    CHECK(out.empty());
    CHECK(out != "\\N");
}

TEST_CASE("mariadb_format_copy_cell NULL nullable TEXT", "[mariadb_copy_format]") {
    const auto c = col("TEXT", true, "varchar(255)");
    CHECK(mariadb_format_copy_cell(nullptr, 0, c).empty());
}

TEST_CASE("mariadb_format_copy_cell NOT NULL default", "[mariadb_copy_format]") {
    const auto c = col("TIMESTAMPTZ", false, "datetime");
    CHECK(mariadb_format_copy_cell(nullptr, 0, c) == "1970-01-01 00:00:00+00");

    auto bigint_col = col("BIGINT", false, "bigint");
    CHECK(mariadb_format_copy_cell(nullptr, 0, bigint_col) == "0");
}

TEST_CASE("mariadb_format_copy_cell invalid datetime normalized to null", "[mariadb_copy_format]") {
    const auto c = col("TIMESTAMPTZ", true, "datetime");
    const char* bad = "0000-00-00 00:00:00";
    CHECK(mariadb_format_copy_cell(bad, std::strlen(bad), c).empty());
}

TEST_CASE("mariadb_format_copy_cell BYTEA hex format", "[mariadb_copy_format]") {
    const unsigned char bytes[] = {0xDE, 0xAD, 0xBE, 0xEF};
    auto bytea_col = col("BYTEA", true, "blob");
    CHECK(mariadb_format_copy_cell(reinterpret_cast<const char*>(bytes), 4, bytea_col) == "\"\\xdeadbeef\"");

    auto binary_col = col("TEXT", true, "varbinary(16)");
    CHECK(mariadb_format_copy_cell(reinterpret_cast<const char*>(bytes), 4, binary_col) == "\"\\xdeadbeef\"");
}

TEST_CASE("mariadb_format_copy_cell valid timestamptz escaped", "[mariadb_copy_format]") {
    const auto c = col("TIMESTAMPTZ", true, "datetime");
    const char* ts = "2020-12-15 10:30:00";
    CHECK(mariadb_format_copy_cell(ts, std::strlen(ts), c) == "2020-12-15 10:30:00+00");
}
