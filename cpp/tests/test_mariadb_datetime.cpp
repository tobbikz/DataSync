#include <catch2/catch_test_macros.hpp>

#include "mariadb_datetime.hpp"

TEST_CASE("is_invalid_sql_date", "[mariadb_datetime]") {
    SECTION("empty or too short") {
        CHECK(is_invalid_sql_date(""));
        CHECK(is_invalid_sql_date("2020-12"));
    }

    SECTION("invalid separators or zero parts") {
        CHECK(is_invalid_sql_date("0000-01-01"));
        CHECK(is_invalid_sql_date("2020-00-15"));
        CHECK(is_invalid_sql_date("2020-12-00"));
        CHECK(is_invalid_sql_date("2020/12/15"));
    }

    SECTION("valid dates") {
        CHECK_FALSE(is_invalid_sql_date("2020-12-15"));
        CHECK_FALSE(is_invalid_sql_date("1970-01-01"));
    }
}

TEST_CASE("fix_date_separators", "[mariadb_datetime]") {
    CHECK(fix_date_separators("2020:12:15") == "2020-12-15");
    CHECK(fix_date_separators("2020:12:15 10:30:00") == "2020-12-15 10:30:00");
    CHECK(fix_date_separators("2020-12-15") == "2020-12-15");
    CHECK(fix_date_separators("short") == "short");
}

TEST_CASE("normalize_text_for_pg DATE", "[mariadb_datetime]") {
    CHECK(normalize_text_for_pg("", "DATE").empty());
    CHECK(normalize_text_for_pg("0000-00-00", "DATE").empty());
    CHECK(normalize_text_for_pg("2020:12:15", "DATE") == "2020-12-15");
    CHECK(normalize_text_for_pg("2020-12-15", "DATE") == "2020-12-15");
}

TEST_CASE("normalize_text_for_pg TIMESTAMPTZ", "[mariadb_datetime]") {
    CHECK(normalize_text_for_pg("", "TIMESTAMPTZ").empty());
    CHECK(normalize_text_for_pg("0000-00-00 00:00:00", "TIMESTAMPTZ").empty());
    CHECK(normalize_text_for_pg("2020-12-15 10:30:00", "TIMESTAMPTZ") == "2020-12-15 10:30:00+00");
    CHECK(normalize_text_for_pg("2020-12-15 10:30:00+05", "TIMESTAMPTZ") == "2020-12-15 10:30:00+05");
}

TEST_CASE("normalize_text_for_pg TIMESTAMP", "[mariadb_datetime]") {
    CHECK(normalize_text_for_pg("", "TIMESTAMP").empty());
    CHECK(normalize_text_for_pg("2020-12-15 10:30:00+00", "TIMESTAMP") == "2020-12-15 10:30:00");
    CHECK(normalize_text_for_pg("2020-12-15 10:30:00", "TIMESTAMP") == "2020-12-15 10:30:00");
}

TEST_CASE("mssql_datetime_to_iso", "[mariadb_datetime]") {
    CHECK(mssql_datetime_to_iso("Jan 15 2020 3:45:30 PM") == "2020-01-15 15:45:30+00");
    CHECK(mssql_datetime_to_iso("Jan 15 2020 12:00:00 AM") == "2020-01-15 00:00:00+00");
    CHECK(mssql_datetime_to_iso("not a date") == "not a date");
    CHECK(mssql_datetime_to_iso("").empty());
}

TEST_CASE("epoch_to_timestamptz", "[mariadb_datetime]") {
    CHECK(epoch_to_timestamptz(0) == "1970-01-01 00:00:00+00");
    CHECK(epoch_to_timestamptz(86400) == "1970-01-02 00:00:00+00");
}

TEST_CASE("epoch_ms_to_timestamptz", "[mariadb_datetime]") {
    CHECK(epoch_ms_to_timestamptz(0) == "1970-01-01 00:00:00.000+00");
    CHECK(epoch_ms_to_timestamptz(1500) == "1970-01-01 00:00:01.500+00");
}

TEST_CASE("pg_escape_literal", "[mariadb_datetime]") {
    CHECK(pg_escape_literal("hello") == "'hello'");
    CHECK(pg_escape_literal("it's") == "'it''s'");
    CHECK(pg_escape_literal("") == "''");
}

TEST_CASE("normalize_pg_sql_literal", "[mariadb_datetime]") {
    CHECK(normalize_pg_sql_literal("NULL", "DATE") == "NULL");
    CHECK(normalize_pg_sql_literal("", "DATE") == "NULL");
    CHECK(normalize_pg_sql_literal("'0000-00-00'", "DATE") == "NULL");
    CHECK(normalize_pg_sql_literal("'2020-12-15'", "DATE") == "'2020-12-15'");
    CHECK(normalize_pg_sql_literal("'2020-12-15 10:30:00'", "TIMESTAMPTZ") == "'2020-12-15 10:30:00+00'");
    CHECK(normalize_pg_sql_literal("42", "INTEGER") == "42");
}
