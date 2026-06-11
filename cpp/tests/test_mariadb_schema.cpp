#include <catch2/catch_test_macros.hpp>

#include "mariadb_schema.hpp"

TEST_CASE("sanitize_utf8_for_json", "[mariadb_schema]") {
    const std::string valid = "caf\u00e9";
    CHECK(sanitize_utf8_for_json(valid) == valid);

    const std::string invalid = std::string("bad") + static_cast<char>(0x98) + "byte";
    CHECK(sanitize_utf8_for_json(invalid) == "bad?byte");

    const std::string truncated = std::string("\xC2");
    CHECK(sanitize_utf8_for_json(truncated) == "?");
}

TEST_CASE("sanitize_mariadb_text_for_pg", "[mariadb_schema]") {
    CHECK(sanitize_mariadb_text_for_pg("hello") == "hello");

    const std::string with_nul = std::string("ab") + '\0' + "cd";
    CHECK(sanitize_mariadb_text_for_pg(with_nul) == "abcd");

    const std::string nul_and_bad = std::string("x") + '\0' + static_cast<char>(0x98);
    CHECK(sanitize_mariadb_text_for_pg(nul_and_bad) == "x?");
}

TEST_CASE("mariadb_to_pg_type", "[mariadb_schema]") {
    CHECK(mariadb_to_pg_type("varchar(255)") == "TEXT");
    CHECK(mariadb_to_pg_type("char(10)") == "TEXT");
    CHECK(mariadb_to_pg_type("text") == "TEXT");
    CHECK(mariadb_to_pg_type("blob") == "BYTEA");
    CHECK(mariadb_to_pg_type("varbinary(64)") == "BYTEA");
    CHECK(mariadb_to_pg_type("bigint") == "BIGINT");
    CHECK(mariadb_to_pg_type("int") == "NUMERIC");
    CHECK(mariadb_to_pg_type("tinyint") == "SMALLINT");
    CHECK(mariadb_to_pg_type("mediumint") == "INTEGER");
    CHECK(mariadb_to_pg_type("datetime") == "TIMESTAMPTZ");
    CHECK(mariadb_to_pg_type("timestamp") == "TIMESTAMPTZ");
    CHECK(mariadb_to_pg_type("date") == "DATE");
    CHECK(mariadb_to_pg_type("time") == "TIME");
    CHECK(mariadb_to_pg_type("json") == "JSONB");
    CHECK(mariadb_to_pg_type("double") == "DOUBLE PRECISION");
    CHECK(mariadb_to_pg_type("float") == "REAL");
    CHECK(mariadb_to_pg_type("decimal(10,2)") == "DECIMAL");
}

TEST_CASE("mariadb_lake_pg_type", "[mariadb_schema]") {
    CHECK(mariadb_lake_pg_type("col", "varchar(100)") == "TEXT");
    CHECK(mariadb_lake_pg_type("data", "longblob") == "BYTEA");
    CHECK(mariadb_lake_pg_type("id", "bigint unsigned") == "BIGINT");
}

TEST_CASE("mariadb_not_null_copy_default", "[mariadb_schema]") {
    MariaDbColumn col;
    col.pg_type = "DATE";
    CHECK(mariadb_not_null_copy_default(col) == "1970-01-01");

    col.pg_type = "TIMESTAMPTZ";
    CHECK(mariadb_not_null_copy_default(col) == "1970-01-01 00:00:00+00");

    col.pg_type = "TIMESTAMP";
    CHECK(mariadb_not_null_copy_default(col) == "1970-01-01 00:00:00");

    col.pg_type = "TIME";
    CHECK(mariadb_not_null_copy_default(col) == "00:00:00");

    col.pg_type = "BOOLEAN";
    CHECK(mariadb_not_null_copy_default(col) == "f");

    col.pg_type = "JSONB";
    CHECK(mariadb_not_null_copy_default(col) == "{}");

    col.pg_type = "BIGINT";
    CHECK(mariadb_not_null_copy_default(col) == "0");

    col.pg_type = "NUMERIC";
    CHECK(mariadb_not_null_copy_default(col) == "0");

    col.pg_type = "TEXT";
    col.is_pk = false;
    const std::string missing = mariadb_not_null_copy_default(col);
    CHECK(missing.rfind("__missing__", 0) == 0);

    col.is_pk = true;
    const std::string pk_uuid = mariadb_not_null_copy_default(col);
    CHECK(pk_uuid.size() == 36);
    CHECK(pk_uuid.substr(0, 8) == "00000000");
}

TEST_CASE("mariadb_bytea_to_copy_csv", "[mariadb_schema]") {
    const unsigned char bytes[] = {0x00, 0xAB, 0xCD};
    CHECK(mariadb_bytea_to_copy_csv(reinterpret_cast<const char*>(bytes), 3) == "\"\\x00abcd\"");

    CHECK(mariadb_bytea_to_copy_csv("AB") == "\"\\xab\"");
    CHECK(mariadb_bytea_to_copy_csv("\\xdeadbeef") == "\"\\xde\"");
    CHECK(mariadb_bytea_to_copy_csv("deadbeef") == "\"\\xdeadbeef\"");
}

TEST_CASE("pg_ident", "[mariadb_schema]") {
    CHECK(pg_ident("col") == "\"col\"");
    CHECK(pg_ident("my_table") == "\"my_table\"");
    CHECK(pg_ident("quote\"inside") == "\"quote\"\"inside\"");
}
