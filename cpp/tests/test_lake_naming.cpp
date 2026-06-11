#include <catch2/catch_test_macros.hpp>
#include <nlohmann/json.hpp>

#include "mongo_lake.hpp"
#include "mssql_conn.hpp"
#include "mssql_lake.hpp"
#include "mssql_schema.hpp"

TEST_CASE("sanitize_pg_identifier_part", "[lake_naming]") {
    SECTION("lowercase and underscore non-ident chars") {
        CHECK(sanitize_pg_identifier_part("Order Details") == "order_details");
        CHECK(sanitize_pg_identifier_part("My-Schema.Name") == "my_schema_name");
    }

    SECTION("strip leading underscores") {
        CHECK(sanitize_pg_identifier_part("___foo") == "foo");
        CHECK(sanitize_pg_identifier_part("---bar") == "bar");
    }

    SECTION("empty becomes x") {
        CHECK(sanitize_pg_identifier_part("") == "x");
        CHECK(sanitize_pg_identifier_part("___") == "x");
    }

    SECTION("leading digit gets t_ prefix") {
        CHECK(sanitize_pg_identifier_part("2024report") == "t_2024report");
        CHECK(sanitize_pg_identifier_part("9lives") == "t_9lives");
    }

    SECTION("max_len truncation") {
        const std::string long_name(80, 'a');
        CHECK(sanitize_pg_identifier_part(long_name, 10) == "aaaaaaaaaa");
        CHECK(sanitize_pg_identifier_part(long_name, 63).size() == 63);
    }
}

TEST_CASE("mssql_pg_schema_name", "[lake_naming]") {
    CHECK(mssql_pg_schema_name("AdventureWorks", "dbo") == "adventureworks_dbo");
    CHECK(mssql_pg_schema_name("Sales DB", "Human Resources") == "sales_db_human_resources");
}

TEST_CASE("mssql_pg_table_name", "[lake_naming]") {
    CHECK(mssql_pg_table_name("Customer") == "customer");
    CHECK(mssql_pg_table_name("Order Details") == "order_details");
    CHECK(mssql_pg_table_name("2024Report") == "t_2024report");
}

TEST_CASE("mongo_pg_schema_name", "[lake_naming]") {
    CHECK(mongo_pg_schema_name("inventory") == "inventory");
    CHECK(mongo_pg_schema_name("My App DB") == "my_app_db");
}

TEST_CASE("mongo_pg_table_name", "[lake_naming]") {
    CHECK(mongo_pg_table_name("users") == "users");
    CHECK(mongo_pg_table_name("order-items") == "order_items");
}

TEST_CASE("mongo_object_id_text", "[lake_naming]") {
    SECTION("null and string") {
        CHECK(mongo_object_id_text(nlohmann::json()) == "");
        CHECK(mongo_object_id_text("507f1f77bcf86cd799439011") == "507f1f77bcf86cd799439011");
    }

    SECTION("extended JSON shapes") {
        CHECK(mongo_object_id_text(nlohmann::json{{"$oid", "abc123"}}) == "abc123");
        CHECK(mongo_object_id_text(nlohmann::json{{"oid", "def456"}}) == "def456");
    }

    SECTION("other JSON values") {
        CHECK(mongo_object_id_text(nlohmann::json(42)) == "42");
        CHECK(mongo_object_id_text(nlohmann::json(std::string("quoted"))) == "quoted");
    }
}

TEST_CASE("mongo_catalog_source_schema", "[lake_naming]") {
    CHECK(mongo_catalog_source_schema("inventory", "") == "inventory");
    CHECK(mongo_catalog_source_schema("inventory", "legacy") == "legacy");
}

TEST_CASE("mssql_to_pg_type", "[lake_naming]") {
    SECTION("character types") {
        CHECK(mssql_to_pg_type("nvarchar", 200, 0, 0) == "VARCHAR(100)");
        CHECK(mssql_to_pg_type("varchar", 50, 0, 0) == "VARCHAR(50)");
        CHECK(mssql_to_pg_type("nvarchar", -1, 0, 0) == "TEXT");
        CHECK(mssql_to_pg_type("nchar", 20, 0, 0) == "VARCHAR(10)");
        CHECK(mssql_to_pg_type("char", 8000, 0, 0) == "TEXT");
    }

    SECTION("numeric types") {
        CHECK(mssql_to_pg_type("int", 0, 0, 0) == "INTEGER");
        CHECK(mssql_to_pg_type("bigint", 0, 0, 0) == "BIGINT");
        CHECK(mssql_to_pg_type("smallint", 0, 0, 0) == "SMALLINT");
        CHECK(mssql_to_pg_type("tinyint", 0, 0, 0) == "SMALLINT");
        CHECK(mssql_to_pg_type("bit", 0, 0, 0) == "BOOLEAN");
        CHECK(mssql_to_pg_type("decimal", 0, 18, 4) == "NUMERIC(18,4)");
        CHECK(mssql_to_pg_type("numeric", 0, 10, 2) == "NUMERIC(10,2)");
        CHECK(mssql_to_pg_type("money", 0, 0, 0) == "NUMERIC(19,4)");
        CHECK(mssql_to_pg_type("float", 0, 0, 0) == "DOUBLE PRECISION");
        CHECK(mssql_to_pg_type("real", 0, 0, 0) == "REAL");
    }

    SECTION("date/time and binary") {
        CHECK(mssql_to_pg_type("datetime", 0, 0, 0) == "TIMESTAMP");
        CHECK(mssql_to_pg_type("datetime2", 0, 0, 0) == "TIMESTAMP");
        CHECK(mssql_to_pg_type("date", 0, 0, 0) == "DATE");
        CHECK(mssql_to_pg_type("time", 0, 0, 0) == "TIME");
        CHECK(mssql_to_pg_type("uniqueidentifier", 0, 0, 0) == "UUID");
        CHECK(mssql_to_pg_type("varbinary", 0, 0, 0) == "BYTEA");
        CHECK(mssql_to_pg_type("binary", 0, 0, 0) == "BYTEA");
        CHECK(mssql_to_pg_type("image", 0, 0, 0) == "BYTEA");
    }

    SECTION("unknown falls back to TEXT") {
        CHECK(mssql_to_pg_type("geography", 0, 0, 0) == "TEXT");
    }
}

TEST_CASE("trim_mssql_text", "[lake_naming]") {
    CHECK(trim_mssql_text("  hello  ") == "hello");
    CHECK(trim_mssql_text("value\x00\x00") == "value");
    CHECK(trim_mssql_text("  ") == "");
    CHECK(trim_mssql_text("") == "");
}

#ifdef HAVE_FREETDS
TEST_CASE("sanitize_mssql_text_for_pg", "[lake_naming]") {
    SECTION("strips NUL bytes") {
        CHECK(sanitize_mssql_text_for_pg(std::string("a\0b\0c", 5)) == "abc");
    }

    SECTION("passes ASCII and valid UTF-8") {
        CHECK(sanitize_mssql_text_for_pg("plain") == "plain");
        const std::string utf8 = "caf\u00e9";
        CHECK(sanitize_mssql_text_for_pg(utf8) == utf8);
    }

    SECTION("uplifts isolated high bytes") {
        const std::string latin1 = std::string("a", 1) + char(0xE9);
        const std::string out = sanitize_mssql_text_for_pg(latin1);
        CHECK(out.size() >= 2);
        CHECK(out.front() == 'a');
    }
}
#endif
