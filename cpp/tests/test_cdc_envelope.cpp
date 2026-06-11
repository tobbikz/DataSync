#include "cdc_envelope.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <regex>
#include <string>
#include <vector>

TEST_CASE("op_char_from_mysql maps binlog ops", "[cdc_envelope]") {
    REQUIRE(op_char_from_mysql("INSERT") == "c");
    REQUIRE(op_char_from_mysql("UPDATE") == "u");
    REQUIRE(op_char_from_mysql("DELETE") == "d");
    REQUIRE(op_char_from_mysql("UNKNOWN") == "u");
}

TEST_CASE("parse_sql_literal handles NULL numbers strings dates", "[cdc_envelope]") {
    SECTION("NULL") {
        REQUIRE(parse_sql_literal("NULL").is_null());
    }

    SECTION("integer") {
        REQUIRE(parse_sql_literal("42").get<long long>() == 42);
        REQUIRE(parse_sql_literal("-7").get<long long>() == -7);
    }

    SECTION("float") {
        REQUIRE(parse_sql_literal("3.14").get<double>() == Catch::Approx(3.14));
    }

    SECTION("string") {
        REQUIRE(parse_sql_literal("'hello'") == "hello");
        REQUIRE(parse_sql_literal("'it''s fine'") == "it's fine");
    }

    SECTION("date literal stays ISO date string") {
        REQUIRE(parse_sql_literal("'2024-06-10'") == "2024-06-10");
    }

    SECTION("hex binary") {
        REQUIRE(parse_sql_literal("0xDEAD") == "DEAD");
    }
}

TEST_CASE("row_dict_from_columns builds object from parallel arrays", "[cdc_envelope]") {
    const std::vector<std::string> columns = {"id", "name", "active", "note"};
    const std::vector<std::string> values = {"42", "'alice'", "NULL", "'2024-01-01'"};

    const nlohmann::json row = row_dict_from_columns(columns, values);
    REQUIRE(row.is_object());
    REQUIRE(row["id"].get<long long>() == 42);
    REQUIRE(row["name"] == "alice");
    REQUIRE(row["active"].is_null());
    REQUIRE(row["note"] == "2024-01-01");
}

TEST_CASE("CdcEvent::to_kafka_dict shape", "[cdc_envelope]") {
    CdcEvent event;
    event.op = "u";
    event.conn_id = "MARIADB_LOCAL";
    event.source_database = "tpch";
    event.schema_name = "nation";
    event.table_name = "countries";
    event.gtid = "0-1-100";
    event.binlog_file = "mariadb-bin.000001";
    event.binlog_pos = 12345;
    event.ts_ms = 1718000000000LL;
    event.before = nlohmann::json{{"id", 1}, {"name", "old"}};
    event.after = nlohmann::json{{"id", 1}, {"name", "new"}};
    event.ingestion_ts = "2024-06-10T12:00:00+00:00";

    const nlohmann::json dict = event.to_kafka_dict();

    REQUIRE(dict["op"] == "u");
    REQUIRE(dict["conn_id"] == "MARIADB_LOCAL");
    REQUIRE(dict["db_engine"] == "mariadb");
    REQUIRE(dict["source_database"] == "tpch");
    REQUIRE(dict["source_schema"] == "nation");
    REQUIRE(dict["source_table"] == "countries");
    REQUIRE(dict["before"]["name"] == "old");
    REQUIRE(dict["after"]["name"] == "new");
    REQUIRE(dict["ingestion_ts"] == "2024-06-10T12:00:00+00:00");

    REQUIRE(dict["source"].is_object());
    REQUIRE(dict["source"]["gtid"] == "0-1-100");
    REQUIRE(dict["source"]["file"] == "mariadb-bin.000001");
    REQUIRE(dict["source"]["pos"].get<long long>() == 12345);
    REQUIRE(dict["source"]["ts_ms"].get<long long>() == 1718000000000LL);
}

TEST_CASE("utc_iso_timestamp_now format", "[cdc_envelope]") {
    const std::string ts = utc_iso_timestamp_now();
    static const std::regex re(R"(^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$)");
    REQUIRE(std::regex_match(ts, re));
}
