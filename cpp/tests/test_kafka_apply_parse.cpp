#include <catch2/catch_test_macros.hpp>

#include "kafka_apply_detail.hpp"

#include <nlohmann/json.hpp>

using json = nlohmann::json;
using kafka_apply_detail::ApplyEvent;
using kafka_apply_detail::parse_apply_event;
using kafka_apply_detail::parse_kafka_message_json;
using kafka_apply_detail::parse_kafka_payload;

namespace {

json mariadb_insert_fixture() {
    return json{
        {"op", "INSERT"},
        {"source_schema", "sales"},
        {"source_table", "orders"},
        {"after", {{"id", 42}, {"status", "new"}}},
        {"source", {{"gtid", "0-1-100"}, {"db", "sales"}, {"table", "orders"}}},
        {"ts_ms", 1700000000000LL},
    };
}

}  // namespace

TEST_CASE("parse_apply_event fields", "[kafka_apply_parse]") {
    const json obj = {
        {"event_id", "evt-1"},
        {"op", "u"},
        {"schema", "lake_sch"},
        {"table", "lake_tbl"},
        {"topic", "MARIADB_LOCAL.cdc.0"},
        {"partition", 2},
        {"offset", 99},
        {"gtid", "0-1-200"},
        {"catalog_id", 7},
        {"row", {{"id", 1}}},
    };
    const ApplyEvent e = parse_apply_event(obj);
    CHECK(e.event_id == "evt-1");
    CHECK(e.op == "u");
    CHECK(e.schema_name == "lake_sch");
    CHECK(e.table_name == "lake_tbl");
    CHECK(e.topic == "MARIADB_LOCAL.cdc.0");
    CHECK(e.partition == 2);
    CHECK(e.offset == 99);
    CHECK(e.gtid == "0-1-200");
    CHECK(e.catalog_id == 7);
    CHECK(e.row["id"] == 1);
}

TEST_CASE("parse_kafka_payload op mapping", "[kafka_apply_parse]") {
    const std::vector<std::string> pk{"id"};
    const std::string topic = "MARIADB_LOCAL.cdc.0";

    auto check_op = [&](const std::string& op_in, const std::string& expected) {
        json data = mariadb_insert_fixture();
        data["op"] = op_in;
        ApplyEvent out;
        REQUIRE(parse_kafka_payload(data, out, topic, 0, 1, pk));
        CHECK(out.op == expected);
    };

    check_op("INSERT", "c");
    check_op("UPDATE", "u");
    check_op("DELETE", "d");
    check_op("c", "c");
}

TEST_CASE("parse_kafka_payload schema and table extraction", "[kafka_apply_parse]") {
    const std::vector<std::string> pk{"id"};
    ApplyEvent out;

    SECTION("top-level source_schema/source_table") {
        json data = mariadb_insert_fixture();
        REQUIRE(parse_kafka_payload(data, out, "t", 0, 0, pk));
        CHECK(out.schema_name == "sales");
        CHECK(out.table_name == "orders");
    }

    SECTION("fallback to source.db and source.table") {
        json data = {
            {"op", "c"},
            {"after", {{"id", 1}}},
            {"source", {{"gtid", "0-1-1"}, {"db", "inventory"}, {"table", "items"}}},
        };
        REQUIRE(parse_kafka_payload(data, out, "t", 0, 0, pk));
        CHECK(out.schema_name == "inventory");
        CHECK(out.table_name == "items");
    }

    SECTION("delete uses before row") {
        json data = {
            {"op", "DELETE"},
            {"before", {{"id", 5}}},
            {"source_schema", "sales"},
            {"source_table", "orders"},
            {"source", {{"gtid", "0-1-2"}}},
        };
        REQUIRE(parse_kafka_payload(data, out, "t", 0, 0, pk));
        CHECK(out.op == "d");
        CHECK(out.row["id"] == 5);
    }
}

TEST_CASE("parse_kafka_payload event_id stability", "[kafka_apply_parse]") {
    const std::vector<std::string> pk{"id"};
    const json data = mariadb_insert_fixture();

    ApplyEvent first;
    ApplyEvent second;
    REQUIRE(parse_kafka_payload(data, first, "topic-a", 1, 10, pk));
    REQUIRE(parse_kafka_payload(data, second, "topic-b", 2, 20, pk));

    CHECK(first.event_id == second.event_id);
    CHECK(first.event_id.find("0-1-100") != std::string::npos);
    CHECK(first.event_id.find("sales.orders") != std::string::npos);
    CHECK(first.event_id.find("|c|") != std::string::npos);
    CHECK(first.event_id.find("id=42") != std::string::npos);

    CHECK(first.topic == "topic-a");
    CHECK(second.topic == "topic-b");
}

TEST_CASE("parse_kafka_message_json skips leading garbage", "[kafka_apply_parse]") {
    const std::string payload = std::string("\0\0\0\0", 4) + R"({"op":"c","after":{"id":1},"source_schema":"s","source_table":"t","source":{"gtid":"g"}})";
    const json data = parse_kafka_message_json(payload);
    REQUIRE_FALSE(data.is_null());
    CHECK(data["op"] == "c");

    ApplyEvent out;
    REQUIRE(parse_kafka_payload(payload.data(), payload.size(), out, "t", 0, 0, {"id"}));
    CHECK(out.schema_name == "s");
    CHECK(out.table_name == "t");
}

TEST_CASE("parse_kafka_payload rejects incomplete payload", "[kafka_apply_parse]") {
    ApplyEvent out;
    const json missing_table = {{"op", "c"}, {"source_schema", "only_schema"}, {"after", {{"id", 1}}}};
    CHECK_FALSE(parse_kafka_payload(missing_table, out, "t", 0, 0, {"id"}));
}
