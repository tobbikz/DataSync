#include "kafka_topics.hpp"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <string>
#include <utility>
#include <vector>

TEST_CASE("topic_bucket is deterministic and in range", "[kafka_topics]") {
    const int num_buckets = 64;
    const int b1 = topic_bucket("nation", "countries", num_buckets);
    const int b2 = topic_bucket("nation", "countries", num_buckets);
    REQUIRE(b1 == b2);
    REQUIRE(b1 >= 0);
    REQUIRE(b1 < num_buckets);

    // Precomputed via SHA256("nation.countries") % 64
    REQUIRE(b1 == 37);

    for (int nb : {1, 2, 16, 64, 128}) {
        const int b = topic_bucket("foo", "bar", nb);
        REQUIRE(b >= 0);
        REQUIRE(b < nb);
    }
}

TEST_CASE("topic_for_catalog per_table vs bucketed mode", "[kafka_topics]") {
    const std::string prefix = "cdc.mariadb";

    SECTION("per_table") {
        REQUIRE(topic_for_catalog(prefix, "nation", "countries", "per_table", 64) ==
                "cdc.mariadb.nation.countries");
    }

    SECTION("bucketed default") {
        REQUIRE(topic_for_catalog(prefix, "nation", "countries", "bucketed", 64) ==
                "cdc.mariadb.b0037");
        REQUIRE(topic_for_catalog(prefix, "nation", "countries", "", 64) == "cdc.mariadb.b0037");
    }

    SECTION("bucket padding") {
        REQUIRE(topic_for_catalog(prefix, "foo", "bar", "bucketed", 64) == "cdc.mariadb.b0010");
    }
}

TEST_CASE("topics_for_tables deduplicates topics", "[kafka_topics]") {
    const std::string prefix = "cdc.mariadb";
    const std::vector<std::pair<std::string, std::string>> tables = {
        {"nation", "countries"},
        {"nation", "countries"},
        {"s336", "t336"},  // same bucket (37) as nation.countries
    };

    const auto topics = topics_for_tables(prefix, tables, "bucketed", 64);
    REQUIRE(topics.size() == 1);
    REQUIRE(topics[0] == "cdc.mariadb.b0037");

    const std::vector<std::pair<std::string, std::string>> per_table = {
        {"nation", "countries"},
        {"nation", "regions"},
    };
    const auto per_table_topics = topics_for_tables(prefix, per_table, "per_table", 64);
    REQUIRE(per_table_topics.size() == 2);
    REQUIRE(std::set<std::string>(per_table_topics.begin(), per_table_topics.end()) ==
            std::set<std::string>{"cdc.mariadb.nation.countries", "cdc.mariadb.nation.regions"});
}

TEST_CASE("kafka_message_key", "[kafka_topics]") {
    REQUIRE(kafka_message_key("nation", "countries") == "nation.countries");
}

TEST_CASE("kafka_message_key_for_row uses first PK column", "[kafka_topics]") {
    const std::string schema = "nation";
    const std::string table = "countries";

    SECTION("null row falls back to table key") {
        REQUIRE(kafka_message_key_for_row(schema, table, nullptr, "id") == "nation.countries");
    }

    SECTION("integer PK") {
        const nlohmann::json row = {{"id", 42}, {"name", "foo"}};
        REQUIRE(kafka_message_key_for_row(schema, table, &row, "id") == "nation.countries|id=42");
    }

    SECTION("string PK with quotes") {
        const nlohmann::json row = {{"code", "US"}};
        REQUIRE(kafka_message_key_for_row(schema, table, &row, "code") == "nation.countries|code='US'");
    }

    SECTION("composite PK uses first column only") {
        const nlohmann::json row = {{"region_id", 1}, {"country_id", 99}};
        REQUIRE(kafka_message_key_for_row(schema, table, &row, "region_id, country_id") ==
                "nation.countries|region_id=1");
    }

    SECTION("missing PK column falls back") {
        const nlohmann::json row = {{"name", "x"}};
        REQUIRE(kafka_message_key_for_row(schema, table, &row, "id") == "nation.countries");
    }

    SECTION("null PK value falls back") {
        const nlohmann::json row = {{"id", nullptr}};
        REQUIRE(kafka_message_key_for_row(schema, table, &row, "id") == "nation.countries");
    }
}
