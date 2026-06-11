#include <catch2/catch_test_macros.hpp>

#include "obs_log.hpp"

#include <regex>

TEST_CASE("make_batch_id format", "[obs_log]") {
    const std::string id = make_batch_id();
    REQUIRE(std::regex_match(id, std::regex(R"(\d{8}_\d{6})")));
}

TEST_CASE("make_log builds event", "[obs_log]") {
    const auto e = make_log(LogLevel::Info, "test", "hello", {{"n", 1}}, "batch1", "CONN");
    REQUIRE(e.component == "test");
    REQUIRE(e.message == "hello");
    REQUIRE(e.batch_id == "batch1");
    REQUIRE(e.conn_id == "CONN");
    REQUIRE(e.context["n"] == 1);
}
