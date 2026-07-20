#include "cdc_envelope.hpp"
#include "kafka_apply_detail.hpp"
#include "mariadb_boolean.hpp"

#include <iostream>
#include <string>

using json = nlohmann::json;

namespace {

int fail_msg(const std::string& msg) {
    std::cerr << "FAIL " << msg << "\n";
    return 1;
}

}  // namespace

int main() {
    int failures = 0;

    // Existing enrich regression
    {
        json row = {{"name", "x"}, {"id", nullptr}};
        json payload = {
            {"op", "u"},
            {"before", {{"id", 42}, {"name", "old"}}},
            {"after", {{"name", "x"}}},
        };
        kafka_apply_detail::enrich_apply_row_from_payload(row, payload, "u", {"id"});
        if (!row.contains("id") || !row["id"].is_number_integer() || row["id"].get<int>() != 42) {
            failures += fail_msg("enrich_apply_row_from_payload id=" + row.dump());
        }
    }

    // Capture: b'1' / b'0' → JSON bool
    {
        const json t = parse_sql_literal("b'1'");
        const json f = parse_sql_literal("b'0'");
        if (!t.is_boolean() || !t.get<bool>()) {
            failures += fail_msg("parse_sql_literal(b'1')=" + t.dump());
        }
        if (!f.is_boolean() || f.get<bool>()) {
            failures += fail_msg("parse_sql_literal(b'0')=" + f.dump());
        }
    }

    // Apply BOOLEAN COPY cells (via json_cell_csv)
    auto expect_cell = [&](const json& val, const std::string& want, const char* label) {
        const std::string got = kafka_apply_detail::json_cell_csv(val, "BOOLEAN");
        if (got != want) {
            failures += fail_msg(std::string(label) + " got=[" + got + "] want=[" + want + "]");
        }
    };

    expect_cell(json(1), "t", "int 1");
    expect_cell(json(0), "f", "int 0");
    expect_cell(json("1"), "t", "str 1");
    expect_cell(json("b'1'"), "t", "str b'1'");
    expect_cell(json("\\x01"), "t", "str \\x01");
    expect_cell(json("0"), "f", "str 0");
    expect_cell(json("b'0'"), "f", "str b'0'");
    expect_cell(json("\\x00"), "f", "str \\x00");
    expect_cell(json(""), "", "empty → COPY null");
    expect_cell(json("garbage-bit"), "", "unknown → COPY null");
    expect_cell(json(nullptr), "", "json null");

    // Helper sanity (shared with full-load path)
    if (!try_parse_mariadb_bool_token("b'1'").value_or(false)) {
        failures += fail_msg("try_parse_mariadb_bool_token(b'1')");
    }
    if (try_parse_mariadb_bool_token("???").has_value()) {
        failures += fail_msg("try_parse_mariadb_bool_token(???) should be nullopt");
    }

    if (failures == 0) {
        std::cout << "kafka_apply_cell_test: ok\n";
    }
    return failures;
}
