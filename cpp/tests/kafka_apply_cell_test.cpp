#include "kafka_apply_detail.hpp"

#include <iostream>

using json = nlohmann::json;

int main() {
    int failures = 0;
    json row = {{"name", "x"}, {"id", nullptr}};
    json payload = {
        {"op", "u"},
        {"before", {{"id", 42}, {"name", "old"}}},
        {"after", {{"name", "x"}}},
    };
    kafka_apply_detail::enrich_apply_row_from_payload(row, payload, "u", {"id"});
    if (!row.contains("id") || !row["id"].is_number_integer() || row["id"].get<int>() != 42) {
        std::cerr << "FAIL enrich_apply_row_from_payload id=" << row.dump() << "\n";
        failures += 1;
    }
    if (failures == 0) {
        std::cout << "kafka_apply_cell_test: ok\n";
    }
    return failures;
}
