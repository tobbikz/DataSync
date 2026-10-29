#pragma once

#include "config.hpp"

#include <nlohmann/json.hpp>

#include <string>
#include <vector>

namespace mariadb_reselect {

/**
 * T12: SELECT * FROM schema.table WHERE pk… and merge into `row` for null/missing keys.
 * Returns true if a source row was found and merged.
 */
bool merge_row_from_source(
    const MariaDbSource& source,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& pk_columns,
    nlohmann::json& row);

}  // namespace mariadb_reselect
