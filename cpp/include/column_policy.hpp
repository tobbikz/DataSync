#pragma once

#include <nlohmann/json.hpp>

#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/** T01/T02/T03: table regex filters + column include/exclude + mask/hash. */
namespace column_policy {

struct TableFilter {
    std::vector<std::regex> include;
    std::vector<std::regex> exclude;
    bool empty() const { return include.empty() && exclude.empty(); }
};

/** Parse connections.extras table_include / table_exclude (JSON arrays of regex strings). */
TableFilter table_filter_from_extras(const nlohmann::json& extras);

/** Match schema.table (or table alone) against include/exclude. Empty include = allow all. */
bool table_allowed(const TableFilter& filter, std::string_view schema, std::string_view table);

/**
 * D08: gh-ost / pt-osc helper table names (`*_gho`, `*_ghc`, `*_del`, `_*_new`, `_*_old`).
 * Skipped at discover unless extras.capture_online_ddl_helpers=true.
 */
bool is_online_ddl_helper_table(std::string_view table);

/** True when connections.extras.capture_online_ddl_helpers is boolean true. */
bool capture_online_ddl_helpers(const nlohmann::json& extras);

struct ColumnFilter {
    std::unordered_set<std::string> include;
    std::unordered_set<std::string> exclude;
    /** col -> "hash" (or other modes later). */
    std::unordered_map<std::string, std::string> mask;
    bool empty() const { return include.empty() && exclude.empty() && mask.empty(); }
};

/** Parse catalog.engine_meta column_include / column_exclude / column_mask. */
ColumnFilter column_filter_from_meta(const nlohmann::json& engine_meta);

bool column_allowed(const ColumnFilter& filter, std::string_view column);

/** SHA-256 hex of UTF-8 bytes; empty input → empty string. */
std::string sha256_hex(std::string_view value);

/** Apply mask policy: if col is hashed, replace value; otherwise pass through. */
std::string maybe_mask_value(
    const ColumnFilter& filter,
    std::string_view column,
    std::string_view value);

}  // namespace column_policy
