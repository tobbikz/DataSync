#include "column_policy.hpp"

#include <openssl/sha.h>

#include <iomanip>
#include <sstream>

namespace column_policy {
namespace {

std::vector<std::regex> parse_regex_list(const nlohmann::json& arr) {
    std::vector<std::regex> out;
    if (!arr.is_array()) {
        return out;
    }
    for (const auto& item : arr) {
        if (!item.is_string()) {
            continue;
        }
        try {
            out.emplace_back(item.get<std::string>(), std::regex::ECMAScript | std::regex::icase);
        } catch (...) {
            // Invalid pattern: skip rather than fail discover.
        }
    }
    return out;
}

std::unordered_set<std::string> parse_string_set(const nlohmann::json& arr) {
    std::unordered_set<std::string> out;
    if (!arr.is_array()) {
        return out;
    }
    for (const auto& item : arr) {
        if (item.is_string()) {
            out.insert(item.get<std::string>());
        }
    }
    return out;
}

}  // namespace

TableFilter table_filter_from_extras(const nlohmann::json& extras) {
    TableFilter f;
    if (!extras.is_object()) {
        return f;
    }
    if (extras.contains("table_include")) {
        f.include = parse_regex_list(extras["table_include"]);
    }
    if (extras.contains("table_exclude")) {
        f.exclude = parse_regex_list(extras["table_exclude"]);
    }
    return f;
}

bool table_allowed(const TableFilter& filter, std::string_view schema, std::string_view table) {
    if (filter.empty()) {
        return true;
    }
    const std::string fqn = std::string(schema) + "." + std::string(table);
    const std::string bare(table);
    auto matches_any = [&](const std::vector<std::regex>& regs) {
        for (const auto& re : regs) {
            if (std::regex_match(fqn, re) || std::regex_match(bare, re)) {
                return true;
            }
        }
        return false;
    };
    for (const auto& re : filter.exclude) {
        if (std::regex_match(fqn, re) || std::regex_match(bare, re)) {
            return false;
        }
    }
    if (!filter.include.empty() && !matches_any(filter.include)) {
        return false;
    }
    return true;
}

bool is_online_ddl_helper_table(std::string_view table) {
    if (table.empty()) {
        return false;
    }
    const std::string t(table);
    auto ends_with = [&](const char* suf) {
        const size_t n = std::char_traits<char>::length(suf);
        return t.size() >= n && t.compare(t.size() - n, n, suf) == 0;
    };
    // gh-ost
    if (ends_with("_gho") || ends_with("_ghc") || ends_with("_del")) {
        return true;
    }
    // pt-osc: _tablename_new / _tablename_old
    if (t.size() >= 5 && t.front() == '_' && (ends_with("_new") || ends_with("_old"))) {
        return true;
    }
    return false;
}

bool capture_online_ddl_helpers(const nlohmann::json& extras) {
    return extras.is_object() && extras.contains("capture_online_ddl_helpers") &&
           extras["capture_online_ddl_helpers"].is_boolean() &&
           extras["capture_online_ddl_helpers"].get<bool>();
}

ColumnFilter column_filter_from_meta(const nlohmann::json& engine_meta) {
    ColumnFilter f;
    if (!engine_meta.is_object()) {
        return f;
    }
    if (engine_meta.contains("column_include")) {
        f.include = parse_string_set(engine_meta["column_include"]);
    }
    if (engine_meta.contains("column_exclude")) {
        f.exclude = parse_string_set(engine_meta["column_exclude"]);
    }
    if (engine_meta.contains("column_mask") && engine_meta["column_mask"].is_object()) {
        for (auto it = engine_meta["column_mask"].begin(); it != engine_meta["column_mask"].end(); ++it) {
            if (it.value().is_string()) {
                f.mask[it.key()] = it.value().get<std::string>();
            } else if (it.value().is_null()) {
                f.mask[it.key()] = "hash";
            }
        }
    }
    return f;
}

bool column_allowed(const ColumnFilter& filter, std::string_view column) {
    const std::string col(column);
    if (!filter.exclude.empty() && filter.exclude.count(col)) {
        return false;
    }
    if (!filter.include.empty() && !filter.include.count(col)) {
        return false;
    }
    return true;
}

std::string sha256_hex(std::string_view value) {
    if (value.empty()) {
        return {};
    }
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(value.data()), value.size(), hash);
    std::ostringstream oss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return oss.str();
}

std::string maybe_mask_value(
    const ColumnFilter& filter,
    std::string_view column,
    std::string_view value) {
    const auto it = filter.mask.find(std::string(column));
    if (it == filter.mask.end()) {
        return std::string(value);
    }
    if (it->second == "hash" || it->second.empty()) {
        return sha256_hex(value);
    }
    return std::string(value);
}

}  // namespace column_policy
