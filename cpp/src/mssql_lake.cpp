#include "mssql_lake.hpp"

#include <cctype>
#include <functional>
#include <set>

namespace {

bool is_ident_start(char c) {
    return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

bool is_ident_char(char c) {
    return is_ident_start(c) || std::isdigit(static_cast<unsigned char>(c));
}

std::string disambiguate_seen(std::string base, const std::string& raw_name, std::set<std::string>& seen, std::size_t max_len) {
    if (!seen.count(base)) {
        seen.insert(base);
        return base;
    }
    const auto hash_suffix = [&](const std::string& seed) {
        const auto h = std::hash<std::string>{}(seed);
        return std::string("_") + std::to_string(h % 1000000ULL);
    };
    std::string suffix = hash_suffix(raw_name);
    std::size_t keep = max_len > suffix.size() ? max_len - suffix.size() : 0;
    std::string candidate = base.substr(0, std::min(base.size(), keep)) + suffix;
    while (seen.count(candidate)) {
        suffix = hash_suffix(raw_name + suffix);
        keep = max_len > suffix.size() ? max_len - suffix.size() : 0;
        candidate = base.substr(0, std::min(base.size(), keep)) + suffix;
    }
    seen.insert(candidate);
    return candidate;
}

}  // namespace

std::string sanitize_pg_identifier_part(const std::string& name, std::size_t max_len, std::set<std::string>* seen) {
    std::string out;
    out.reserve(name.size());
    for (char c : name) {
        if (is_ident_char(c)) {
            out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        } else {
            out.push_back('_');
        }
    }
    while (!out.empty() && out.front() == '_') {
        out.erase(out.begin());
    }
    if (out.empty()) {
        out.push_back('x');
    }
    if (std::isdigit(static_cast<unsigned char>(out.front()))) {
        out = "t_" + out;
    }
    if (out.size() > max_len) {
        out.resize(max_len);
    }
    if (seen) {
        return disambiguate_seen(std::move(out), name, *seen, max_len);
    }
    return out;
}

std::string mssql_pg_schema_name(const std::string& database_name, const std::string& schema_name) {
    return sanitize_pg_identifier_part(database_name + "_" + schema_name);
}

std::string mssql_pg_table_name(const std::string& table_name) {
    return sanitize_pg_identifier_part(table_name);
}
