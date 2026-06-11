#include "mariadb_copy_format.hpp"

#include "mariadb_datetime.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace {

bool is_binary_type(const std::string& mysql_type_raw) {
    std::string lower = mysql_type_raw;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    return lower.find("blob") != std::string::npos || lower.find("binary") != std::string::npos;
}

std::string csv_escape(const std::string& value) {
    bool quote = value.empty();
    for (char c : value) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            quote = true;
            break;
        }
    }
    if (!quote) {
        return value;
    }
    std::string out = "\"";
    for (char c : value) {
        out += (c == '"') ? "\"\"" : std::string(1, c);
    }
    out += "\"";
    return out;
}

}  // namespace

std::string mariadb_format_copy_cell(const char* data, unsigned long len, const MariaDbColumn& col) {
    const bool missing = !data || len == 0;
    // COPY ... FORMAT csv: NULL is an unquoted empty field (not text-format \N).
    auto csv_null = []() { return std::string{}; };
    if (col.pg_type == "BYTEA" || is_binary_type(col.mysql_type)) {
        if (missing) {
            if (!col.is_nullable) {
                return mariadb_bytea_to_copy_csv(static_cast<const char*>(nullptr), 0);
            }
            return csv_null();
        }
        return mariadb_bytea_to_copy_csv(data, static_cast<std::size_t>(len));
    }
    if (missing) {
        if (!col.is_nullable || col.is_pk) {
            return csv_escape(mariadb_not_null_copy_default(col));
        }
        if (col.pg_type == "TIMESTAMPTZ" || col.pg_type == "TIMESTAMP" || col.pg_type == "DATE" ||
            col.pg_type == "TIME") {
            return csv_null();
        }
        return csv_null();
    }
    const std::string s = normalize_text_for_pg(std::string(data, len), col.pg_type);
    if (s.empty()) {
        if (!col.is_nullable || col.is_pk) {
            return csv_escape(mariadb_not_null_copy_default(col));
        }
        if (col.pg_type == "TIMESTAMPTZ" || col.pg_type == "TIMESTAMP" || col.pg_type == "DATE" ||
            col.pg_type == "TIME") {
            return csv_null();
        }
        return csv_null();
    }
    return csv_escape(s);
}
