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

bool mariadb_format_copy_cell(const char* data, unsigned long len, const MariaDbColumn& col, std::string& out) {
    const bool is_null = data == nullptr;
    // COPY ... FORMAT csv: NULL is an unquoted empty field (not text-format \N).
    auto csv_null = [&out]() {
        out.clear();
        return true;
    };
    if (col.is_pk && is_null) {
        return false;
    }
    if (col.pg_type == "BYTEA" || is_binary_type(col.mysql_type)) {
        if (is_null) {
            if (!col.is_nullable) {
                out = mariadb_bytea_to_copy_csv(static_cast<const char*>(nullptr), 0);
                return true;
            }
            return csv_null();
        }
        out = mariadb_bytea_to_copy_csv(data, static_cast<std::size_t>(len));
        return true;
    }
    if (is_null) {
        if (!col.is_nullable) {
            out = csv_escape(mariadb_not_null_copy_default(col));
            return true;
        }
        if (col.pg_type == "TIMESTAMPTZ" || col.pg_type == "TIMESTAMP" || col.pg_type == "DATE" ||
            col.pg_type == "TIME") {
            return csv_null();
        }
        return csv_null();
    }
    if (len == 0) {
        if (col.is_pk) {
            return false;
        }
        if (!col.is_nullable) {
            out = csv_escape(mariadb_not_null_copy_default(col));
            return true;
        }
        if (col.pg_type == "BOOLEAN") {
            return csv_null();
        }
        out = csv_escape("");
        return true;
    }
    const std::string s = normalize_text_for_pg(std::string(data, len), col.pg_type);
    if (s.empty()) {
        if (col.is_pk) {
            return false;
        }
        if (!col.is_nullable) {
            out = csv_escape(mariadb_not_null_copy_default(col));
            return true;
        }
        if (col.pg_type == "TIMESTAMPTZ" || col.pg_type == "TIMESTAMP" || col.pg_type == "DATE" ||
            col.pg_type == "TIME" || col.pg_type == "BOOLEAN") {
            return csv_null();
        }
        out = csv_escape("");
        return true;
    }
    if (col.pg_type == "BOOLEAN") {
        if (s == "0" || s == "false" || s == "FALSE" || s == "f" || s == "F" || s == "no" || s == "NO") {
            out = "f";
            return true;
        }
        if (s == "1" || s == "true" || s == "TRUE" || s == "t" || s == "T" || s == "yes" || s == "YES") {
            out = "t";
            return true;
        }
        if (!col.is_nullable) {
            out = "f";
            return true;
        }
        return csv_null();
    }
    out = csv_escape(s);
    return true;
}
