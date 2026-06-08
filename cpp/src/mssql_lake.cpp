#include "mssql_lake.hpp"

#include <cctype>

namespace {

bool is_ident_start(char c) {
    return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

bool is_ident_char(char c) {
    return is_ident_start(c) || std::isdigit(static_cast<unsigned char>(c));
}

}  // namespace

std::string sanitize_pg_identifier_part(const std::string& name, std::size_t max_len) {
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
        out = "x";
    }
    if (std::isdigit(static_cast<unsigned char>(out.front()))) {
        out = "t_" + out;
    }
    if (out.size() > max_len) {
        out.resize(max_len);
    }
    return out;
}

std::string mssql_pg_schema_name(const std::string& database_name, const std::string& schema_name) {
    return sanitize_pg_identifier_part(database_name + "_" + schema_name);
}

std::string mssql_pg_table_name(const std::string& table_name) {
    return sanitize_pg_identifier_part(table_name);
}
