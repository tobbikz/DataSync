#include "mariadb_datetime.hpp"

#include <cctype>

bool is_invalid_sql_date(const std::string& s) {
    return s.size() < 10 || s[4] != '-' || s[7] != '-' || s.substr(5, 2) == "00" || s.substr(8, 2) == "00";
}

std::string fix_date_separators(std::string s) {
    if (s.size() >= 10 && s[4] == ':' && s[7] == ':') {
        s[4] = '-';
        s[7] = '-';
    }
    return s;
}

std::string normalize_text_for_pg(const std::string& s, const std::string& pg_type) {
    if (pg_type == "DATE") {
        const std::string d = fix_date_separators(s);
        if (is_invalid_sql_date(d)) {
            return {};
        }
        return d;
    }
    if (pg_type == "TIMESTAMPTZ") {
        std::string t = fix_date_separators(s);
        if (t.find('+') == std::string::npos && t.find('Z') == std::string::npos) {
            t += "+00";
        }
        return t;
    }
    return s;
}

std::string pg_escape_literal(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        out += (c == '\'') ? "''" : std::string(1, c);
    }
    out += "'";
    return out;
}

std::string normalize_pg_sql_literal(const std::string& sql_lit, const std::string& pg_type) {
    if (sql_lit == "NULL" || sql_lit.empty()) {
        return "NULL";
    }
    if (pg_type != "DATE" && pg_type != "TIMESTAMPTZ") {
        return sql_lit;
    }
    if (sql_lit.size() < 2 || sql_lit.front() != '\'' || sql_lit.back() != '\'') {
        return sql_lit;
    }
    const std::string inner = sql_lit.substr(1, sql_lit.size() - 2);
    const std::string norm = normalize_text_for_pg(inner, pg_type);
    if (pg_type == "DATE" && norm.empty()) {
        return "NULL";
    }
    return pg_escape_literal(norm);
}
