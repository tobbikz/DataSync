#include "mariadb_reselect.hpp"

#include "mariadb_conn.hpp"

#include <sstream>
#include <stdexcept>

namespace mariadb_reselect {
namespace {

std::string sql_ident(const std::string& name) {
    std::string out = "`";
    for (char c : name) {
        if (c == '`') {
            out += "``";
        } else {
            out += c;
        }
    }
    out += "`";
    return out;
}

std::string sql_literal(const nlohmann::json& v) {
    if (v.is_null()) {
        return "NULL";
    }
    if (v.is_number_integer()) {
        return std::to_string(v.get<long long>());
    }
    if (v.is_number_unsigned()) {
        return std::to_string(v.get<unsigned long long>());
    }
    if (v.is_number_float()) {
        return std::to_string(v.get<double>());
    }
    if (v.is_boolean()) {
        return v.get<bool>() ? "1" : "0";
    }
    std::string s = v.is_string() ? v.get<std::string>() : v.dump();
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') {
            out += "''";
        } else if (c == '\\') {
            out += "\\\\";
        } else {
            out += c;
        }
    }
    out += "'";
    return out;
}

}  // namespace

bool merge_row_from_source(
    const MariaDbSource& source,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& pk_columns,
    nlohmann::json& row) {
    if (pk_columns.empty() || !row.is_object()) {
        return false;
    }
    for (const auto& pk : pk_columns) {
        if (!row.contains(pk) || row[pk].is_null()) {
            return false;
        }
    }

    try {
        MariaDbConn conn(source);
        if (!conn.handle) {
            return false;
        }
        std::ostringstream sql;
        sql << "SELECT * FROM " << sql_ident(schema) << "." << sql_ident(table) << " WHERE ";
        for (size_t i = 0; i < pk_columns.size(); ++i) {
            if (i) {
                sql << " AND ";
            }
            sql << sql_ident(pk_columns[i]) << "=" << sql_literal(row[pk_columns[i]]);
        }
        sql << " LIMIT 1";
        if (mysql_query(conn.handle, sql.str().c_str()) != 0) {
            return false;
        }
        MYSQL_RES* res = mysql_store_result(conn.handle);
        if (!res) {
            return false;
        }
        MYSQL_ROW mysql_row = mysql_fetch_row(res);
        if (!mysql_row) {
            mysql_free_result(res);
            return false;
        }
        const unsigned n = mysql_num_fields(res);
        MYSQL_FIELD* fields = mysql_fetch_fields(res);
        for (unsigned i = 0; i < n; ++i) {
            const std::string col = fields[i].name ? fields[i].name : "";
            if (col.empty()) {
                continue;
            }
            if (!row.contains(col) || row[col].is_null()) {
                if (mysql_row[i] == nullptr) {
                    row[col] = nullptr;
                } else {
                    row[col] = std::string(mysql_row[i]);
                }
            }
        }
        mysql_free_result(res);
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace mariadb_reselect
