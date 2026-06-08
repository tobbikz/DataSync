#include "mariadb_preflight.hpp"

#include <cctype>
#include <map>
#include <stdexcept>
#include <string>

namespace {

std::string upper_ascii(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return s;
}

std::map<std::string, std::string> fetch_mariadb_variables(MYSQL* mysql) {
    std::map<std::string, std::string> out;
    if (mysql_query(
            mysql,
            "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','gtid_mode')") != 0) {
        throw std::runtime_error(std::string("SHOW VARIABLES failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        throw std::runtime_error(std::string("SHOW VARIABLES store_result failed: ") + mysql_error(mysql));
    }

    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (row[0] && row[1]) {
            out[row[0]] = row[1];
        }
    }
    mysql_free_result(res);
    return out;
}

}  // namespace

MariaDbPreflightResult check_mariadb_cdc_ready(MYSQL* mysql) {
    MariaDbPreflightResult result;
    if (!mysql) {
        result.ok = false;
        result.errors.push_back("mysql handle is null");
        return result;
    }

    std::map<std::string, std::string> vars;
    try {
        vars = fetch_mariadb_variables(mysql);
    } catch (const std::exception& ex) {
        result.ok = false;
        result.errors.push_back(ex.what());
        return result;
    }

    const auto require = [&](const char* name) -> std::string {
        const auto it = vars.find(name);
        if (it == vars.end()) {
            result.ok = false;
            result.errors.push_back(std::string(name) + " variable not found");
            return {};
        }
        return it->second;
    };

    const std::string log_bin = require("log_bin");
    const std::string binlog_format = require("binlog_format");
    if (!result.ok) {
        return result;
    }

    if (upper_ascii(log_bin) != "ON") {
        result.ok = false;
        result.errors.push_back("log_bin=" + log_bin + " (required ON)");
    }
    if (upper_ascii(binlog_format) != "ROW") {
        result.ok = false;
        result.errors.push_back("binlog_format=" + binlog_format + " (required ROW)");
    }

    // gtid_mode is optional — missing on some MariaDB builds; warn only (same as verify_sources.py).
    const auto gtid_it = vars.find("gtid_mode");
    if (gtid_it == vars.end()) {
        result.warnings.push_back("gtid_mode variable not found (recommended ON for prod failover)");
    } else if (upper_ascii(gtid_it->second) != "ON") {
        result.warnings.push_back("gtid_mode=" + gtid_it->second + " (recommended ON for CDC)");
    }

    return result;
}
