#include "mariadb_preflight.hpp"

#include <cctype>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

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
            "SHOW VARIABLES WHERE Variable_name IN ("
            "'log_bin','binlog_format','binlog_row_image','server_id',"
            "'gtid_strict_mode','gtid_domain_id',"
            "'binlog_expire_logs_seconds','expire_logs_days')") != 0) {
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

/** Single-value query. nullopt when the server rejects it (variable absent on this flavor). */
std::optional<std::string> query_scalar(MYSQL* mysql, const char* sql) {
    if (mysql_query(mysql, sql) != 0) {
        return std::nullopt;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return std::nullopt;
    }
    std::optional<std::string> out;
    if (MYSQL_ROW row = mysql_fetch_row(res); row != nullptr) {
        out = row[0] ? std::string(row[0]) : std::string();
    }
    mysql_free_result(res);
    return out;
}

std::vector<std::string> fetch_grants(MYSQL* mysql) {
    std::vector<std::string> out;
    if (mysql_query(mysql, "SHOW GRANTS FOR CURRENT_USER()") != 0) {
        return out;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return out;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (row[0]) {
            out.emplace_back(row[0]);
        }
    }
    mysql_free_result(res);
    return out;
}

/** Binlog retention in seconds. nullopt when unset/never-expire or unparsable. */
std::optional<double> binlog_retention_seconds(const std::map<std::string, std::string>& vars) {
    const auto to_double = [](const std::string& text) -> std::optional<double> {
        try {
            return std::stod(text);
        } catch (const std::exception&) {
            return std::nullopt;
        }
    };
    if (const auto it = vars.find("binlog_expire_logs_seconds"); it != vars.end()) {
        if (const auto secs = to_double(it->second); secs && *secs > 0.0) {
            return secs;
        }
    }
    if (const auto it = vars.find("expire_logs_days"); it != vars.end()) {
        if (const auto days = to_double(it->second); days && *days > 0.0) {
            return *days * 86400.0;
        }
    }
    return std::nullopt;
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

    for (const auto& [name, value] : vars) {
        result.facts[name] = value;
    }

    if (upper_ascii(log_bin) != "ON") {
        result.ok = false;
        result.errors.push_back("log_bin=" + log_bin + " (required ON)");
    }
    if (upper_ascii(binlog_format) != "ROW") {
        result.ok = false;
        result.errors.push_back("binlog_format=" + binlog_format + " (required ROW)");
    }

    // Without full row images an UPDATE only carries the changed columns and a DELETE only the PK,
    // so the lake mirror cannot be rebuilt from the event alone.
    if (const auto it = vars.find("binlog_row_image"); it != vars.end()) {
        if (upper_ascii(it->second) != "FULL") {
            result.ok = false;
            result.errors.push_back("binlog_row_image=" + it->second + " (required FULL)");
        }
    }

    // The capture registers as a replica; server_id=0 makes the master refuse the binlog stream.
    if (const auto it = vars.find("server_id"); it != vars.end()) {
        if (it->second == "0") {
            result.ok = false;
            result.errors.push_back("server_id=0 (required non-zero for binlog streaming)");
        }
    }

    const auto gtid_pos = query_scalar(mysql, "SELECT @@gtid_current_pos");
    if (!gtid_pos) {
        result.warnings.push_back(
            "@@gtid_current_pos unavailable (server is not MariaDB or GTID is not supported)");
    } else {
        result.facts["gtid_current_pos"] = *gtid_pos;
        if (gtid_pos->empty()) {
            result.warnings.push_back(
                "gtid_current_pos is empty (no GTID written yet; capture positions itself on first event)");
        }
    }

    if (const auto retention = binlog_retention_seconds(vars); retention && *retention < 86400.0) {
        result.warnings.push_back(
            "binlog retention is under 24h (" + std::to_string(static_cast<long long>(*retention)) +
            "s): a stopped capture can outlive the binlog and force a full-load reboot");
    }

    const auto grants = fetch_grants(mysql);
    if (grants.empty()) {
        result.warnings.push_back("SHOW GRANTS failed: replication privileges not verified");
    } else {
        bool slave = false;
        bool client = false;
        for (const auto& grant : grants) {
            const std::string up = upper_ascii(grant);
            // REPLICATION SLAVE/CLIENT are global-only, so a database-scoped ALL PRIVILEGES
            // (ON `db`.*) does not confer them.
            const bool global_scope = up.find("ON *.*") != std::string::npos;
            if (global_scope && up.find("ALL PRIVILEGES") != std::string::npos) {
                slave = true;
                client = true;
            }
            if (up.find("REPLICATION SLAVE") != std::string::npos) {
                slave = true;
            }
            // MariaDB 10.5+ renders REPLICATION CLIENT as BINLOG MONITOR.
            if (up.find("REPLICATION CLIENT") != std::string::npos ||
                up.find("BINLOG MONITOR") != std::string::npos) {
                client = true;
            }
        }
        result.facts["replication_slave_grant"] = slave ? "yes" : "no";
        result.facts["replication_client_grant"] = client ? "yes" : "no";
        if (!slave || !client) {
            result.warnings.push_back(
                "user may lack REPLICATION SLAVE / REPLICATION CLIENT (aka BINLOG MONITOR); grants are "
                "parsed heuristically, but if they are truly missing the capture cannot open the "
                "binlog stream");
        }
    }

    return result;
}

MariaDbPreflightResult check_mariadb_load_ready(MYSQL* mysql) {
    MariaDbPreflightResult result;
    if (!mysql) {
        result.ok = false;
        result.errors.push_back("mysql handle is null");
        return result;
    }
    if (mysql_query(mysql, "SELECT 1") != 0) {
        result.ok = false;
        result.errors.push_back(std::string("connection check failed: ") + mysql_error(mysql));
        return result;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        result.ok = false;
        result.errors.push_back(
            std::string("connection check store_result failed: ") + mysql_error(mysql));
        return result;
    }
    mysql_free_result(res);
    if (const char* version = mysql_get_server_info(mysql); version != nullptr) {
        result.facts["server_version"] = version;
    }
    return result;
}
