#pragma once

#include <mysql/mysql.h>
#include <map>
#include <string>
#include <vector>

struct MariaDbPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
    /** Observed server settings (log_bin, binlog_format, gtid position, ...) for the report. */
    std::map<std::string, std::string> facts;
};

MariaDbPreflightResult check_mariadb_cdc_ready(MYSQL* mysql);
MariaDbPreflightResult check_mariadb_load_ready(MYSQL* mysql);
