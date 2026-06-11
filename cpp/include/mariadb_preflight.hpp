#pragma once

#include <mysql/mysql.h>

#include <string>
#include <vector>

struct MariaDbPreflightResult {
    bool ok{true};
    std::vector<std::string> errors;
    std::vector<std::string> warnings;
};

MariaDbPreflightResult check_mariadb_cdc_ready(MYSQL* mysql);
MariaDbPreflightResult check_mariadb_load_ready(MYSQL* mysql);
