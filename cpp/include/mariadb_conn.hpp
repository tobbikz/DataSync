#pragma once

#include "config.hpp"

#include <mysql/mysql.h>

#include <stdexcept>
#include <string>

struct MariaDbConn {
    MYSQL* handle{nullptr};

    explicit MariaDbConn(const MariaDbSource& src) {
        handle = mysql_init(nullptr);
        if (!handle) {
            throw std::runtime_error("mysql_init failed");
        }
        unsigned int timeout = 30;
        mysql_options(handle, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        if (!mysql_real_connect(
                handle,
                src.host.c_str(),
                src.user.c_str(),
                src.password.c_str(),
                nullptr,
                src.port,
                nullptr,
                0)) {
            throw std::runtime_error(
                std::string("MariaDB connect failed conn_id=") + src.conn_id + ": " + mysql_error(handle));
        }
        if (mysql_query(handle, "SET SESSION time_zone = '+00:00'") != 0) {
            throw std::runtime_error(
                std::string("MariaDB SET time_zone failed: ") + mysql_error(handle));
        }
    }

    ~MariaDbConn() {
        if (handle) {
            mysql_close(handle);
        }
    }

    MariaDbConn(const MariaDbConn&) = delete;
    MariaDbConn& operator=(const MariaDbConn&) = delete;
};

const MariaDbSource* find_mariadb_source(const AppConfig& cfg, const std::string& conn_id);
