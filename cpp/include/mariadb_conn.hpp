#pragma once

#include "config.hpp"

#include <mysql/mysql.h>

#include <stdexcept>
#include <string>

struct MariaDbRetryOptions {
    int max_attempts{12};
    int base_ms{500};
    int max_ms{60000};
};

struct MariaDbConn {
    MYSQL* handle{nullptr};

    explicit MariaDbConn(const MariaDbSource& src);
    ~MariaDbConn();

    MariaDbConn(const MariaDbConn&) = delete;
    MariaDbConn& operator=(const MariaDbConn&) = delete;

    void reconnect();
    const MariaDbSource& source() const { return source_; }

private:
    MariaDbSource source_;

    static MYSQL* open_handle(const MariaDbSource& src);
};

const MariaDbSource* find_mariadb_source(const AppConfig& cfg, const std::string& conn_id);

bool mariadb_error_is_transient(const std::string& message);
bool mariadb_errno_is_transient(unsigned int err);

void mariadb_sleep_retry_backoff(int attempt, const MariaDbRetryOptions& opts);

/** Run mysql_query with reconnect + exponential backoff on transient connection errors. */
void mariadb_mysql_query_retry(MariaDbConn& conn, const std::string& sql, const MariaDbRetryOptions& opts);

/** mysql_query + mysql_store_result with the same retry policy (caller must mysql_free_result). */
MYSQL_RES* mariadb_mysql_query_store_retry(MariaDbConn& conn, const std::string& sql, const MariaDbRetryOptions& opts);
