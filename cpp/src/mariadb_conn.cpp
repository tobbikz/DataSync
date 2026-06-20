#include "mariadb_conn.hpp"

#include <algorithm>
#include <climits>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace {

bool contains_ci(const std::string& haystack, const char* needle) {
    if (!needle || !*needle) {
        return false;
    }
    const std::size_t nlen = std::strlen(needle);
    if (haystack.size() < nlen) {
        return false;
    }
    for (std::size_t i = 0; i + nlen <= haystack.size(); ++i) {
        if (strncasecmp(haystack.data() + i, needle, nlen) == 0) {
            return true;
        }
    }
    return false;
}

}  // namespace

bool mariadb_error_is_transient(const std::string& message) {
    if (message.empty()) {
        return false;
    }
    return contains_ci(message, "lost connection") || contains_ci(message, "server has gone away") ||
           contains_ci(message, "can't connect") || contains_ci(message, "reading from the stream has failed") ||
           contains_ci(message, "connection refused") || contains_ci(message, "timed out") ||
           contains_ci(message, "broken pipe");
}

bool mariadb_errno_is_transient(unsigned int err) {
    switch (err) {
    case 2002:  // CR_CONNECTION_ERROR
    case 2003:  // CR_CONN_HOST_ERROR
    case 2006:  // CR_SERVER_GONE_ERROR
    case 2013:  // CR_SERVER_LOST
    case 2055:  // CR_SERVER_LOST_EXTENDED
        return true;
    default:
        return false;
    }
}

void mariadb_sleep_retry_backoff(int attempt, const MariaDbRetryOptions& opts) {
    const int capped = std::min(attempt, 10);
    const int delay_ms = std::min(opts.max_ms, opts.base_ms * (1 << capped));
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
}

MYSQL* MariaDbConn::open_handle(const MariaDbSource& src) {
    MYSQL* raw = mysql_init(nullptr);
    if (!raw) {
        throw std::runtime_error("mysql_init failed");
    }
    unsigned int timeout = 30;
    mysql_options(raw, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    const bool local_host = src.host == "localhost" || src.host == "127.0.0.1" || src.host == "::1";
    const char* socket_path = nullptr;
    if (local_host && src.password.empty()) {
        static const char* kSockets[] = {
            "/run/mysqld/mysqld.sock",
            "/var/run/mysqld/mysqld.sock",
            "/run/mariadb/mariadb.sock",
            nullptr,
        };
        for (const char* const* p = kSockets; *p; ++p) {
            if (access(*p, F_OK) == 0) {
                socket_path = *p;
                break;
            }
        }
    }
    if (!mysql_real_connect(
            raw,
            socket_path ? "localhost" : src.host.c_str(),
            src.user.c_str(),
            src.password.c_str(),
            nullptr,
            socket_path ? 0 : src.port,
            socket_path,
            0)) {
        const std::string err = mysql_error(raw);
        mysql_close(raw);
        throw std::runtime_error(
            std::string("MariaDB connect failed conn_id=") + src.conn_id + ": " + err);
    }
    if (mysql_query(raw, "SET SESSION time_zone = '+00:00'") != 0) {
        const std::string err = mysql_error(raw);
        mysql_close(raw);
        throw std::runtime_error(std::string("MariaDB SET time_zone failed: ") + err);
    }
    return raw;
}

MariaDbConn::MariaDbConn(const MariaDbSource& src) : source_(src) {
    handle = open_handle(source_);
}

MariaDbConn::~MariaDbConn() {
    if (handle) {
        mysql_close(handle);
        handle = nullptr;
    }
}

void MariaDbConn::reconnect() {
    if (handle) {
        mysql_close(handle);
        handle = nullptr;
    }
    handle = open_handle(source_);
}

int mariadb_retry_attempt_limit(int configured) {
    return configured <= 0 ? INT_MAX : std::max(1, configured);
}

bool mariadb_should_retry(unsigned int err_no, const std::string& err, int attempt, int attempts) {
    const bool transient = mariadb_errno_is_transient(err_no) || mariadb_error_is_transient(err);
    return transient && attempt + 1 < attempts;
}

void mariadb_mysql_query_retry(MariaDbConn& conn, const std::string& sql, const MariaDbRetryOptions& opts) {
    if (!conn.handle) throw std::runtime_error("MariaDB handle is null");
    const int attempts = mariadb_retry_attempt_limit(opts.max_attempts);
    for (int attempt = 0; attempt < attempts; ++attempt) {
        if (mysql_query(conn.handle, sql.c_str()) == 0) {
            return;
        }
        const unsigned int err_no = mysql_errno(conn.handle);
        const std::string err = mysql_error(conn.handle);
        if (!mariadb_should_retry(err_no, err, attempt, attempts)) {
            throw std::runtime_error(std::string("MariaDB query failed: ") + err);
        }
        conn.reconnect();
        mariadb_sleep_retry_backoff(attempt, opts);
    }
}

MYSQL_RES* mariadb_mysql_query_store_retry(MariaDbConn& conn, const std::string& sql, const MariaDbRetryOptions& opts) {
    if (!conn.handle) throw std::runtime_error("MariaDB handle is null");
    const int attempts = mariadb_retry_attempt_limit(opts.max_attempts);
    for (int attempt = 0; attempt < attempts; ++attempt) {
        if (mysql_query(conn.handle, sql.c_str()) != 0) {
            const unsigned int err_no = mysql_errno(conn.handle);
            const std::string err = mysql_error(conn.handle);
            if (!mariadb_should_retry(err_no, err, attempt, attempts)) {
                throw std::runtime_error(std::string("MariaDB SELECT failed: ") + err);
            }
            conn.reconnect();
            mariadb_sleep_retry_backoff(attempt, opts);
            continue;
        }

        MYSQL_RES* res = mysql_store_result(conn.handle);
        if (res) {
            return res;
        }
        if (mysql_field_count(conn.handle) == 0) {
            return nullptr;
        }

        const unsigned int err_no = mysql_errno(conn.handle);
        const std::string err = mysql_error(conn.handle);
        if (!mariadb_should_retry(err_no, err, attempt, attempts)) {
            throw std::runtime_error(std::string("MariaDB store_result failed: ") + err);
        }
        conn.reconnect();
        mariadb_sleep_retry_backoff(attempt, opts);
    }
    throw std::runtime_error("MariaDB query/store exhausted retries");
}

const MariaDbSource* find_mariadb_source(const AppConfig& cfg, const std::string& conn_id) {
    for (const auto& src : cfg.mariadb_sources) {
        if (src.conn_id == conn_id) {
            return &src;
        }
    }
    return nullptr;
}
