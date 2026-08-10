#pragma once

#include <libpq-fe.h>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

/** Decode libpq bytea (hex \\x... or raw binary) to bytes. */
inline std::vector<std::uint8_t> pg_bytea_to_bytes(const char* data, int len) {
    if (!data || len <= 0) {
        return {};
    }
    if (len >= 2 && data[0] == '\\' && data[1] == 'x') {
        const int hex_len = len - 2;
        if (hex_len <= 0 || (hex_len % 2) != 0) {
            return {};
        }
        std::vector<std::uint8_t> out;
        out.reserve(static_cast<std::size_t>(hex_len / 2));
        for (int i = 2; i + 1 < len; i += 2) {
            char hex_pair[3] = {data[i], data[i + 1], '\0'};
            out.push_back(static_cast<std::uint8_t>(std::strtoul(hex_pair, nullptr, 16)));
        }
        return out;
    }
    return std::vector<std::uint8_t>(data, data + len);
}

struct PgRetryOptions {
    int max_attempts{0};
    int base_ms{500};
    int max_ms{60000};
};

struct PgConn {
    std::string conninfo_;
    PGconn* raw{nullptr};

    explicit PgConn(const std::string& conninfo);
    ~PgConn();

    PgConn(const PgConn&) = delete;
    PgConn& operator=(const PgConn&) = delete;

    void reconnect();
};

void pg_abort_copy_in(PGconn* pg);
bool pg_full_load_error_is_transient(PGconn* pg, const std::string& err);
void pg_sleep_retry_backoff(int attempt, const PgRetryOptions& opts);
void pg_copy_batch_with_retry(
    PgConn& lake_pg,
    const std::string& copy_sql,
    const std::vector<std::string>& batch_lines,
    const PgRetryOptions& opts);

void pg_exec(PGconn* pg, const std::string& sql);
void pg_exec_params_simple(PGconn* pg, const char* sql, int n, const char* const* vals);
/** Like pg_exec_params_simple but retries on deadlock (40P01). Issues ROLLBACK between attempts. */
void pg_exec_params_retry_deadlock(
    PGconn* pg,
    const char* sql,
    int n,
    const char* const* vals,
    int max_attempts = 5,
    int base_sleep_ms = 25);
