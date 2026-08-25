#include "pg_conn.hpp"

#include <algorithm>
#include <chrono>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <thread>

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

PGconn* pg_connect_or_throw(const std::string& conninfo) {
    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (!pg) {
        throw std::runtime_error("PostgreSQL connect failed: PQconnectdb returned null");
    }
    if (PQstatus(pg) != CONNECTION_OK) {
        const std::string err = PQerrorMessage(pg);
        PQfinish(pg);
        throw std::runtime_error("PostgreSQL connect failed: " + err);
    }
    PGresult* tz = PQexec(pg, "SET TIME ZONE 'UTC'");
    if (!tz || PQresultStatus(tz) != PGRES_COMMAND_OK) {
        if (tz) {
            PQclear(tz);
        }
        const std::string err = PQerrorMessage(pg);
        PQfinish(pg);
        throw std::runtime_error("PostgreSQL SET TIME ZONE failed: " + err);
    }
    PQclear(tz);
    return pg;
}

int pg_retry_attempt_limit(int configured) {
    return configured <= 0 ? INT_MAX : std::max(1, configured);
}

}  // namespace

PgConn::PgConn(const std::string& conninfo) : conninfo_(conninfo) {
    raw = pg_connect_or_throw(conninfo_);
}

PgConn::~PgConn() {
    if (raw) {
        PQfinish(raw);
        raw = nullptr;
    }
}

void PgConn::reconnect() {
    if (raw) {
        PQfinish(raw);
        raw = nullptr;
    }
    raw = pg_connect_or_throw(conninfo_);
}

void pg_abort_copy_in(PGconn* pg) {
    if (!pg) {
        return;
    }
    PQputCopyEnd(pg, "abort");
    while (PGresult* r = PQgetResult(pg)) {
        PQclear(r);
    }
}

bool pg_full_load_error_is_transient(PGconn* pg, const std::string& err) {
    if (pg && PQstatus(pg) == CONNECTION_BAD) {
        return true;
    }
    if (contains_ci(err, "08P01") || contains_ci(err, "08006") || contains_ci(err, "57P01") ||
        contains_ci(err, "53300") || contains_ci(err, "08001") || contains_ci(err, "08003") ||
        contains_ci(err, "08004") || contains_ci(err, "08007") || contains_ci(err, "connection") ||
        contains_ci(err, "EOF") || contains_ci(err, "timeout") || contains_ci(err, "reset") ||
        contains_ci(err, "broken pipe") || contains_ci(err, "server closed")) {
        return true;
    }
    return false;
}

void pg_sleep_retry_backoff(int attempt, const PgRetryOptions& opts) {
    const int base_ms = std::max(1, opts.base_ms);
    const int max_ms = std::max(base_ms, opts.max_ms);
    const int shift = std::min(attempt, 10);
    const int delay_ms = std::min(max_ms, base_ms * (1 << shift));
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
}

void pg_copy_batch_with_retry(
    PgConn& lake_pg,
    const std::string& copy_sql,
    const std::vector<std::string>& batch_lines,
    const PgRetryOptions& opts,
    const std::function<void(PGconn*)>& in_txn_after_copy) {
    const int attempts = pg_retry_attempt_limit(opts.max_attempts);
    const bool transactional = static_cast<bool>(in_txn_after_copy);
    std::string last_err;

    for (int attempt = 0; attempt < attempts; ++attempt) {
        try {
            if (transactional) {
                pg_exec(lake_pg.raw, "BEGIN");
            }
            PGresult* copy_res = PQexec(lake_pg.raw, copy_sql.c_str());
            if (!copy_res || PQresultStatus(copy_res) != PGRES_COPY_IN) {
                last_err = PQerrorMessage(lake_pg.raw);
                if (copy_res) {
                    PQclear(copy_res);
                }
                throw std::runtime_error(std::string("COPY start failed: ") + last_err);
            }
            PQclear(copy_res);

            for (const auto& line : batch_lines) {
                if (PQputCopyData(lake_pg.raw, line.data(), static_cast<int>(line.size())) != 1 ||
                    PQputCopyData(lake_pg.raw, "\n", 1) != 1) {
                    last_err = PQerrorMessage(lake_pg.raw);
                    pg_abort_copy_in(lake_pg.raw);
                    throw std::runtime_error(std::string("PQputCopyData failed: ") + last_err);
                }
            }

            if (PQputCopyEnd(lake_pg.raw, nullptr) != 1) {
                last_err = PQerrorMessage(lake_pg.raw);
                pg_abort_copy_in(lake_pg.raw);
                throw std::runtime_error(std::string("PQputCopyEnd failed: ") + last_err);
            }

            PGresult* end_res = PQgetResult(lake_pg.raw);
            while (end_res) {
                if (PQresultStatus(end_res) != PGRES_COMMAND_OK) {
                    last_err = PQerrorMessage(lake_pg.raw);
                    PQclear(end_res);
                    while (PGresult* r = PQgetResult(lake_pg.raw)) {
                        PQclear(r);
                    }
                    throw std::runtime_error(std::string("COPY failed: ") + last_err);
                }
                PQclear(end_res);
                end_res = PQgetResult(lake_pg.raw);
            }
            if (transactional) {
                in_txn_after_copy(lake_pg.raw);
                pg_exec(lake_pg.raw, "COMMIT");
            }
            return;
        } catch (const std::runtime_error& ex) {
            last_err = ex.what();
            pg_abort_copy_in(lake_pg.raw);
            if (transactional) {
                PQclear(PQexec(lake_pg.raw, "ROLLBACK"));
            }
            if (!pg_full_load_error_is_transient(lake_pg.raw, last_err) || attempt + 1 >= attempts) {
                throw;
            }
            lake_pg.reconnect();
            pg_sleep_retry_backoff(attempt, opts);
        }
    }

    throw std::runtime_error("COPY batch retry exhausted: " + last_err);
}

void pg_exec(PGconn* pg, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        const std::string err = pg ? PQerrorMessage(pg) : "null connection";
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("PostgreSQL exec failed: " + err);
    }
    PQclear(res);
}

void pg_exec_params_simple(PGconn* pg, const char* sql, int n, const char* const* vals) {
    PGresult* res = PQexecParams(pg, sql, n, nullptr, vals, nullptr, nullptr, 0);
    if (!res || (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK)) {
        const std::string err = pg ? PQerrorMessage(pg) : "null connection";
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("PostgreSQL exec_params failed: " + err);
    }
    PQclear(res);
}

namespace {

bool pg_err_is_deadlock(const std::string& err) {
    return contains_ci(err, "40P01") || contains_ci(err, "deadlock detected");
}

std::string pg_result_err_text(PGconn* pg, PGresult* res) {
    std::string err = pg && PQerrorMessage(pg) ? PQerrorMessage(pg) : "";
    if (const char* msg = res ? PQresultErrorMessage(res) : nullptr) {
        if (msg[0]) {
            if (!err.empty()) {
                err += " | ";
            }
            err += msg;
        }
    }
    if (err.empty()) {
        err = "unknown PostgreSQL error";
    }
    return err;
}

}  // namespace

void pg_exec_params_retry_deadlock(
    PGconn* pg,
    const char* sql,
    int n,
    const char* const* vals,
    int max_attempts,
    int base_sleep_ms) {
    if (max_attempts < 1) {
        max_attempts = 1;
    }
    if (base_sleep_ms < 1) {
        base_sleep_ms = 1;
    }
    std::string last_err;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        PGresult* res = PQexecParams(pg, sql, n, nullptr, vals, nullptr, nullptr, 0);
        if (res &&
            (PQresultStatus(res) == PGRES_COMMAND_OK || PQresultStatus(res) == PGRES_TUPLES_OK)) {
            PQclear(res);
            return;
        }
        last_err = pg_result_err_text(pg, res);
        if (res) {
            PQclear(res);
        }
        if (!pg_err_is_deadlock(last_err) || attempt + 1 >= max_attempts) {
            break;
        }
        // Deadlock aborts the current transaction; clear so the next attempt can run.
        PGresult* rb = PQexec(pg, "ROLLBACK");
        if (rb) {
            PQclear(rb);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(base_sleep_ms * (attempt + 1)));
    }
    throw std::runtime_error("PostgreSQL exec_params failed after deadlock retries: " + last_err);
}
