#include "full_load_common.hpp"

#include "mariadb_schema.hpp"
#include "pg_conn.hpp"

#include <iomanip>
#include <sstream>

namespace full_load {

long long elapsed_ms(const std::chrono::steady_clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

std::string utc_now_ts() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S+00");
    return oss.str();
}

std::string utc_now_date() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d");
    return oss.str();
}

std::string csv_escape(const std::string& value) {
    bool quote = value.empty();
    for (char c : value) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            quote = true;
            break;
        }
    }
    if (!quote) {
        return value;
    }
    std::string out = "\"";
    for (char c : value) {
        out += (c == '"') ? "\"\"" : std::string(1, c);
    }
    out += "\"";
    return out;
}

long long lake_table_row_count(PGconn* pg, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT COUNT(*)::bigint FROM " + pg_ident(schema) + "." + pg_ident(table);
    PGresult* res = PQexec(pg, sql.c_str());
    long long count = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoll(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

void acquire_full_load_table_lock(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(pg, "SELECT pg_advisory_lock($1::bigint)", 1, vals);
}

void release_full_load_table_lock(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(pg, "SELECT pg_advisory_unlock($1::bigint)", 1, vals);
}

void log(
    PGconn* log_pg,
    std::mutex* log_mtx,
    const std::string_view component,
    const LogLevel level,
    const std::string& batch_id,
    const std::string& message,
    const nlohmann::json& context,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table) {
    if (!log_pg) {
        return;
    }
    LogEvent ev;
    ev.level = level;
    ev.component = std::string(component);
    ev.message = message;
    ev.batch_id = batch_id;
    if (!conn_id.empty()) {
        ev.conn_id = conn_id;
    }
    if (!schema.empty()) {
        ev.source_schema = schema;
    }
    if (!table.empty()) {
        ev.source_table = table;
    }
    ev.context = context;
    if (log_mtx) {
        std::lock_guard<std::mutex> lock(*log_mtx);
        log_write(log_pg, ev);
    } else {
        log_write(log_pg, ev);
    }
}

}  // namespace full_load
