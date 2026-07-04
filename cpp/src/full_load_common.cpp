#include "full_load_common.hpp"

#include "full_load_checkpoint.hpp"
#include "mariadb_schema.hpp"
#include "pg_conn.hpp"
#include "pipeline_defaults.hpp"

#include <cmath>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <thread>

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

TruncateResult truncate_lake_table_verified(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    int max_retries) {
    TruncateResult result;
    const int retries = std::max(1, max_retries);
    for (int attempt = 1; attempt <= retries; ++attempt) {
        result.attempts = attempt;
        try {
            truncate_lake_table(pg, schema, table);
        } catch (const std::exception& ex) {
            result.error = ex.what();
            if (attempt < retries) {
                const int backoff_ms = 1000 << (attempt - 1);
                std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                continue;
            }
            result.ok = false;
            result.rows_after = lake_table_row_count(pg, schema, table);
            return result;
        }
        result.rows_after = lake_table_row_count(pg, schema, table);
        if (result.rows_after == 0) {
            result.ok = true;
            result.error.clear();
            return result;
        }
        result.error = "rows_after_truncate=" + std::to_string(result.rows_after);
        if (attempt < retries) {
            const int backoff_ms = 1000 << (attempt - 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }
    result.ok = false;
    return result;
}

RowCountVerifyResult verify_full_load_row_counts(
    long long source_rows,
    long long lake_rows,
    long long rows_loaded) {
    RowCountVerifyResult result;
    result.source_rows = source_rows;
    result.lake_rows = lake_rows;
    result.diff = source_rows - lake_rows;
    if (!pipeline_defaults::kFullLoadRowCountVerify) {
        return result;
    }
    const long long loaded = rows_loaded > 0 ? rows_loaded : lake_rows;
    if (source_rows < 0 || loaded < 0) {
        return result;
    }
    if (source_rows == loaded) {
        return result;
    }
    const long long abs_diff = std::llabs(source_rows - loaded);
    if (source_rows <= pipeline_defaults::kFullLoadRowCountVerifyLargeTableThreshold) {
        result.ok = false;
        result.message = "row count mismatch (exact verify)";
        return result;
    }
    const double pct =
        source_rows > 0 ? static_cast<double>(abs_diff) / static_cast<double>(source_rows) : 1.0;
    if (pct > pipeline_defaults::kFullLoadRowCountVerifyTolerancePct) {
        result.ok = false;
        result.message = "row count mismatch above tolerance";
    }
    return result;
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

bool try_acquire_full_load_table_lock(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(pg, "SELECT pg_try_advisory_lock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    bool locked = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        locked = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return locked;
}

bool full_load_table_lock_held_by_other(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT EXISTS (
            SELECT 1
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND classid = 0
              AND objid = $1::bigint
              AND granted
              AND pid <> pg_backend_pid()
        )
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    bool held = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        held = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return held;
}

void save_copy_batch_checkpoint(
    CopyCheckpointContext& ctx,
    const std::vector<std::string>& last_pk_values,
    long long rows_loaded_total) {
    if (!ctx.app_pg || ctx.catalog_id <= 0) {
        return;
    }
    FullLoadCheckpoint cp;
    cp.catalog_id = ctx.catalog_id;
    cp.worker_id = ctx.worker_id;
    cp.batch_id = ctx.batch_id;
    cp.phase = FullLoadPhase::Copy;
    cp.last_pk = last_pk_to_json(last_pk_values);
    cp.rows_loaded = rows_loaded_total;
    cp.source_rows = ctx.source_rows;
    {
        std::unique_lock<std::mutex> lock;
        if (ctx.checkpoint_mtx) {
            lock = std::unique_lock<std::mutex>(*ctx.checkpoint_mtx);
        }
        save_full_load_checkpoint(ctx.app_pg, cp);
    }
    ctx.batches_completed += 1;
    if (!ctx.log_pg || ctx.progress_log_interval <= 0 ||
        (ctx.batches_completed % ctx.progress_log_interval) != 0) {
        return;
    }
    log(
        ctx.log_pg,
        ctx.log_mtx,
        ctx.log_component,
        LogLevel::Info,
        ctx.batch_id,
        "full load copy progress",
        {{"rows_loaded", rows_loaded_total},
         {"worker_id", ctx.worker_id},
         {"last_pk", cp.last_pk},
         {"batches_completed", ctx.batches_completed}},
        ctx.conn_id,
        ctx.source_schema,
        ctx.source_table);
}

bool should_resume_from_copy_checkpoint(
    PGconn* app_pg,
    PGconn* lake_pg,
    long long catalog_id,
    const std::string& lake_schema,
    const std::string& lake_table,
    std::vector<FullLoadCheckpoint>& checkpoints_out) {
    checkpoints_out = load_full_load_checkpoints(app_pg, catalog_id);
    if (checkpoints_out.empty()) {
        return false;
    }
    bool has_copy = false;
    long long checkpoint_rows = 0;
    for (const auto& cp : checkpoints_out) {
        if (cp.phase == FullLoadPhase::Copy) {
            has_copy = true;
            checkpoint_rows += cp.rows_loaded;
        }
    }
    if (!has_copy) {
        return false;
    }
    const long long lake_rows = lake_table_row_count(lake_pg, lake_schema, lake_table);
    if (lake_rows < 0 || lake_rows < checkpoint_rows) {
        clear_full_load_checkpoints(app_pg, catalog_id);
        checkpoints_out.clear();
        return false;
    }
    return true;
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
