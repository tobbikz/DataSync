#include "mariadb_full_load.hpp"

#include "capture_common.hpp"
#include "full_load_common.hpp"
#include "mariadb_binlog.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_copy_format.hpp"
#include "mariadb_ddl_sync.hpp"
#include "mariadb_datetime.hpp"
#include "mariadb_preflight.hpp"
#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct InvalidPkSkipStats {
    long long count{0};
    nlohmann::json samples{nlohmann::json::array()};

    void merge(const InvalidPkSkipStats& other) {
        count += other.count;
        for (const auto& sample : other.samples) {
            if (samples.size() >= 5) {
                break;
            }
            samples.push_back(sample);
        }
    }
};

struct CatalogTableRow {
    long long catalog_id{0};
    std::string conn_id;
    std::string source_schema;
    std::string source_table;
    bool has_pk{false};
    std::string pk_columns;
};

void log_fl(
    PGconn* log_pg,
    std::mutex* log_mtx,
    LogLevel level,
    const std::string& batch_id,
    const std::string& message,
    const nlohmann::json& context,
    const std::string& conn_id = {},
    const std::string& schema = {},
    const std::string& table = {}) {
    full_load::log(log_pg, log_mtx, "mariadb_load", level, batch_id, message, context, conn_id, schema, table);
}

using full_load::csv_escape;
using full_load::elapsed_ms;
using full_load::utc_now_date;
using full_load::utc_now_ts;

std::string format_pk_row_sample(
    MYSQL_ROW row,
    const std::vector<std::string>& pk_cols,
    const std::vector<std::size_t>& pk_indices) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i) {
            oss << ", ";
        }
        const std::size_t idx = pk_indices[i];
        oss << pk_cols[i] << '=';
        if (!row[idx]) {
            oss << "NULL";
        } else {
            oss << row[idx];
        }
    }
    return oss.str();
}

void advance_keyset_pk_values(MYSQL_ROW row, const std::vector<std::size_t>& pk_indices, std::vector<std::string>& out) {
    out.clear();
    for (const std::size_t idx : pk_indices) {
        out.push_back(row[idx] ? row[idx] : "");
    }
}

std::string sql_quote(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        out += (c == '\'') ? "''" : std::string(1, c);
    }
    out += "'";
    return out;
}

void apply_catalog_pk_columns(std::vector<MariaDbColumn>& cols, const std::vector<std::string>& pk_cols) {
    if (pk_cols.empty()) {
        return;
    }
    for (auto& col : cols) {
        for (const auto& pk : pk_cols) {
            if (col.name == pk) {
                col.is_pk = true;
                break;
            }
        }
    }
}

struct PkRange {
    bool active{false};
    std::string lower_inclusive;
    std::string upper_exclusive;
    bool numeric{false};
};

bool is_integer_pk_type(const MariaDbColumn& col) {
    std::string lower = col.mysql_type;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (lower.find("decimal") != std::string::npos || lower.find("numeric") != std::string::npos ||
        lower.find("float") != std::string::npos || lower.find("double") != std::string::npos) {
        return false;
    }
    return lower.find("int") != std::string::npos;
}

MariaDbRetryOptions mariadb_retry_options(RuntimeConfig& runtime, const std::string& conn_id) {
    (void)runtime;
    (void)conn_id;
    MariaDbRetryOptions opts;
    opts.max_attempts = pipeline_defaults::kMariadbReconnectMaxAttempts;
    opts.base_ms = std::max(100, pipeline_defaults::kMariadbReconnectBaseMs);
    opts.max_ms = std::max(opts.base_ms, pipeline_defaults::kMariadbReconnectMaxMs);
    return opts;
}

std::optional<PkRange> fetch_integer_pk_range(
    MariaDbConn& conn,
    const CatalogTableRow& target,
    const std::string& pk_col,
    const MariaDbRetryOptions& retry) {
    std::ostringstream sql;
    sql << "SELECT MIN(`" << pk_col << "`), MAX(`" << pk_col << "`) FROM `" << target.source_schema << "`.`"
        << target.source_table << "`";
    MYSQL_RES* res = mariadb_mysql_query_store_retry(conn, sql.str(), retry);
    if (!res) {
        return std::nullopt;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    if (!row || !row[0] || !row[1]) {
        mysql_free_result(res);
        return std::nullopt;
    }
    PkRange range;
    range.active = true;
    range.numeric = true;
    range.lower_inclusive = row[0];
    range.upper_exclusive = row[1];
    mysql_free_result(res);
    return range;
}

// MIN/MAX from MariaDB are inclusive; keyset WHERE uses upper_exclusive as id < N.
PkRange pk_range_for_keyset(const PkRange& bounds) {
    PkRange range = bounds;
    if (!range.active || !range.numeric || range.upper_exclusive.empty()) {
        return range;
    }
    try {
        const long long max_v = std::stoll(range.upper_exclusive);
        range.upper_exclusive = (max_v < LLONG_MAX) ? std::to_string(max_v + 1) : range.upper_exclusive;
    } catch (...) {
    }
    return range;
}

std::string pk_range_clause(
    const std::string& pk_col,
    const PkRange& range,
    bool upper_exclusive = true) {
    if (!range.active) {
        return {};
    }
    std::ostringstream clause;
    if (!range.lower_inclusive.empty()) {
        clause << " AND `" << pk_col << "` >= ";
        clause << (range.numeric ? range.lower_inclusive : sql_quote(range.lower_inclusive));
    }
    if (!range.upper_exclusive.empty()) {
        clause << " AND `" << pk_col << "` "
               << (upper_exclusive ? "< " : "<= ");
        clause << (range.numeric ? range.upper_exclusive : sql_quote(range.upper_exclusive));
    }
    return clause.str();
}

long long copy_rows_keyset(
    MariaDbConn& conn,
    PGconn* pg,
    const CatalogTableRow& target,
    const std::vector<MariaDbColumn>& cols,
    const std::vector<std::string>& pk_cols,
    std::size_t batch_size,
    int source_sleep_ms,
    const std::string& snapshot_id,
    const MariaDbRetryOptions& retry,
    const PkRange& pk_range = {},
    InvalidPkSkipStats* skip_stats = nullptr) {
    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();

    std::vector<std::size_t> pk_indices;
    for (const auto& pk : pk_cols) {
        for (std::size_t i = 0; i < cols.size(); ++i) {
            if (cols[i].name == pk) {
                pk_indices.push_back(i);
                break;
            }
        }
    }

    std::ostringstream select_cols;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i) {
            select_cols << ", ";
        }
        select_cols << "`" << cols[i].name << "`";
    }

    std::ostringstream copy_cols;
    for (const auto& c : cols) {
        copy_cols << pg_ident(c.name) << ", ";
    }
    copy_cols << pg_ident("_dl_load_timestamp") << ", " << pg_ident("_dl_load_date") << ", "
              << pg_ident("_dl_source_system") << ", " << pg_ident("_dl_snapshot_id");

    const std::string fq = pg_ident(target.source_schema) + "." + pg_ident(target.source_table);
    const std::string copy_sql = "COPY " + fq + " (" + copy_cols.str() + ") FROM STDIN WITH (FORMAT csv)";

    long long total_rows = 0;
    std::vector<std::string> last_pk_values;

    while (true) {
        if (g_shutdown.load()) {
            break;
        }
        std::ostringstream query;
        query << "SELECT " << select_cols.str() << " FROM `" << target.source_schema << "`.`" << target.source_table
              << "` WHERE 1=1";
        if (pk_range.active && pk_cols.size() == 1) {
            query << pk_range_clause(pk_cols[0], pk_range, true);
        }
        if (!last_pk_values.empty()) {
            query << " AND (";
            for (std::size_t i = 0; i < pk_cols.size(); ++i) {
                if (i) {
                    query << ", ";
                }
                query << "`" << pk_cols[i] << "`";
            }
            query << ") > (";
            for (std::size_t i = 0; i < last_pk_values.size(); ++i) {
                if (i) {
                    query << ", ";
                }
                query << sql_quote(last_pk_values[i]);
            }
            query << ")";
        }
        query << " ORDER BY ";
        for (std::size_t i = 0; i < pk_cols.size(); ++i) {
            if (i) {
                query << ", ";
            }
            query << "`" << pk_cols[i] << "`";
        }
        query << " LIMIT " << batch_size;

        MYSQL_RES* res = mariadb_mysql_query_store_retry(conn, query.str(), retry);
        if (!res) {
            break;
        }

        std::vector<std::string> batch_lines;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
            unsigned long* lens = mysql_fetch_lengths(res);
            std::ostringstream line;
            bool row_ok = true;
            for (std::size_t i = 0; i < cols.size(); ++i) {
                if (i) {
                    line << ',';
                }
                std::string cell;
                if (!mariadb_format_copy_cell(row[i], lens ? lens[i] : 0, cols[i], cell)) {
                    row_ok = false;
                    break;
                }
                line << cell;
            }
            if (!row_ok) {
                if (skip_stats) {
                    skip_stats->count += 1;
                    if (skip_stats->samples.size() < 5) {
                        skip_stats->samples.push_back(format_pk_row_sample(row, pk_cols, pk_indices));
                    }
                }
                advance_keyset_pk_values(row, pk_indices, last_pk_values);
                continue;
            }
            line << ',' << csv_escape(load_ts) << ',' << csv_escape(load_date) << ",MariaDB,"
                 << csv_escape(snapshot_id);
            batch_lines.push_back(line.str());
            advance_keyset_pk_values(row, pk_indices, last_pk_values);
        }
        mysql_free_result(res);

        if (batch_lines.empty()) {
            break;
        }

        PGresult* copy_res = PQexec(pg, copy_sql.c_str());
        if (!copy_res || PQresultStatus(copy_res) != PGRES_COPY_IN) {
            if (copy_res) {
                PQclear(copy_res);
            }
            throw std::runtime_error(std::string("COPY start failed: ") + PQerrorMessage(pg));
        }
        PQclear(copy_res);

        bool copy_ok = true;
        for (const auto& line : batch_lines) {
            if (PQputCopyData(pg, line.data(), static_cast<int>(line.size())) != 1 ||
                PQputCopyData(pg, "\n", 1) != 1) {
                copy_ok = false;
                break;
            }
        }
        if (!copy_ok) {
            PQputCopyEnd(pg, "abort");
            while (PGresult* r = PQgetResult(pg)) {
                PQclear(r);
            }
            throw std::runtime_error(std::string("PQputCopyData failed: ") + PQerrorMessage(pg));
        }
        if (PQputCopyEnd(pg, nullptr) != 1) {
            PQputCopyEnd(pg, "abort");
            while (PGresult* r = PQgetResult(pg)) {
                PQclear(r);
            }
            throw std::runtime_error(std::string("PQputCopyEnd failed: ") + PQerrorMessage(pg));
        }

        PGresult* end_res = PQgetResult(pg);
        while (end_res) {
            if (PQresultStatus(end_res) != PGRES_COMMAND_OK) {
                const std::string err = PQerrorMessage(pg);
                PQclear(end_res);
                while (PGresult* r = PQgetResult(pg)) {
                    PQclear(r);
                }
                throw std::runtime_error(std::string("COPY failed: ") + err);
            }
            PQclear(end_res);
            end_res = PQgetResult(pg);
        }

        total_rows += static_cast<long long>(batch_lines.size());
        if (batch_lines.size() < batch_size) {
            break;
        }
        if (source_sleep_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(source_sleep_ms));
        }
    }

    if (total_rows == 0 && skip_stats && skip_stats->count > 0) {
        throw std::runtime_error(
            "full load: all scanned rows skipped due to invalid primary key (count=" +
            std::to_string(skip_stats->count) + ")");
    }

    return total_rows;
}

long long copy_rows_parallel(
    const AppConfig& cfg,
    const MariaDbSource& source,
    PGconn* log_pg,
    std::mutex* log_mtx,
    MariaDbConn& conn,
    PGconn* pg,
    const CatalogTableRow& target,
    const std::vector<MariaDbColumn>& cols,
    const std::vector<std::string>& pk_cols,
    std::size_t batch_size,
    int source_sleep_ms,
    int workers,
    const std::string& snapshot_id,
    const MariaDbRetryOptions& retry,
    InvalidPkSkipStats* skip_stats = nullptr) {
    if (workers <= 1) {
        return copy_rows_keyset(
            conn, pg, target, cols, pk_cols, batch_size, source_sleep_ms, snapshot_id, retry, {}, skip_stats);
    }

    if (pk_cols.size() != 1) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            snapshot_id,
            "parallel copy workers disabled: composite primary key",
            {{"workers_requested", workers}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        return copy_rows_keyset(
            conn, pg, target, cols, pk_cols, batch_size, source_sleep_ms, snapshot_id, retry, {}, skip_stats);
    }

    const MariaDbColumn* pk_col_def = nullptr;
    for (const auto& col : cols) {
        if (col.name == pk_cols[0]) {
            pk_col_def = &col;
            break;
        }
    }
    if (!pk_col_def || !is_integer_pk_type(*pk_col_def)) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            snapshot_id,
            "parallel copy workers disabled: non-numeric primary key",
            {{"workers_requested", workers}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        return copy_rows_keyset(
            conn, pg, target, cols, pk_cols, batch_size, source_sleep_ms, snapshot_id, retry, {}, skip_stats);
    }

    const auto bounds = fetch_integer_pk_range(conn, target, pk_cols[0], retry);
    if (!bounds) {
        return 0;
    }

    long long min_v = 0;
    long long max_v = 0;
    try {
        min_v = std::stoll(bounds->lower_inclusive);
        max_v = std::stoll(bounds->upper_exclusive);
    } catch (...) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            snapshot_id,
            "parallel copy workers disabled: PK bounds not integer",
            {{"workers_requested", workers}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        return copy_rows_keyset(
            conn,
            pg,
            target,
            cols,
            pk_cols,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            pk_range_for_keyset(*bounds),
            skip_stats);
    }

    if (min_v >= max_v) {
        return copy_rows_keyset(
            conn,
            pg,
            target,
            cols,
            pk_cols,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            pk_range_for_keyset(*bounds),
            skip_stats);
    }

    const unsigned long long total = static_cast<unsigned long long>(max_v) - static_cast<unsigned long long>(min_v) + 1ULL;
    const long long span = (total + workers - 1) / workers;

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        snapshot_id,
        "parallel copy workers started",
        {{"workers", workers}, {"pk_min", min_v}, {"pk_max", max_v}, {"span", span}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    std::vector<long long> row_counts(static_cast<std::size_t>(workers), 0);
    std::vector<InvalidPkSkipStats> worker_skips(static_cast<std::size_t>(workers));
    std::vector<std::exception_ptr> errors(static_cast<std::size_t>(workers));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(workers));

    for (int w = 0; w < workers; ++w) {
        threads.emplace_back([&, w]() {
            try {
                PkRange slice;
                slice.active = true;
                slice.numeric = true;
                const unsigned long long lo = static_cast<unsigned long long>(min_v) + static_cast<unsigned long long>(w) * static_cast<unsigned long long>(span);
                const unsigned long long hi_ex =
                    (w == workers - 1) ? (max_v < LLONG_MAX ? static_cast<unsigned long long>(max_v) + 1ULL : static_cast<unsigned long long>(max_v)) : static_cast<unsigned long long>(min_v) + static_cast<unsigned long long>(w + 1) * static_cast<unsigned long long>(span);
                slice.lower_inclusive = std::to_string(lo);
                slice.upper_exclusive = std::to_string(hi_ex);

                MariaDbConn worker_db(source);
                PgConn worker_pg(cfg.datalake.conn_string());
                row_counts[static_cast<std::size_t>(w)] = copy_rows_keyset(
                    worker_db,
                    worker_pg.raw,
                    target,
                    cols,
                    pk_cols,
                    batch_size,
                    source_sleep_ms,
                    snapshot_id,
                    retry,
                    slice,
                    &worker_skips[static_cast<std::size_t>(w)]);
            } catch (...) {
                errors[static_cast<std::size_t>(w)] = std::current_exception();
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    for (const auto& err : errors) {
        if (err) {
            std::rethrow_exception(err);
        }
    }

    long long total_rows = 0;
    for (long long n : row_counts) {
        total_rows += n;
    }
    if (skip_stats) {
        for (const auto& worker_skip : worker_skips) {
            skip_stats->merge(worker_skip);
        }
    }
    return total_rows;
}

std::vector<CatalogTableRow> fetch_full_load_targets(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        R"(
        SELECT catalog_id, conn_id, source_schema, source_table, has_pk, COALESCE(pk_columns, '')
        FROM cdc_catalog.catalog
        WHERE db_engine = 'mariadb'
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled', 'full_load_in_progress')
        ORDER BY conn_id, source_schema, source_table
        )");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to fetch full load targets");
    }
    std::vector<CatalogTableRow> out;
    for (int i = 0; i < PQntuples(res); ++i) {
        CatalogTableRow row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.source_schema = PQgetvalue(res, i, 2);
        row.source_table = PQgetvalue(res, i, 3);
        row.has_pk = std::string(PQgetvalue(res, i, 4)) == "t";
        row.pk_columns = PQgetvalue(res, i, 5);
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

void mark_catalog_success(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = false,
            cdc_enabled = true,
            status = 'success',
            last_full_load_at = now(),
            last_error_at = NULL,
            last_error = NULL,
            engine_meta = COALESCE(engine_meta, '{}'::jsonb) - 'full_load_fail_count',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void reactivate_full_load_after_cooldown(PGconn* pg, RuntimeConfig& runtime, const std::string& conn_id) {
    const int max_retries = pipeline_defaults::kFullLoadMaxFailRetries;
    const int cooldown_min = pipeline_defaults::kFullLoadFailedCooldownMinutes;
    const std::string max_str = std::to_string(max_retries);
    const std::string cooldown_str = std::to_string(cooldown_min);
    const char* vals[] = {conn_id.c_str(), max_str.c_str(), cooldown_str.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            engine_meta = COALESCE(engine_meta, '{}'::jsonb) - 'full_load_fail_count',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = 'mariadb'
          AND active = true
          AND status = 'failed'
          AND needs_full_load = false
          AND COALESCE((engine_meta->>'full_load_fail_count')::int, 0) >= $2::int
          AND last_error_at < now() - ($3::int * interval '1 minute')
        )",
        3,
        vals);
}

void mark_catalog_failed(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& error) {
    RuntimeConfig runtime;
    runtime.reload(pg);
    const int max_retries = pipeline_defaults::kFullLoadMaxFailRetries;
    const std::string trunc = error.substr(0, 950);
    const std::string id = std::to_string(catalog_id);
    const std::string max_str = std::to_string(max_retries);
    const char* vals[] = {id.c_str(), trunc.c_str(), max_str.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET engine_meta = jsonb_set(
                COALESCE(engine_meta, '{}'::jsonb),
                '{full_load_fail_count}',
                to_jsonb(COALESCE((engine_meta->>'full_load_fail_count')::int, 0) + 1),
                true),
            status = 'failed',
            needs_full_load = CASE
                WHEN COALESCE((engine_meta->>'full_load_fail_count')::int, 0) + 1 >= $3::int THEN false
                ELSE true
            END,
            last_error_at = now(),
            last_error = CASE
                WHEN COALESCE((engine_meta->>'full_load_fail_count')::int, 0) + 1 >= $3::int
                THEN $2 || ' [full-load paused: max retries reached]'
                ELSE $2
            END,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        3,
        vals);
}

enum class TableLoadOutcome { Success, Skipped, Failed };

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
    pg_exec_params_simple(
        pg,
        "SELECT pg_advisory_lock($1::bigint)",
        1,
        vals);
}

void release_full_load_table_lock(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        "SELECT pg_advisory_unlock($1::bigint)",
        1,
        vals);
}

TableLoadOutcome load_one_table(
    const AppConfig& cfg,
    PGconn* log_pg,
    std::mutex* log_mtx,
    const MariaDbSource& source,
    const CatalogTableRow& target,
    const std::string& batch_id,
    long long& rows_out) {
    const auto start = std::chrono::steady_clock::now();
    rows_out = 0;

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    MariaDbConn mariadb(source);
    RuntimeConfig runtime;
    runtime.reload(app_pg.raw);

    const std::size_t batch_size = runtime.get_size_t(
        "full_load_batch_size",
        pipeline_defaults::kFullLoadBatchSizeDefault,
        "mariadb_load",
        target.conn_id);
    const int source_sleep_ms = pipeline_defaults::kFullLoadSourceSleepMs;
    const int workers = pipeline_defaults::kFullLoadWorkers;

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table full load started",
        {{"batch_size", batch_size}, {"workers", workers}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    mark_catalog_full_load_in_progress(app_pg.raw, target.catalog_id);

    if (!target.has_pk || target.pk_columns.empty()) {
        mark_catalog_skipped(app_pg.raw, target.catalog_id, "no primary key");
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "table skipped: no primary key",
            {},
            target.conn_id,
            target.source_schema,
            target.source_table);
        return TableLoadOutcome::Skipped;
    }

    auto cols = fetch_mariadb_columns(mariadb.handle, target.source_schema, target.source_table);
    const auto pk_cols = split_pk_columns(target.pk_columns);
    apply_catalog_pk_columns(cols, pk_cols);

    const int partition_months =
        pipeline_defaults::kLakePartitionMonthsAhead;

    migrate_lake_table_schema(lake_pg.raw, target.source_schema, target.source_table, cols);
    ensure_lake_table_base(lake_pg.raw, target.source_schema, target.source_table, cols, partition_months);

    acquire_full_load_table_lock(lake_pg.raw, target.catalog_id);
    bool lock_released = false;
    auto release_lock = [&]() {
        if (!lock_released) {
            try {
                release_full_load_table_lock(lake_pg.raw, target.catalog_id);
            } catch (...) {
            }
            lock_released = true;
        }
    };

    try {
    try {
    truncate_lake_table(lake_pg.raw, target.source_schema, target.source_table);
    } catch (const std::exception& ex) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Error,
            batch_id,
            "lake table truncate failed, aborting full load",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        release_lock();
        mark_catalog_failed(app_pg.raw, target.catalog_id, target.conn_id, "truncate failed: " + std::string(ex.what()));
        return TableLoadOutcome::Failed;
    }

    const long long rows_after_truncate =
        lake_table_row_count(lake_pg.raw, target.source_schema, target.source_table);

    log_fl(
        log_pg,
        log_mtx,
        rows_after_truncate == 0 ? LogLevel::Info : LogLevel::Warning,
        batch_id,
        rows_after_truncate == 0 ? "lake table truncated" : "lake table truncate incomplete",
        {{"rows_after_truncate", rows_after_truncate}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    {
        try {
            seed_stream_capture_bookmark_if_needed(
                app_pg.raw, target.conn_id, target.catalog_id, "mariadb", batch_id);
        } catch (const std::exception& ex) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Warning,
                batch_id,
                "stream capture bookmark skipped; continuing full load",
                {{"error", ex.what()}},
                target.conn_id,
                target.source_schema,
                target.source_table);
        }
    }

    const DdlSyncResult ddl = sync_mariadb_ddl_after_truncate(
        lake_pg.raw, mariadb.handle, target.source_schema, target.source_table, cols, runtime, target.conn_id);

    merge_lake_column_nullability(lake_pg.raw, target.source_schema, target.source_table, cols);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "ddl sync completed",
        {{"columns_added", ddl.columns_added},
         {"columns_promoted_to_bytea", ddl.columns_widened},
         {"indexes_created", ddl.indexes_created},
         {"foreign_keys_created", ddl.foreign_keys_created}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    const MariaDbRetryOptions retry = mariadb_retry_options(runtime, target.conn_id);
    InvalidPkSkipStats skip_stats;
    rows_out = copy_rows_parallel(
        cfg,
        source,
        log_pg,
        log_mtx,
        mariadb,
        lake_pg.raw,
        target,
        cols,
        pk_cols,
        batch_size,
        source_sleep_ms,
        workers,
        batch_id,
        retry,
        &skip_stats);

    if (skip_stats.count > 0) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "full load skipped rows with invalid primary key",
            {{"rows_skipped", skip_stats.count}, {"pk_samples", skip_stats.samples}},
            target.conn_id,
            target.source_schema,
            target.source_table);
    }

    mark_catalog_success(app_pg.raw, target.catalog_id);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table full load completed",
        {{"rows_loaded", rows_out},
         {"duration_ms", elapsed_ms(start)},
         {"workers", workers}},
        target.conn_id,
        target.source_schema,
        target.source_table);
    release_lock();
    return TableLoadOutcome::Success;
    } catch (const std::exception& ex) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Error,
            batch_id,
            "table full load failed",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        release_lock();
        throw;
    } catch (...) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Error,
            batch_id,
            "table full load failed",
            {{"error", "unknown exception"}},
            target.conn_id,
            target.source_schema,
            target.source_table);
        release_lock();
        throw;
    }
}

}  // namespace

FullLoadRunStats run_mariadb_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter) {
    const auto run_start = std::chrono::steady_clock::now();
    FullLoadRunStats stats;

    PgConn app_pg(cfg.datasync.conn_string());
    RuntimeConfig runtime;
    runtime.reload(app_pg.raw);

    log_fl(
        log_pg,
        nullptr,
        LogLevel::Info,
        batch_id,
        "full load started",
        {{"batch_size",
          runtime.get_size_t(
              "full_load_batch_size", pipeline_defaults::kFullLoadBatchSizeDefault, "mariadb_load")},
         {"workers", pipeline_defaults::kFullLoadWorkers},
         {"parallel_tables", kFullLoadParallelTables}});

    const auto targets_all = fetch_full_load_targets(app_pg.raw);
    std::vector<CatalogTableRow> targets;
    targets.reserve(targets_all.size());
    for (const auto& row : targets_all) {
        if (conn_id_filter && !conn_id_filter->empty() && row.conn_id != *conn_id_filter) {
            continue;
        }
        targets.push_back(row);
    }
    log_fl(log_pg, nullptr, LogLevel::Info, batch_id, "full load targets loaded", {{"table_count", targets.size()}});

    std::set<std::string> conn_ids;
    for (const auto& t : targets) {
        conn_ids.insert(t.conn_id);
    }
    for (const auto& cid : conn_ids) {
        const int stale_minutes =
            pipeline_defaults::kFullLoadStaleInProgressMinutes;
        clear_stale_full_load_in_progress(app_pg.raw, cid, "mariadb", stale_minutes);
    }

    if (targets.empty()) {
        log_fl(log_pg, nullptr, LogLevel::Info, batch_id, "full load completed", {{"tables_processed", 0}});
        return stats;
    }

    std::map<std::string, std::vector<CatalogTableRow>> by_conn;
    for (const auto& t : targets) {
        by_conn[t.conn_id].push_back(t);
    }

    std::mutex log_mtx;
    std::mutex stats_mtx;

    for (auto& [conn_id, conn_targets] : by_conn) {
        const MariaDbSource* src = find_mariadb_source(cfg, conn_id);
        if (!src) {
            for (const auto& t : conn_targets) {
                stats.tables_processed += 1;
                stats.tables_failed += 1;
                log_fl(
                    log_pg,
                    nullptr,
                    LogLevel::Error,
                    batch_id,
                    "unknown conn_id in runtime config scope",
                    {{"conn_id", conn_id}},
                    conn_id,
                    t.source_schema,
                    t.source_table);
            }
            continue;
        }

        MariaDbConn order_db(*src);
        runtime.reload(app_pg.raw);
        reactivate_full_load_after_cooldown(app_pg.raw, runtime, conn_id);

        const MariaDbPreflightResult preflight = check_mariadb_load_ready(order_db.handle);
        for (const auto& w : preflight.warnings) {
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Warning,
                batch_id,
                "mariadb cdc preflight warning",
                {{"detail", w}},
                conn_id);
        }
        if (!preflight.ok) {
            nlohmann::json err_ctx = nlohmann::json::array();
            for (const auto& e : preflight.errors) {
                err_ctx.push_back(e);
            }
            stats.tables_processed += static_cast<int>(conn_targets.size());
            stats.tables_failed += static_cast<int>(conn_targets.size());
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Error,
                batch_id,
                "full load conn skipped: mariadb load preflight failed",
                {{"errors", err_ctx}},
                conn_id);
            continue;
        }

        const int parallel_tables = kFullLoadParallelTables;

        try {
            if (capture_binlog_position_t0_if_absent(app_pg.raw, order_db.handle, conn_id)) {
                log_fl(
                    log_pg,
                    nullptr,
                    LogLevel::Info,
                    batch_id,
                    "binlog T0 captured at full load start",
                    {},
                    conn_id);
            } else {
                log_fl(
                    log_pg,
                    nullptr,
                    LogLevel::Info,
                    batch_id,
                    "binlog T0 already set — skip",
                    {},
                    conn_id);
            }
        } catch (const std::exception& ex) {
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Warning,
                batch_id,
                "binlog T0 capture failed",
                {{"error", ex.what()}},
                conn_id);
        }

        std::map<std::string, std::vector<CatalogTableRow>> by_schema;
        for (const auto& t : conn_targets) {
            by_schema[t.source_schema].push_back(t);
        }

        for (auto& [schema, rows] : by_schema) {
            std::vector<std::string> names;
            std::map<std::string, CatalogTableRow> row_by_table;
            names.reserve(rows.size());
            for (const auto& row : rows) {
                names.push_back(row.source_table);
                row_by_table[row.source_table] = row;
            }

            log_fl(
                log_pg,
                nullptr,
                LogLevel::Info,
                batch_id,
                "full load schema batch",
                {{"schema", schema}, {"table_count", names.size()}, {"parallel_tables", parallel_tables}},
                conn_id);

            for (std::size_t batch_start = 0; batch_start < names.size();
                 batch_start += static_cast<std::size_t>(parallel_tables)) {
                const std::size_t batch_end =
                    std::min(batch_start + static_cast<std::size_t>(parallel_tables), names.size());
                std::vector<std::thread> pool;
                pool.reserve(batch_end - batch_start);

                for (std::size_t i = batch_start; i < batch_end; ++i) {
                    const CatalogTableRow target = row_by_table[names[i]];
                    pool.emplace_back([&, target]() {
                        long long rows = 0;
                        try {
                            const auto outcome = load_one_table(cfg, log_pg, &log_mtx, *src, target, batch_id, rows);
                            std::lock_guard<std::mutex> lock(stats_mtx);
                            stats.tables_processed += 1;
                            if (outcome == TableLoadOutcome::Success) {
                                stats.tables_success += 1;
                                stats.total_rows += rows;
                            } else if (outcome == TableLoadOutcome::Skipped) {
                                stats.tables_skipped += 1;
                            } else {
                                stats.tables_failed += 1;
                            }
                        } catch (const std::exception& ex) {
                            try {
                                PgConn fail_pg(cfg.datasync.conn_string());
                                mark_catalog_failed(fail_pg.raw, target.catalog_id, target.conn_id, ex.what());
                            } catch (...) {
                            }
                            std::lock_guard<std::mutex> lock(stats_mtx);
                            stats.tables_processed += 1;
                            stats.tables_failed += 1;
                        }
                    });
                }

                for (auto& thread : pool) {
                    thread.join();
                }
            }
        }

    }

    log_fl(
        log_pg,
        nullptr,
        LogLevel::Info,
        batch_id,
        stats.tables_failed == 0 ? "full load completed" : "full load completed with errors",
        {{"tables_processed", stats.tables_processed},
         {"tables_success", stats.tables_success},
         {"tables_skipped", stats.tables_skipped},
         {"tables_failed", stats.tables_failed},
         {"total_rows", stats.total_rows},
         {"duration_ms", elapsed_ms(run_start)}});

    return stats;
}
