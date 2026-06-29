#include "mssql_full_load.hpp"

#include "capture_common.hpp"
#include "full_load_common.hpp"
#include "lake_apply_index.hpp"
#include "mariadb_datetime.hpp"
#include "mariadb_schema.hpp"
#include "mssql_conn.hpp"
#include "mssql_kafka_capture.hpp"
#include "mssql_lake.hpp"
#include "mssql_preflight.hpp"
#include "mssql_ddl_sync.hpp"
#include "mssql_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#include <algorithm>
#include <cctype>
#include <climits>
#include <exception>
#include <iomanip>
#include <map>
#include <mutex>
#include <set>
#include <optional>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef HAVE_FREETDS

FullLoadRunStats run_mssql_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter) {
    (void)cfg;
    (void)conn_id_filter;
    if (log_pg) {
        log_write(
            log_pg,
            LogEvent{
                .level = LogLevel::Warning,
                .component = "mssql_load",
                .message = "full load skipped: rebuild with FreeTDS (pacman -S freetds && cmake --build cpp/build)",
                .batch_id = batch_id,
                .context = {{"hint", "install freetds"}}});
    }
    return {};
}

#else

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

struct MssqlCatalogTableRow {
    long long catalog_id{0};
    std::string conn_id;
    std::string source_database;
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
    full_load::log(log_pg, log_mtx, "mssql_load", level, batch_id, message, context, conn_id, schema, table);
}

using full_load::csv_escape;
using full_load::elapsed_ms;
using full_load::utc_now_date;
using full_load::utc_now_ts;

std::string brack(const std::string& name) {
    std::string escaped;
    escaped.reserve(name.size() + 2);
    for (char c : name) {
        if (c == ']') {
            escaped += "]]";
        } else {
            escaped += c;
        }
    }
    return "[" + escaped + "]";
}

std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

bool is_mssql_time_type(const std::string& mssql_type) {
    const std::string t = to_lower(mssql_type);
    if (t == "time" || t.rfind("time(", 0) == 0) {
        return true;
    }
    return t.find("time") != std::string::npos && t.find("datetime") == std::string::npos &&
           t.find("timestamp") == std::string::npos;
}

std::string mssql_select_expr(const MssqlColumn& col) {
    const std::string b = brack(col.name);
    const std::string t = to_lower(col.mssql_type);
    if (t == "datetime" || t == "datetime2" || t == "smalldatetime") {
        return "CONVERT(VARCHAR(33), " + b + ", 127)";
    }
    if (t == "date") {
        return "CONVERT(VARCHAR(10), " + b + ", 23)";
    }
    if (is_mssql_time_type(col.mssql_type)) {
        return "CONVERT(VARCHAR(30), " + b + ", 108)";
    }
    return b;
}

std::string cell_as_csv(DBPROCESS* db, int col, const std::string& pg_type) {
    if (!dbdata(db, col)) {
        return "";
    }
    const int col_type = dbcoltype(db, col);
    if (col_type == SYBBINARY || col_type == SYBVARBINARY || col_type == SYBIMAGE) {
        const char* data = reinterpret_cast<const char*>(dbdata(db, col));
        const DBINT len = dbdatlen(db, col);
        if (!data || len <= 0) {
            return "";
        }
        std::ostringstream hex;
        hex << "\\x";
        hex << std::hex << std::setfill('0');
        for (DBINT i = 0; i < len; ++i) {
            hex << std::setw(2) << (static_cast<unsigned>(static_cast<unsigned char>(data[i])) & 0xFF);
        }
        return csv_escape(hex.str());
    }
    if (col_type == SYBBIT || col_type == SYBBITN) {
        const auto v = *(const DBBIT*)dbdata(db, col);
        return csv_escape(v ? "true" : "false");
    }
    char buf[4096];
    mssql_cell_to_char(db, col, buf, static_cast<DBINT>(sizeof(buf) - 1));
    buf[sizeof(buf) - 1] = '\0';
    std::string text = sanitize_mssql_text_for_pg(trim_mssql_text(buf));
    if (pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP" || pg_type == "DATE" || is_time_pg_type(pg_type)) {
        text = normalize_text_for_pg(text, pg_type);
    }
    return csv_escape(text);
}

std::string format_dberror(DBPROCESS* db) {
    char err_buf[1024];
    err_buf[0] = '\0';
    if (db) {
        dbstrcpy(db, 1, static_cast<int>(sizeof(err_buf) - 1), err_buf);
    }
    return err_buf[0] ? std::string(err_buf) : "unknown DB-Library error";
}

std::string cell_as_text(DBPROCESS* db, int col) {
    if (!dbdata(db, col)) {
        return "";
    }
    char buf[4096];
    mssql_cell_to_char(db, col, buf, static_cast<DBINT>(sizeof(buf) - 1));
    buf[sizeof(buf) - 1] = '\0';
    return trim_mssql_text(buf);
}

std::string sql_quote(const std::string& value) {
    std::string out = "N'";
    for (char c : value) {
        out += (c == '\'') ? "''" : std::string(1, c);
    }
    out += "'";
    return out;
}

void apply_catalog_pk_columns(std::vector<MssqlColumn>& cols, const std::vector<std::string>& pk_cols) {
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

bool is_integer_pk_type(const MssqlColumn& col) {
    const std::string lower = to_lower(col.mssql_type);
    if (lower.find("decimal") != std::string::npos || lower.find("numeric") != std::string::npos ||
        lower.find("float") != std::string::npos || lower.find("double") != std::string::npos ||
        lower.find("real") != std::string::npos || lower.find("money") != std::string::npos ||
        lower.find("smallmoney") != std::string::npos) {
        return false;
    }
    return lower.find("int") != std::string::npos;
}

using full_load::acquire_full_load_table_lock;
using full_load::lake_table_row_count;
using full_load::release_full_load_table_lock;

MssqlRetryOptions mssql_retry_options(RuntimeConfig& runtime, const std::string& conn_id) {
    (void)runtime;
    (void)conn_id;
    MssqlRetryOptions opts;
    opts.max_attempts = pipeline_defaults::kMssqlReconnectMaxAttempts;
    opts.base_ms = std::max(100, pipeline_defaults::kMssqlReconnectBaseMs);
    opts.max_ms = std::max(opts.base_ms, pipeline_defaults::kMssqlReconnectMaxMs);
    return opts;
}

struct PkRange {
    bool active{false};
    std::string lower_inclusive;
    std::string upper_exclusive;
    bool numeric{false};
};

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

std::string pk_sql_literal(const MssqlColumn* col_def, const std::string& value) {
    if (col_def && is_integer_pk_type(*col_def)) {
        return value;
    }
    return sql_quote(value);
}

std::string pk_range_clause(
    const std::string& pk_col,
    const PkRange& range,
    const MssqlColumn* col_def,
    bool upper_exclusive = true) {
    if (!range.active) {
        return {};
    }
    std::ostringstream clause;
    if (!range.lower_inclusive.empty()) {
        clause << " AND " << brack(pk_col) << " >= "
               << (range.numeric ? range.lower_inclusive : pk_sql_literal(col_def, range.lower_inclusive));
    }
    if (!range.upper_exclusive.empty()) {
        clause << " AND " << brack(pk_col) << " " << (upper_exclusive ? "< " : "<= ")
               << (range.numeric ? range.upper_exclusive : pk_sql_literal(col_def, range.upper_exclusive));
    }
    return clause.str();
}

std::string mssql_keyset_clause(
    const std::vector<std::string>& pk_cols,
    const std::vector<std::string>& last_pk_values,
    const std::vector<MssqlColumn>& cols) {
    if (last_pk_values.empty()) {
        return {};
    }
    auto col_def = [&](const std::string& name) -> const MssqlColumn* {
        for (const auto& col : cols) {
            if (col.name == name) {
                return &col;
            }
        }
        return nullptr;
    };

    std::ostringstream clause;
    clause << " AND (";
    for (std::size_t depth = 0; depth < pk_cols.size(); ++depth) {
        if (depth) {
            clause << " OR ";
        }
        clause << "(";
        for (std::size_t eq = 0; eq < depth; ++eq) {
            clause << brack(pk_cols[eq]) << " = "
                   << pk_sql_literal(col_def(pk_cols[eq]), last_pk_values[eq]) << " AND ";
        }
        clause << brack(pk_cols[depth]) << " > "
               << pk_sql_literal(col_def(pk_cols[depth]), last_pk_values[depth]);
        clause << ")";
    }
    clause << ")";
    return clause.str();
}

std::optional<PkRange> fetch_integer_pk_range(
    MssqlConn& mssql,
    const std::string& schema,
    const std::string& table,
    const std::string& pk_col,
    const MssqlRetryOptions& retry) {
    std::ostringstream sql;
    sql << "SELECT MIN(" << brack(pk_col) << "), MAX(" << brack(pk_col) << ") FROM " << brack(schema) << "."
        << brack(table);
    try {
        const auto result = mssql.query_retry(sql.str(), retry);
        if (result.rows.empty() || result.rows[0].size() < 2) {
            return std::nullopt;
        }
        const std::string min_v = result.rows[0][0].text;
        const std::string max_v = result.rows[0][1].text;
        if (min_v.empty() || max_v.empty()) {
            return std::nullopt;
        }
        PkRange range;
        range.active = true;
        range.numeric = true;
        range.lower_inclusive = min_v;
        range.upper_exclusive = max_v;
        return range;
    } catch (...) {
        return std::nullopt;
    }
}

std::string format_pk_row_sample(
    DBPROCESS* db,
    const std::vector<std::string>& pk_cols,
    const std::vector<int>& pk_col_nums) {
    std::ostringstream oss;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i) {
            oss << ", ";
        }
        oss << pk_cols[i] << '=';
        const std::string v = cell_as_text(db, pk_col_nums[i]);
        oss << (v.empty() ? "NULL" : v);
    }
    return oss.str();
}

void advance_keyset_pk_values(
    DBPROCESS* db,
    const std::vector<int>& pk_col_nums,
    std::vector<std::string>& out) {
    out.clear();
    for (const int col : pk_col_nums) {
        out.push_back(cell_as_text(db, col));
    }
}

long long copy_rows_keyset(
    MssqlConn& mssql,
    PGconn* pg,
    PGconn* log_pg,
    std::mutex* log_mtx,
    const MssqlCatalogTableRow& target,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& pk_cols,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::size_t batch_size,
    int source_sleep_ms,
    const std::string& snapshot_id,
    const MssqlRetryOptions& retry,
    const PkRange& pk_range = {},
    InvalidPkSkipStats* skip_stats = nullptr) {
    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();

    std::vector<int> pk_col_nums;
    for (const auto& pk : pk_cols) {
        for (std::size_t i = 0; i < cols.size(); ++i) {
            if (cols[i].name == pk) {
                pk_col_nums.push_back(static_cast<int>(i) + 1);
                break;
            }
        }
    }

    std::ostringstream select_cols;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i) {
            select_cols << ", ";
        }
        select_cols << mssql_select_expr(cols[i]);
    }

    std::ostringstream copy_cols;
    for (const auto& c : cols) {
        copy_cols << pg_ident(c.name) << ", ";
    }
    copy_cols << pg_ident("_dl_load_timestamp") << ", " << pg_ident("_dl_load_date") << ", "
              << pg_ident("_dl_source_system") << ", " << pg_ident("_dl_snapshot_id");

    const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
    const std::string copy_sql = "COPY " + fq + " (" + copy_cols.str() + ") FROM STDIN WITH (FORMAT csv)";

    const MssqlColumn* pk_col_def = nullptr;
    if (pk_cols.size() == 1) {
        for (const auto& col : cols) {
            if (col.name == pk_cols[0]) {
                pk_col_def = &col;
                break;
            }
        }
    }

    long long total_rows = 0;
    std::vector<std::string> last_pk_values;
    int batch_num = 0;
    const int progress_interval = pipeline_defaults::kMssqlFullLoadCopyProgressInterval;
    bool copy_started_logged = false;

    while (true) {
        if (g_shutdown.load()) {
            break;
        }

        if (!copy_started_logged) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Info,
                snapshot_id,
                "mssql copy started",
                {{"batch_size", batch_size}, {"pk_columns", nlohmann::json(pk_cols)}},
                target.conn_id,
                target.source_schema,
                target.source_table);
            copy_started_logged = true;
        }

        std::ostringstream query;
        query << "SELECT TOP (" << batch_size << ") " << select_cols.str() << " FROM " << brack(target.source_schema)
              << "." << brack(target.source_table) << " WHERE 1=1";
        if (pk_range.active && pk_cols.size() == 1) {
            query << pk_range_clause(pk_cols[0], pk_range, pk_col_def, true);
        }
        query << mssql_keyset_clause(pk_cols, last_pk_values, cols);
        query << " ORDER BY ";
        for (std::size_t i = 0; i < pk_cols.size(); ++i) {
            if (i) {
                query << ", ";
            }
            query << brack(pk_cols[i]);
        }

        mssql_run_dbsql_retry(mssql, query.str(), retry);
        if (dbresults(mssql.handle) == FAIL) {
            throw std::runtime_error("MSSQL SELECT dbresults failed: " + format_dberror(mssql.handle));
        }

        std::vector<std::string> batch_lines;
        while (true) {
            const int rc = dbnextrow(mssql.handle);
            if (rc == NO_MORE_ROWS) {
                break;
            }
            if (rc == FAIL) {
                throw std::runtime_error("MSSQL SELECT dbnextrow failed: " + format_dberror(mssql.handle));
            }

            bool row_ok = true;
            for (const int pk_col : pk_col_nums) {
                if (cell_as_text(mssql.handle, pk_col).empty()) {
                    row_ok = false;
                    break;
                }
            }

            if (!row_ok) {
                if (skip_stats) {
                    skip_stats->count += 1;
                    if (skip_stats->samples.size() < 5) {
                        skip_stats->samples.push_back(format_pk_row_sample(mssql.handle, pk_cols, pk_col_nums));
                    }
                }
                advance_keyset_pk_values(mssql.handle, pk_col_nums, last_pk_values);
                continue;
            }

            std::ostringstream line;
            for (int col = 1; col <= static_cast<int>(cols.size()); ++col) {
                if (col > 1) {
                    line << ',';
                }
                line << cell_as_csv(mssql.handle, col, cols[static_cast<std::size_t>(col - 1)].pg_type);
            }
            line << ',' << csv_escape(load_ts) << ',' << csv_escape(load_date) << ",MSSQL," << csv_escape(snapshot_id);
            batch_lines.push_back(line.str());
            advance_keyset_pk_values(mssql.handle, pk_col_nums, last_pk_values);
        }
        while (dbresults(mssql.handle) != NO_MORE_RESULTS) {
        }

        if (batch_lines.empty()) {
            break;
        }

        PGresult* copy_res = PQexec(pg, copy_sql.c_str());
        if (!copy_res || PQresultStatus(copy_res) != PGRES_COPY_IN) {
            const std::string err = PQerrorMessage(pg);
            if (copy_res) {
                PQclear(copy_res);
            }
            throw std::runtime_error("COPY start failed: " + err);
        }
        PQclear(copy_res);

        for (const auto& line : batch_lines) {
            if (PQputCopyData(pg, line.c_str(), static_cast<int>(line.size())) != 1) {
                throw std::runtime_error(std::string("COPY data failed: ") + PQerrorMessage(pg));
            }
            if (PQputCopyData(pg, "\n", 1) != 1) {
                throw std::runtime_error(std::string("COPY newline failed: ") + PQerrorMessage(pg));
            }
        }
        if (PQputCopyEnd(pg, nullptr) != 1) {
            throw std::runtime_error(std::string("COPY end failed: ") + PQerrorMessage(pg));
        }
        PGresult* end_res = PQgetResult(pg);
        while (end_res) {
            if (PQresultStatus(end_res) != PGRES_COMMAND_OK) {
                const std::string err = PQerrorMessage(pg);
                PQclear(end_res);
                throw std::runtime_error("COPY commit failed: " + err);
            }
            PQclear(end_res);
            end_res = PQgetResult(pg);
        }

        total_rows += static_cast<long long>(batch_lines.size());
        batch_num += 1;
        if (batch_num == 1 || batch_num % progress_interval == 0) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Info,
                snapshot_id,
                "mssql copy batch completed",
                {{"batch", batch_num},
                 {"batch_rows", batch_lines.size()},
                 {"rows_loaded", total_rows}},
                target.conn_id,
                target.source_schema,
                target.source_table);
        }

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
    const MssqlSource& source,
    PGconn* log_pg,
    std::mutex* log_mtx,
    MssqlConn& mssql,
    PGconn* pg,
    const MssqlCatalogTableRow& target,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& pk_cols,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::size_t batch_size,
    int source_sleep_ms,
    int workers,
    const std::string& snapshot_id,
    const MssqlRetryOptions& retry,
    InvalidPkSkipStats* skip_stats = nullptr) {
    if (workers <= 1) {
        return copy_rows_keyset(
            mssql,
            pg,
            log_pg,
            log_mtx,
            target,
            cols,
            pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            {},
            skip_stats);
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
            mssql,
            pg,
            log_pg,
            log_mtx,
            target,
            cols,
            pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            {},
            skip_stats);
    }

    const MssqlColumn* pk_col_def = nullptr;
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
            mssql,
            pg,
            log_pg,
            log_mtx,
            target,
            cols,
            pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            {},
            skip_stats);
    }

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        snapshot_id,
        "mssql pk bounds query started",
        {{"pk_column", pk_cols[0]}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    const auto bounds = fetch_integer_pk_range(mssql, target.source_schema, target.source_table, pk_cols[0], retry);
    if (!bounds) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            snapshot_id,
            "mssql pk bounds empty; table has no rows or non-integer PK bounds",
            {},
            target.conn_id,
            target.source_schema,
            target.source_table);
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
            mssql,
            pg,
            log_pg,
            log_mtx,
            target,
            cols,
            pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            pk_range_for_keyset(*bounds),
            skip_stats);
    }

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        snapshot_id,
        "mssql pk bounds resolved",
        {{"pk_min", min_v}, {"pk_max", max_v}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    if (min_v >= max_v) {
        return copy_rows_keyset(
            mssql,
            pg,
            log_pg,
            log_mtx,
            target,
            cols,
            pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            snapshot_id,
            retry,
            pk_range_for_keyset(*bounds),
            skip_stats);
    }

    const unsigned long long total =
        static_cast<unsigned long long>(max_v) - static_cast<unsigned long long>(min_v) + 1ULL;
    const long long span = static_cast<long long>((total + static_cast<unsigned long long>(workers) - 1ULL) /
                                                  static_cast<unsigned long long>(workers));

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
                const unsigned long long lo =
                    static_cast<unsigned long long>(min_v) +
                    static_cast<unsigned long long>(w) * static_cast<unsigned long long>(span);
                const unsigned long long hi_ex =
                    (w == workers - 1)
                        ? (max_v < LLONG_MAX ? static_cast<unsigned long long>(max_v) + 1ULL
                                             : static_cast<unsigned long long>(max_v))
                        : static_cast<unsigned long long>(min_v) +
                              static_cast<unsigned long long>(w + 1) * static_cast<unsigned long long>(span);
                slice.lower_inclusive = std::to_string(lo);
                slice.upper_exclusive = std::to_string(hi_ex);

                MssqlConn worker_mssql(source);
                worker_mssql.use_database(target.source_database);
                PgConn worker_lake(cfg.datalake.conn_string());
                row_counts[static_cast<std::size_t>(w)] = copy_rows_keyset(
                    worker_mssql,
                    worker_lake.raw,
                    log_pg,
                    log_mtx,
                    target,
                    cols,
                    pk_cols,
                    pg_schema,
                    pg_table,
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

std::vector<std::vector<std::string>> group_mssql_tables_by_fk_level(
    MssqlConn& mssql,
    const std::string& schema,
    const std::vector<std::string>& tables) {
    std::set<std::string> table_set(tables.begin(), tables.end());
    if (table_set.size() <= 1) {
        return {tables};
    }

    std::string escaped_schema;
    escaped_schema.reserve(schema.size());
    for (char c : schema) {
        if (c == '\'') escaped_schema += "''";
        else escaped_schema += c;
    }
    const std::string sql =
        "SELECT OBJECT_NAME(f.parent_object_id), OBJECT_NAME(f.referenced_object_id) "
        "FROM sys.foreign_keys f "
        "JOIN sys.tables pt ON f.parent_object_id = pt.object_id "
        "JOIN sys.schemas ps ON pt.schema_id = ps.schema_id "
        "WHERE ps.name = '" + escaped_schema + "'";
    const auto result = mssql.query(sql);

    std::unordered_map<std::string, int> in_degree;
    std::unordered_map<std::string, std::vector<std::string>> dependents;
    for (const auto& t : tables) {
        in_degree[t] = 0;
    }

    for (const auto& row : result.rows) {
        if (row.size() < 2) {
            continue;
        }
        const std::string child = trim_mssql_text(row[0].text);
        const std::string parent = trim_mssql_text(row[1].text);
        if (table_set.find(child) == table_set.end() || table_set.find(parent) == table_set.end()) {
            continue;
        }
        if (child == parent) {
            continue;
        }
        in_degree[child] += 1;
        dependents[parent].push_back(child);
    }

    std::queue<std::string> ready;
    for (const auto& t : tables) {
        if (in_degree[t] == 0) {
            ready.push(t);
        }
    }

    std::vector<std::vector<std::string>> levels;
    std::size_t placed = 0;
    while (!ready.empty()) {
        const std::size_t wave = ready.size();
        std::vector<std::string> level;
        level.reserve(wave);
        for (std::size_t i = 0; i < wave; ++i) {
            const std::string current = ready.front();
            ready.pop();
            level.push_back(current);
            placed += 1;
            for (const auto& dep : dependents[current]) {
                in_degree[dep] -= 1;
                if (in_degree[dep] == 0) {
                    ready.push(dep);
                }
            }
        }
        levels.push_back(std::move(level));
    }

    if (placed != tables.size()) {
        return {tables};
    }
    return levels;
}

std::vector<MssqlCatalogTableRow> fetch_full_load_targets(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        R"(
        SELECT catalog_id, conn_id, source_database, source_schema, source_table, has_pk, COALESCE(pk_columns, '')
        FROM cdc_catalog.catalog
        WHERE db_engine = 'mssql'
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled', 'full_load_in_progress')
        ORDER BY conn_id, source_database, source_schema, source_table
        )");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to fetch MSSQL full load targets");
    }

    std::vector<MssqlCatalogTableRow> out;
    for (int i = 0; i < PQntuples(res); ++i) {
        MssqlCatalogTableRow row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.source_database = PQgetvalue(res, i, 2);
        row.source_schema = PQgetvalue(res, i, 3);
        row.source_table = PQgetvalue(res, i, 4);
        row.has_pk = std::string(PQgetvalue(res, i, 5)) == "t";
        row.pk_columns = PQgetvalue(res, i, 6);
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

void mark_catalog_success(PGconn* pg, long long catalog_id) {
    mark_catalog_full_load_data_ready(pg, catalog_id);
}

void reactivate_full_load_after_cooldown(PGconn* pg, RuntimeConfig& runtime, const std::string& conn_id) {
    (void)runtime;
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
          AND db_engine = 'mssql'
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
    (void)conn_id;
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

std::vector<MariaDbColumn> mssql_cols_as_mariadb(const std::vector<MssqlColumn>& cols) {
    std::vector<MariaDbColumn> out;
    out.reserve(cols.size());
    for (const auto& c : cols) {
        MariaDbColumn mc;
        mc.name = c.name;
        mc.mysql_type = c.mssql_type;
        mc.is_pk = c.is_pk;
        mc.pg_type = c.pg_type;
        out.push_back(std::move(mc));
    }
    return out;
}

TableLoadOutcome load_one_table(
    const AppConfig& cfg,
    PGconn* log_pg,
    std::mutex* log_mtx,
    const MssqlSource& source,
    const MssqlCatalogTableRow& target,
    const std::string& batch_id,
    long long& rows_out) {
    const auto start = std::chrono::steady_clock::now();
    rows_out = 0;

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    MssqlConn mssql(source);
    mssql.use_database(target.source_database);

    RuntimeConfig runtime;
    runtime.reload(app_pg.raw);

    const std::size_t batch_size = runtime.get_size_t(
        "full_load_batch_size",
        pipeline_defaults::kFullLoadBatchSizeDefault,
        "mariadb_load",
        target.conn_id);
    const int source_sleep_ms = pipeline_defaults::kFullLoadSourceSleepMs;
    const int partition_months = pipeline_defaults::kLakePartitionMonthsAhead;
    const int workers = pipeline_defaults::kMssqlFullLoadWorkers;

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table full load started",
        {{"batch_size", batch_size},
         {"workers", workers},
         {"source_database", target.source_database}},
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

    auto cols = fetch_mssql_columns(mssql.handle, target.source_schema, target.source_table);
    const auto source_pk_cols = split_pk_columns(target.pk_columns);
    apply_catalog_pk_columns(cols, source_pk_cols);
    const std::string pg_schema = mssql_pg_schema_name(target.source_database, target.source_schema);
    const std::string pg_table = mssql_pg_table_name(target.source_table);

    auto mariadb_shape_cols = mssql_cols_as_mariadb(cols);
    migrate_lake_table_schema(lake_pg.raw, pg_schema, pg_table, mariadb_shape_cols);
    ensure_mssql_lake_table_base(lake_pg.raw, pg_schema, pg_table, cols, source_pk_cols, partition_months);
    ensure_mirror_apply_pk_index(lake_pg.raw, pg_schema, pg_table, source_pk_cols);

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
    truncate_lake_table(lake_pg.raw, pg_schema, pg_table);
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

    const long long rows_after_truncate = lake_table_row_count(lake_pg.raw, pg_schema, pg_table);

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
                app_pg.raw, target.conn_id, target.catalog_id, "mssql", batch_id);
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

    DdlSyncResult ddl{};
    if (pipeline_defaults::kDdlSyncColumns) {
        ddl = sync_mssql_ddl_after_truncate(
            lake_pg.raw,
            mssql,
            target.source_database,
            target.source_schema,
            target.source_table,
            cols,
            runtime,
            target.conn_id);
    }

    merge_lake_column_nullability(lake_pg.raw, pg_schema, pg_table, mariadb_shape_cols);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "ddl sync completed",
        {{"columns_added", ddl.columns_added},
         {"columns_widened", ddl.columns_widened},
         {"indexes_created", ddl.indexes_created},
         {"foreign_keys_created", ddl.foreign_keys_created}},
        target.conn_id,
        target.source_schema,
        target.source_table);

    const MssqlRetryOptions retry = mssql_retry_options(runtime, target.conn_id);
    InvalidPkSkipStats skip_stats;
    rows_out = copy_rows_parallel(
        cfg,
        source,
        log_pg,
        log_mtx,
        mssql,
        lake_pg.raw,
        target,
        cols,
        source_pk_cols,
        pg_schema,
        pg_table,
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

    try {
        if (seed_mssql_cdc_lsn_t0_for_table(
                app_pg.raw,
                mssql,
                target.conn_id,
                target.source_database,
                target.source_schema,
                target.source_table)) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Info,
                batch_id,
                "mssql LSN T0 reset after full load",
                {},
                target.conn_id,
                target.source_schema,
                target.source_table);
        }
    } catch (const std::exception& ex) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "mssql LSN T0 reset after full load failed",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_schema,
            target.source_table);
    }

    if (!onboard_table_after_full_load(
            app_pg.raw,
            target.conn_id,
            "mssql",
            target.catalog_id,
            batch_id,
            target.source_schema,
            target.source_table)) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "table full load copied; cdc enable deferred (daemon will retry onboard)",
            {},
            target.conn_id,
            target.source_schema,
            target.source_table);
    }

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

FullLoadRunStats run_mssql_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter) {
    const auto run_start = std::chrono::steady_clock::now();
    FullLoadRunStats stats;

    if (cfg.mssql_sources.empty()) {
        return stats;
    }

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
         {"workers", pipeline_defaults::kMssqlFullLoadWorkers},
         {"parallel_tables", pipeline_defaults::kMssqlFullLoadParallelTables}});

    if (conn_id_filter && !conn_id_filter->empty()) {
        reset_full_load_in_progress_for_conn(app_pg.raw, *conn_id_filter, "mssql");
        clear_stale_full_load_in_progress(
            app_pg.raw,
            *conn_id_filter,
            "mssql",
            pipeline_defaults::kMssqlFullLoadStaleInProgressMinutes);
    }

    const auto targets_all = fetch_full_load_targets(app_pg.raw);
    std::vector<MssqlCatalogTableRow> targets;
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
        clear_stale_full_load_in_progress(
            app_pg.raw, cid, "mssql", pipeline_defaults::kMssqlFullLoadStaleInProgressMinutes);
    }

    if (targets.empty()) {
        log_fl(
            log_pg,
            nullptr,
            LogLevel::Info,
            batch_id,
            "full load completed",
            {{"tables_processed", 0}, {"duration_ms", elapsed_ms(run_start)}});
        return stats;
    }

    std::map<std::string, std::vector<MssqlCatalogTableRow>> by_conn;
    for (const auto& t : targets) {
        by_conn[t.conn_id].push_back(t);
    }

    std::mutex log_mtx;
    std::mutex stats_mtx;

    for (auto& [conn_id, conn_targets] : by_conn) {
        reactivate_full_load_after_cooldown(app_pg.raw, runtime, conn_id);

        const MssqlSource* src = find_mssql_source(cfg, conn_id);
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

        const int parallel_tables = pipeline_defaults::kMssqlFullLoadParallelTables;

        MssqlConn order_mssql(*src);
        std::set<std::string> load_databases;
        for (const auto& t : conn_targets) {
            load_databases.insert(t.source_database);
        }

        MssqlPreflightResult preflight;
        for (const auto& database : load_databases) {
            merge_mssql_preflight(preflight, check_mssql_load_ready(order_mssql, database));
            const MssqlPreflightResult cdc_preflight = check_mssql_cdc_ready(order_mssql, database);
            preflight.warnings.insert(
                preflight.warnings.end(), cdc_preflight.warnings.begin(), cdc_preflight.warnings.end());
            if (!cdc_preflight.ok) {
                merge_mssql_preflight(preflight, cdc_preflight);
            }
        }
        const MssqlPreflightResult catalog_preflight =
            check_mssql_catalog_capture_instances(app_pg.raw, conn_id);
        preflight.warnings.insert(
            preflight.warnings.end(), catalog_preflight.warnings.begin(), catalog_preflight.warnings.end());
        if (!catalog_preflight.ok) {
            merge_mssql_preflight(preflight, catalog_preflight);
        }
        for (const auto& w : preflight.warnings) {
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Warning,
                batch_id,
                "mssql cdc preflight warning",
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
                "full load conn skipped: mssql load preflight failed",
                {{"errors", err_ctx}},
                conn_id);
            continue;
        }

        try {
            const int seeded = seed_mssql_cdc_lsn_for_conn(cfg, log_pg, conn_id, batch_id);
            if (seeded > 0) {
                log_fl(
                    log_pg,
                    nullptr,
                    LogLevel::Info,
                    batch_id,
                    "mssql LSN T0 captured at full load start",
                    {{"tables_seeded", seeded}},
                    conn_id);
            } else {
                log_fl(
                    log_pg,
                    nullptr,
                    LogLevel::Info,
                    batch_id,
                    "mssql LSN T0 already set — skip",
                    {},
                    conn_id);
            }
        } catch (const std::exception& ex) {
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Warning,
                batch_id,
                "mssql LSN T0 capture failed",
                {{"error", ex.what()}},
                conn_id);
        }

        std::map<std::string, std::vector<MssqlCatalogTableRow>> by_db_schema;
        for (const auto& t : conn_targets) {
            by_db_schema[t.source_database + '\x1f' + t.source_schema].push_back(t);
        }

        int fk_levels = 0;
        for (auto& [db_schema_key, rows] : by_db_schema) {
            (void)db_schema_key;
            if (rows.empty()) {
                continue;
            }
            const std::string& source_database = rows.front().source_database;
            const std::string& schema = rows.front().source_schema;
            order_mssql.use_database(source_database);
            std::vector<std::string> names;
            std::map<std::string, MssqlCatalogTableRow> row_by_table;
            names.reserve(rows.size());
            for (const auto& row : rows) {
                names.push_back(row.source_table);
                row_by_table[row.source_table] = row;
            }

            const auto levels = group_mssql_tables_by_fk_level(order_mssql, schema, names);
            fk_levels += static_cast<int>(levels.size());

            log_fl(
                log_pg,
                nullptr,
                LogLevel::Info,
                batch_id,
                "fk load level resolved",
                {{"schema", schema},
                 {"source_database", source_database},
                 {"levels", levels.size()},
                 {"parallel_tables", parallel_tables}},
                conn_id);

            for (const auto& level : levels) {
                for (std::size_t batch_start = 0; batch_start < level.size();
                     batch_start += static_cast<std::size_t>(parallel_tables)) {
                    const std::size_t batch_end = std::min(
                        batch_start + static_cast<std::size_t>(parallel_tables), level.size());
                    std::vector<std::thread> threads;
                    threads.reserve(batch_end - batch_start);

                    for (std::size_t i = batch_start; i < batch_end; ++i) {
                        const MssqlCatalogTableRow target = row_by_table[level[i]];
                        threads.emplace_back([&, target]() {
                            long long rows_loaded = 0;
                            try {
                                const auto outcome =
                                    load_one_table(cfg, log_pg, &log_mtx, *src, target, batch_id, rows_loaded);
                                std::lock_guard<std::mutex> lock(stats_mtx);
                                stats.tables_processed += 1;
                                if (outcome == TableLoadOutcome::Success) {
                                    stats.tables_success += 1;
                                    stats.total_rows += rows_loaded;
                                } else if (outcome == TableLoadOutcome::Skipped) {
                                    stats.tables_skipped += 1;
                                } else {
                                    stats.tables_failed += 1;
                                }
                            } catch (const std::exception& ex) {
                                PgConn mark_pg(cfg.datasync.conn_string());
                                mark_catalog_failed(mark_pg.raw, target.catalog_id, target.conn_id, ex.what());
                                log_fl(
                                    log_pg,
                                    &log_mtx,
                                    LogLevel::Error,
                                    batch_id,
                                    "table full load failed",
                                    {{"error", ex.what()}},
                                    target.conn_id,
                                    target.source_schema,
                                    target.source_table);
                                std::lock_guard<std::mutex> lock(stats_mtx);
                                stats.tables_processed += 1;
                                stats.tables_failed += 1;
                            }
                        });
                    }
                    for (auto& th : threads) {
                        th.join();
                    }
                }
            }
        }

    }

    log_fl(
        log_pg,
        nullptr,
        stats.tables_failed == 0 ? LogLevel::Info : LogLevel::Warning,
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

#endif
