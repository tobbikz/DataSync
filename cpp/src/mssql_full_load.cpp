#include "mssql_full_load.hpp"

#include "capture_common.hpp"
#include "mariadb_datetime.hpp"
#include "mssql_conn.hpp"
#include "mssql_kafka_capture.hpp"
#include "mssql_lake.hpp"
#include "mssql_ddl_sync.hpp"
#include "mssql_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <algorithm>
#include <cctype>
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
    const std::optional<std::string>& service_tier,
    const std::optional<std::string>& conn_id_filter) {
    (void)cfg;
    (void)service_tier;
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
    if (!log_pg) {
        return;
    }
    LogEvent ev;
    ev.level = level;
    ev.component = "mssql_load";
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

long long elapsed_ms(const std::chrono::steady_clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
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

std::string mssql_select_expr(const MssqlColumn& col) {
    const std::string b = brack(col.name);
    const std::string t = to_lower(col.mssql_type);
    if (t == "datetime" || t == "datetime2" || t == "smalldatetime") {
        return "CONVERT(VARCHAR(33), " + b + ", 127)";
    }
    if (t == "date") {
        return "CONVERT(VARCHAR(10), " + b + ", 23)";
    }
    if (t == "time") {
        return "CONVERT(VARCHAR(16), " + b + ", 114)";
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
    if (pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP" || pg_type == "DATE") {
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

struct MssqlPkRange {
    bool active{false};
    long long lower{0};
    long long upper_exclusive{0};
};

MssqlPkRange mssql_pk_range_for_keyset(long long min_v, long long max_v) {
    MssqlPkRange range;
    range.active = true;
    range.lower = min_v;
    range.upper_exclusive = max_v + 1;
    return range;
}

std::optional<MssqlPkRange> fetch_mssql_numeric_pk_bounds(
    DBPROCESS* db,
    const std::string& schema,
    const std::string& table,
    const std::string& pk_col) {
    std::ostringstream sql;
    sql << "SELECT MIN(" << brack(pk_col) << "), MAX(" << brack(pk_col) << ") FROM " << brack(schema) << "."
        << brack(table);
    try {
        run_dbsql(db, sql.str());
    } catch (...) {
        return std::nullopt;
    }
    if (dbresults(db) == FAIL) {
        return std::nullopt;
    }
    const int rc = dbnextrow(db);
    if (rc != REG_ROW) {
        while (dbresults(db) != NO_MORE_RESULTS) {
        }
        return std::nullopt;
    }
    char min_buf[64]{};
    char max_buf[64]{};
    if (dbdata(db, 1)) {
        mssql_cell_to_char(db, 1, min_buf, static_cast<DBINT>(sizeof(min_buf) - 1));
    }
    if (dbdata(db, 2)) {
        mssql_cell_to_char(db, 2, max_buf, static_cast<DBINT>(sizeof(max_buf) - 1));
    }
    while (dbresults(db) != NO_MORE_RESULTS) {
    }
    try {
        return mssql_pk_range_for_keyset(std::stoll(min_buf), std::stoll(max_buf));
    } catch (...) {
        return std::nullopt;
    }
}

std::vector<std::vector<std::string>> group_mssql_tables_by_fk_level(
    MssqlConn& mssql,
    const std::string& schema,
    const std::vector<std::string>& tables) {
    std::set<std::string> table_set(tables.begin(), tables.end());
    if (table_set.size() <= 1) {
        return {tables};
    }

    const std::string sql =
        "SELECT OBJECT_NAME(f.parent_object_id), OBJECT_NAME(f.referenced_object_id) "
        "FROM sys.foreign_keys f "
        "JOIN sys.tables pt ON f.parent_object_id = pt.object_id "
        "JOIN sys.schemas ps ON pt.schema_id = ps.schema_id "
        "WHERE ps.name = '" + schema + "'";
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

long long copy_rows_offset(
    DBPROCESS* db,
    PGconn* pg,
    const MssqlCatalogTableRow& target,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& pk_cols,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::size_t batch_size,
    int source_sleep_ms,
    const std::string& snapshot_id,
    const MssqlPkRange& pk_range = {}) {
    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();

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

    std::vector<std::string> order_pk = pk_cols;
    if (order_pk.empty() && !cols.empty()) {
        order_pk.push_back(cols.front().name);
    }
    std::ostringstream order_by;
    for (std::size_t i = 0; i < order_pk.size(); ++i) {
        if (i) {
            order_by << ", ";
        }
        order_by << brack(order_pk[i]);
    }

    long long total_rows = 0;
    long long offset = 0;
    const bool filter_pk_range = pk_range.active && order_pk.size() == 1;

    while (true) {
        std::ostringstream query;
        query << "SELECT " << select_cols.str() << " FROM " << brack(target.source_schema) << "."
              << brack(target.source_table) << " WHERE 1=1";
        if (filter_pk_range) {
            query << " AND " << brack(order_pk[0]) << " >= " << pk_range.lower << " AND " << brack(order_pk[0])
                  << " < " << pk_range.upper_exclusive;
        }
        query << " ORDER BY " << order_by.str() << " OFFSET " << offset << " ROWS FETCH NEXT " << batch_size
              << " ROWS ONLY";

        run_dbsql(db, query.str());
        if (dbresults(db) == FAIL) {
            throw std::runtime_error("MSSQL SELECT dbresults failed: " + format_dberror(db));
        }

        std::vector<std::string> batch_lines;
        while (true) {
            const int rc = dbnextrow(db);
            if (rc == NO_MORE_ROWS) {
                break;
            }
            if (rc == FAIL) {
                throw std::runtime_error("MSSQL SELECT dbnextrow failed: " + format_dberror(db));
            }
            std::ostringstream line;
            for (int col = 1; col <= static_cast<int>(cols.size()); ++col) {
                if (col > 1) {
                    line << ',';
                }
                line << cell_as_csv(db, col, cols[static_cast<std::size_t>(col - 1)].pg_type);
            }
            line << ',' << csv_escape(load_ts) << ',' << csv_escape(load_date) << ",MSSQL," << csv_escape(snapshot_id);
            batch_lines.push_back(line.str());
        }
        while (dbresults(db) != NO_MORE_RESULTS) {
            // drain
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
        offset += static_cast<long long>(batch_lines.size());

        if (batch_lines.size() < batch_size) {
            break;
        }
        if (source_sleep_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(source_sleep_ms));
        }
    }

    return total_rows;
}

std::vector<MssqlCatalogTableRow> fetch_full_load_targets(PGconn* pg, const std::optional<std::string>& service_tier) {
    const char* sql_tier = R"(
        SELECT catalog_id, conn_id, source_database, source_schema, source_table, has_pk, COALESCE(pk_columns, '')
        FROM cdc_catalog.catalog
        WHERE db_engine = 'mssql'
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
          AND service_tier::text = lower($1)
        ORDER BY conn_id, source_database, source_schema, source_table
    )";
    const char* sql_all = R"(
        SELECT catalog_id, conn_id, source_database, source_schema, source_table, has_pk, COALESCE(pk_columns, '')
        FROM cdc_catalog.catalog
        WHERE db_engine = 'mssql'
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        ORDER BY conn_id, source_database, source_schema, source_table
    )";

    PGresult* res = nullptr;
    if (service_tier && !service_tier->empty()) {
        const char* vals[] = {service_tier->c_str()};
        res = PQexecParams(pg, sql_tier, 1, nullptr, vals, nullptr, nullptr, 0);
    } else {
        res = PQexec(pg, sql_all);
    }
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
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_failed(PGconn* pg, long long catalog_id, const std::string& error) {
    const std::string trunc = error.substr(0, 1000);
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str(), trunc.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'failed',
            needs_full_load = true,
            last_error_at = now(),
            last_error = $2,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        vals);
}

enum class TableLoadOutcome { Success, Skipped, Failed };

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
        "full_load_batch_size", 5000, "mssql_load", target.conn_id);
    const int source_sleep_ms = runtime.get_int("full_load_source_sleep_ms", 0, "mssql_load", target.conn_id);
    const int partition_months =
        runtime.get_int("lake_partition_months_ahead", 3, "mssql_load", target.conn_id);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table full load started",
        {{"batch_size", batch_size}, {"source_database", target.source_database}},
        target.conn_id,
        target.source_schema,
        target.source_table);

#ifdef HAVE_FREETDS
    try {
        if (seed_mssql_cdc_lsn_for_table_if_absent(
                log_pg,
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
                "mssql LSN T0 captured at full load start",
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
            "mssql LSN T0 capture failed",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_schema,
            target.source_table);
    }
#endif

    mark_catalog_full_load_in_progress(app_pg.raw, target.catalog_id);

    {
        RuntimeConfig bookmark_runtime;
        bookmark_runtime.reload(app_pg.raw);
        seed_stream_capture_bookmark_if_needed(
            app_pg.raw, bookmark_runtime, target.conn_id, target.catalog_id, "mssql", batch_id);
    }

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

    const auto cols = fetch_mssql_columns(mssql.handle, target.source_schema, target.source_table);
    const auto source_pk_cols = split_pk_columns(target.pk_columns);
    const std::string pg_schema = mssql_pg_schema_name(target.source_database, target.source_schema);
    const std::string pg_table = mssql_pg_table_name(target.source_table);

    ensure_mssql_lake_table_base(lake_pg.raw, pg_schema, pg_table, cols, source_pk_cols, partition_months);
    truncate_lake_table(lake_pg.raw, pg_schema, pg_table);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "lake table truncated",
        {},
        target.conn_id,
        target.source_schema,
        target.source_table);

    DdlSyncResult ddl{};
    if (runtime.get_bool("ddl_sync_columns", true, "mssql_load", target.conn_id)) {
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

    const int workers = std::max(1, runtime.get_int("full_load_workers", 1, "mssql_load", target.conn_id));
    if (workers > 1 && source_pk_cols.size() == 1) {
        if (const auto bounds = fetch_mssql_numeric_pk_bounds(
                mssql.handle, target.source_schema, target.source_table, source_pk_cols[0])) {
            const long long span = std::max(1LL, bounds->upper_exclusive - bounds->lower);
            const long long chunk = (span + workers - 1) / workers;
            std::vector<std::thread> pool;
            std::vector<long long> part_rows(static_cast<std::size_t>(workers), 0);
            std::exception_ptr worker_error;
            std::mutex worker_err_mtx;
            pool.reserve(static_cast<std::size_t>(workers));
            for (int w = 0; w < workers; ++w) {
                pool.emplace_back([&, w]() {
                    try {
                        MssqlConn worker_mssql(source);
                        worker_mssql.use_database(target.source_database);
                        MssqlPkRange slice;
                        slice.active = true;
                        slice.lower = bounds->lower + static_cast<long long>(w) * chunk;
                        slice.upper_exclusive = std::min(bounds->upper_exclusive, slice.lower + chunk);
                        if (slice.lower >= slice.upper_exclusive) {
                            return;
                        }
                        PgConn worker_lake(cfg.datalake.conn_string());
                        part_rows[static_cast<std::size_t>(w)] = copy_rows_offset(
                            worker_mssql.handle,
                            worker_lake.raw,
                            target,
                            cols,
                            source_pk_cols,
                            pg_schema,
                            pg_table,
                            batch_size,
                            source_sleep_ms,
                            batch_id,
                            slice);
                    } catch (...) {
                        std::lock_guard<std::mutex> lock(worker_err_mtx);
                        if (!worker_error) {
                            worker_error = std::current_exception();
                        }
                    }
                });
            }
            for (auto& th : pool) {
                th.join();
            }
            if (worker_error) {
                std::rethrow_exception(worker_error);
            }
            for (long long part : part_rows) {
                rows_out += part;
            }
        } else {
            rows_out = copy_rows_offset(
                mssql.handle,
                lake_pg.raw,
                target,
                cols,
                source_pk_cols,
                pg_schema,
                pg_table,
                batch_size,
                source_sleep_ms,
                batch_id);
        }
    } else {
        rows_out = copy_rows_offset(
            mssql.handle,
            lake_pg.raw,
            target,
            cols,
            source_pk_cols,
            pg_schema,
            pg_table,
            batch_size,
            source_sleep_ms,
            batch_id);
    }

    mark_catalog_success(app_pg.raw, target.catalog_id);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table COPY done",
        {{"rows_loaded", rows_out}, {"duration_ms", elapsed_ms(start)}},
        target.conn_id,
        target.source_schema,
        target.source_table);
    return TableLoadOutcome::Success;
}

}  // namespace

FullLoadRunStats run_mssql_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& service_tier,
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
        {{"service_tier", service_tier.value_or("all")},
         {"batch_size", runtime.get_size_t("full_load_batch_size", 5000, "mssql_load")},
         {"parallel_tables", runtime.get_int("full_load_parallel_tables", 1, "mssql_load")}});

    const auto targets_all = fetch_full_load_targets(app_pg.raw, service_tier);
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
        clear_stale_full_load_in_progress(app_pg.raw, cid, "mssql");
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

        const int parallel_tables =
            std::max(1, runtime.get_int("full_load_parallel_tables", 1, "mssql_load", conn_id));

        MssqlConn order_mssql(*src);
        std::map<std::string, std::vector<MssqlCatalogTableRow>> by_schema;
        for (const auto& t : conn_targets) {
            by_schema[t.source_schema].push_back(t);
        }

        int fk_levels = 0;
        for (auto& [schema, rows] : by_schema) {
            if (!rows.empty()) {
                order_mssql.use_database(rows.front().source_database);
            }
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
                {{"schema", schema}, {"levels", levels.size()}, {"parallel_tables", parallel_tables}},
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
                                mark_catalog_failed(mark_pg.raw, target.catalog_id, ex.what());
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
