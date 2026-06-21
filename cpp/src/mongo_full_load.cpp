#include "mongo_full_load.hpp"

#include "capture_common.hpp"
#include "full_load_common.hpp"
#include "mongo_conn.hpp"
#include "mongo_preflight.hpp"
#include "mongo_kafka_capture.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#include <chrono>
#include <exception>
#include <iomanip>
#include <map>
#include <set>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#ifndef HAVE_MONGOC

FullLoadRunStats run_mongo_full_load(
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
                .component = "mongo_load",
                .message = "full load skipped: rebuild with libmongoc (docker build via ./install.sh)",
                .batch_id = batch_id,
                .context = {{"hint", "install libmongoc"}}});
    }
    return {};
}

#else

#include <bson/bson.h>
#include <mongoc/mongoc.h>

namespace {

struct MongoCatalogTableRow {
    long long catalog_id{0};
    std::string conn_id;
    std::string source_database;
    std::string source_schema;
    std::string source_table;
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
    full_load::log(log_pg, log_mtx, "mongo_load", level, batch_id, message, context, conn_id, schema, table);
}

using full_load::csv_escape;
using full_load::elapsed_ms;
using full_load::utc_now_date;
using full_load::utc_now_ts;

nlohmann::json bson_to_json(const bson_t* doc) {
    char* json_str = bson_as_relaxed_extended_json(doc, nullptr);
    if (!json_str) {
        return nlohmann::json::object();
    }
    nlohmann::json out = nlohmann::json::parse(json_str);
    bson_free(json_str);
    return out;
}

std::string json_cell_csv(const nlohmann::json& v) {
    if (v.is_null()) {
        return "";
    }
    if (v.is_boolean()) {
        return csv_escape(v.get<bool>() ? "true" : "false");
    }
    if (v.is_number_integer()) {
        return csv_escape(std::to_string(v.get<long long>()));
    }
    if (v.is_number_float()) {
        return csv_escape(std::to_string(v.get<double>()));
    }
    if (v.is_string()) {
        return csv_escape(v.get<std::string>());
    }
    return csv_escape(v.dump());
}

std::vector<MongoCatalogTableRow> fetch_full_load_targets(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        R"(
        SELECT catalog_id, conn_id, source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE db_engine = 'mongodb'
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        ORDER BY conn_id, source_database, source_table
        )");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to fetch MongoDB full load targets");
    }

    std::vector<MongoCatalogTableRow> out;
    for (int i = 0; i < PQntuples(res); ++i) {
        MongoCatalogTableRow row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.source_database = PQgetvalue(res, i, 2);
        row.source_schema = PQgetvalue(res, i, 3);
        row.source_table = PQgetvalue(res, i, 4);
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

void mark_catalog_success(PGconn* pg, long long catalog_id) {
    mark_catalog_full_load_data_ready(pg, catalog_id);
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

long long copy_collection_batches(
    mongoc_collection_t* coll,
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::vector<std::string>& all_cols,
    std::map<std::string, std::string>& pg_cols,
    std::size_t batch_size,
    int source_sleep_ms,
    const std::string& snapshot_id) {
    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();

    auto rebuild_copy_sql = [&]() {
        std::ostringstream copy_cols;
        for (const auto& c : all_cols) {
            copy_cols << pg_ident(c) << ", ";
        }
        copy_cols << pg_ident("_dl_load_timestamp") << ", " << pg_ident("_dl_load_date") << ", "
                  << pg_ident("_dl_source_system") << ", " << pg_ident("_dl_snapshot_id");
        const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
        return std::string("COPY ") + fq + " (" + copy_cols.str() + ") FROM STDIN WITH (FORMAT csv)";
    };

    std::string copy_sql = rebuild_copy_sql();

    long long total_rows = 0;
    bson_value_t* last_id_value = nullptr;

    auto cleanup_last_id = [&]() {
        if (last_id_value) {
            bson_value_destroy(last_id_value);
            delete last_id_value;
            last_id_value = nullptr;
        }
    };

    while (true) {
        if (g_shutdown.load()) {
            break;
        }
        bson_t query = BSON_INITIALIZER;
        if (last_id_value) {
            bson_t id_wrap;
            bson_append_document_begin(&query, "_id", 3, &id_wrap);
            bson_append_value(&id_wrap, "$gt", 3, last_id_value);
            bson_append_document_end(&query, &id_wrap);
        }

        bson_t opts = BSON_INITIALIZER;
        bson_t sort_doc;
        BSON_APPEND_DOCUMENT_BEGIN(&opts, "sort", &sort_doc);
        BSON_APPEND_INT32(&sort_doc, "_id", 1);
        bson_append_document_end(&opts, &sort_doc);
        BSON_APPEND_INT32(&opts, "limit", static_cast<int>(batch_size));

        mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
        bson_destroy(&query);
        bson_destroy(&opts);

        std::vector<std::string> batch_lines;
        std::vector<std::map<std::string, nlohmann::json>> batch_flats;
        const bson_t* doc = nullptr;
        try {
            while (mongoc_cursor_next(cursor, &doc)) {
                bson_iter_t iter;
                if (bson_iter_init_find(&iter, doc, "_id")) {
                    const bson_value_t* iter_val = bson_iter_value(&iter);
                    if (iter_val) {
                        if (last_id_value) {
                            bson_value_destroy(last_id_value);
                            delete last_id_value;
                        }
                        last_id_value = new bson_value_t();
                        bson_value_copy(iter_val, last_id_value);
                    }
                }

                const auto j = bson_to_json(doc);
                batch_flats.push_back(flatten_mongo_document(j));
            }
        } catch (...) {
            mongoc_cursor_destroy(cursor);
            throw;
        }
        mongoc_cursor_destroy(cursor);

        if (batch_flats.empty()) {
            break;
        }

        const auto batch_schema = infer_schema_from_flat_rows(batch_flats, batch_flats.size());
        bool schema_changed = false;
        for (const auto& [name, pg_type] : batch_schema) {
            if (name == "mongo_id") {
                continue;
            }
            const auto it = pg_cols.find(name);
            if (it == pg_cols.end()) {
                pg_cols[name] = pg_type;
                schema_changed = true;
            } else {
                std::set<std::string> type_set;
                type_set.insert(it->second);
                type_set.insert(pg_type);
                const std::string merged = resolve_pg_type_from_type_set(type_set);
                if (merged != it->second) {
                    it->second = merged;
                    schema_changed = true;
                }
            }
        }
        if (schema_changed) {
            sync_missing_mongo_columns(pg, pg_schema, pg_table, pg_cols);
            sync_mongo_column_types(pg, pg_schema, pg_table, pg_cols);
            all_cols = {"mongo_id"};
            for (const auto& [name, _] : pg_cols) {
                if (name != "mongo_id") {
                    all_cols.push_back(name);
                }
            }
            copy_sql = rebuild_copy_sql();
        }

        for (const auto& flat : batch_flats) {
            std::ostringstream line;
            for (std::size_t i = 0; i < all_cols.size(); ++i) {
                if (i) {
                    line << ',';
                }
                const auto it = flat.find(all_cols[i]);
                if (it != flat.end()) {
                    line << json_cell_csv(it->second);
                }
            }
            line << ',' << csv_escape(load_ts) << ',' << csv_escape(load_date) << ",MongoDB," << csv_escape(snapshot_id);
            batch_lines.push_back(line.str());
        }

        PGresult* copy_res = PQexec(pg, copy_sql.c_str());
        if (!copy_res || PQresultStatus(copy_res) != PGRES_COPY_IN) {
            const std::string err = PQerrorMessage(pg);
            if (copy_res) {
                PQclear(copy_res);
            }
            cleanup_last_id();
            throw std::runtime_error("COPY start failed: " + err);
        }
        PQclear(copy_res);

        for (const auto& line : batch_lines) {
            if (PQputCopyData(pg, line.c_str(), static_cast<int>(line.size())) != 1) {
                cleanup_last_id();
                throw std::runtime_error(std::string("COPY data failed: ") + PQerrorMessage(pg));
            }
            if (PQputCopyData(pg, "\n", 1) != 1) {
                cleanup_last_id();
                throw std::runtime_error(std::string("COPY newline failed: ") + PQerrorMessage(pg));
            }
        }
        if (PQputCopyEnd(pg, nullptr) != 1) {
            cleanup_last_id();
            throw std::runtime_error(std::string("COPY end failed: ") + PQerrorMessage(pg));
        }
        PGresult* end_res = PQgetResult(pg);
        while (end_res) {
            if (PQresultStatus(end_res) != PGRES_COMMAND_OK) {
                const std::string err = PQerrorMessage(pg);
                PQclear(end_res);
                cleanup_last_id();
                throw std::runtime_error("COPY commit failed: " + err);
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

    cleanup_last_id();
    return total_rows;
}

bool load_one_collection(
    const AppConfig& cfg,
    PGconn* log_pg,
    std::mutex* log_mtx,
    const MongoSource& source,
    const MongoCatalogTableRow& target,
    const std::string& batch_id,
    long long& rows_out) {
    const auto start = std::chrono::steady_clock::now();
    rows_out = 0;

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    MongoConn mongo(source);

    RuntimeConfig runtime;
    runtime.reload(app_pg.raw);

    const std::size_t batch_size = runtime.get_size_t(
        "full_load_batch_size",
        pipeline_defaults::kFullLoadBatchSizeDefault,
        "mariadb_load",
        target.conn_id);
    const int source_sleep_ms = pipeline_defaults::kFullLoadSourceSleepMs;
    const int partition_months = pipeline_defaults::kLakePartitionMonthsAhead;
    const bool ddl_sync = pipeline_defaults::kDdlSyncColumns;

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table full load started",
        {{"batch_size", batch_size}, {"source_database", target.source_database}},
        target.conn_id,
        target.source_database,
        target.source_table);

#ifdef HAVE_MONGOC
    try {
        if (seed_mongo_cdc_resume_for_collection_if_absent(
                log_pg, mongo, target.conn_id, target.source_database, target.source_table)) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Info,
                batch_id,
                "mongo resume T0 captured at full load start",
                {},
                target.conn_id,
                target.source_database,
                target.source_table);
        }
    } catch (const std::exception& ex) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "mongo resume T0 capture failed",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_database,
            target.source_table);
    }
#endif

    mark_catalog_full_load_in_progress(app_pg.raw, target.catalog_id);

    {
        seed_stream_capture_bookmark_if_needed(
            app_pg.raw, target.conn_id, target.catalog_id, "mongodb", batch_id);
    }

    mongoc_collection_t* coll = mongo.collection(target.source_database, target.source_table);
    bool coll_destroyed = false;
    auto destroy_coll = [&]() {
        if (!coll_destroyed) {
            mongoc_collection_destroy(coll);
            coll_destroyed = true;
        }
    };

    try {
    const auto sample = sample_flattened_mongo_docs(coll, 1000);
    auto pg_cols = infer_schema_from_flat_rows(sample);

    const std::string pg_schema = mongo_pg_schema_name(target.source_database);
    const std::string pg_table = mongo_pg_table_name(target.source_table);

    ensure_mongo_lake_table_base(lake_pg.raw, pg_schema, pg_table, pg_cols, partition_months);
    truncate_lake_table(lake_pg.raw, pg_schema, pg_table);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "lake table truncated",
        {},
        target.conn_id,
        target.source_database,
        target.source_table);

    int columns_added = 0;
    int columns_widened = 0;
    if (ddl_sync) {
        columns_added = sync_missing_mongo_columns(lake_pg.raw, pg_schema, pg_table, pg_cols);
        columns_widened = sync_mongo_column_types(lake_pg.raw, pg_schema, pg_table, pg_cols);
    }

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "ddl sync completed",
        {{"columns_added", columns_added}, {"columns_widened", columns_widened}},
        target.conn_id,
        target.source_database,
        target.source_table);

    std::vector<std::string> all_cols = {"mongo_id"};
    for (const auto& [name, _] : pg_cols) {
        if (name != "mongo_id") {
            all_cols.push_back(name);
        }
    }

    rows_out = copy_collection_batches(
        coll,
        lake_pg.raw,
        pg_schema,
        pg_table,
        all_cols,
        pg_cols,
        batch_size,
        source_sleep_ms,
        batch_id);

    destroy_coll();

    mark_catalog_success(app_pg.raw, target.catalog_id);

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        "table COPY done",
        {{"rows_loaded", rows_out}, {"duration_ms", elapsed_ms(start)}},
        target.conn_id,
        target.source_database,
        target.source_table);
    return true;
    } catch (...) {
        destroy_coll();
        throw;
    }
}

}  // namespace

FullLoadRunStats run_mongo_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter) {
    const auto run_start = std::chrono::steady_clock::now();
    FullLoadRunStats stats;

    if (cfg.mongo_sources.empty()) {
        return stats;
    }

    mongoc_init();

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
         {"parallel_tables", kFullLoadParallelTables}});

    const auto targets_all = fetch_full_load_targets(app_pg.raw);
    std::vector<MongoCatalogTableRow> targets;
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
        clear_stale_full_load_in_progress(app_pg.raw, cid, "mongodb", 30);
    }

    if (targets.empty()) {
        log_fl(
            log_pg,
            nullptr,
            LogLevel::Info,
            batch_id,
            "full load completed",
            {{"tables_processed", 0}, {"duration_ms", elapsed_ms(run_start)}});
        mongoc_cleanup();
        return stats;
    }

    std::map<std::string, std::vector<MongoCatalogTableRow>> by_conn;
    for (const auto& t : targets) {
        by_conn[t.conn_id].push_back(t);
    }

    std::mutex log_mtx;
    std::mutex stats_mtx;

    for (auto& [conn_id, conn_targets] : by_conn) {
        const MongoSource* src = find_mongo_source(cfg, conn_id);
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
                    t.source_database,
                    t.source_table);
            }
            continue;
        }

        MongoConn preflight_db(*src);
        const auto preflight = check_mongo_cdc_ready(preflight_db, *src);
        for (const auto& w : preflight.warnings) {
            log_fl(
                log_pg,
                nullptr,
                LogLevel::Warning,
                batch_id,
                "mongo CDC preflight warning",
                {{"warning", w}},
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
                "full load conn skipped: mongo CDC preflight failed",
                {{"errors", err_ctx}},
                conn_id);
            continue;
        }

        const int parallel_tables = kFullLoadParallelTables;

        std::size_t idx = 0;
        while (idx < conn_targets.size()) {
            const std::size_t end = std::min(conn_targets.size(), idx + static_cast<std::size_t>(parallel_tables));
            std::vector<std::thread> threads;
            threads.reserve(end - idx);

            for (std::size_t i = idx; i < end; ++i) {
                threads.emplace_back([&, i]() {
                    long long rows = 0;
                    try {
                        const bool ok =
                            load_one_collection(cfg, log_pg, &log_mtx, *src, conn_targets[i], batch_id, rows);
                        std::lock_guard<std::mutex> lock(stats_mtx);
                        stats.tables_processed += 1;
                        if (ok) {
                            stats.tables_success += 1;
                            stats.total_rows += rows;
                        } else {
                            stats.tables_failed += 1;
                        }
                    } catch (const std::exception& ex) {
                        PgConn mark_pg(cfg.datasync.conn_string());
                        mark_catalog_failed(mark_pg.raw, conn_targets[i].catalog_id, ex.what());
                        log_fl(
                            log_pg,
                            &log_mtx,
                            LogLevel::Error,
                            batch_id,
                            "table full load failed",
                            {{"error", ex.what()}},
                            conn_targets[i].conn_id,
                            conn_targets[i].source_database,
                            conn_targets[i].source_table);
                        std::lock_guard<std::mutex> lock(stats_mtx);
                        stats.tables_processed += 1;
                        stats.tables_failed += 1;
                    }
                });
            }
            for (auto& th : threads) {
                th.join();
            }
            idx = end;
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
         {"tables_failed", stats.tables_failed},
         {"total_rows", stats.total_rows},
         {"duration_ms", elapsed_ms(run_start)}});

    mongoc_cleanup();
    return stats;
}

#endif
