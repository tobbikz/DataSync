#include "mongo_full_load.hpp"

#include "capture_common.hpp"
#include "full_load_checkpoint.hpp"
#include "full_load_common.hpp"
#include "lake_apply_index.hpp"
#include "mongo_conn.hpp"
#include "mongo_preflight.hpp"
#include "mongo_kafka_capture.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "pipeline_defaults.hpp"

#include <chrono>
#include <cctype>
#include <cstring>
#include <exception>
#include <iomanip>
#include <map>
#include <set>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
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
using full_load::acquire_full_load_table_lock;
using full_load::release_full_load_table_lock;

/**
 * Checkpoints written before typed encoding stored a bare ObjectId hex or the raw string,
 * leaving the type to be guessed from the shape. Kept only to resume those.
 */
bson_value_t* mongo_id_legacy_text_to_bson_value(const std::string& id_text) {
    auto* out = new bson_value_t();
    if (id_text.size() == 24 &&
        std::all_of(id_text.begin(), id_text.end(), [](unsigned char c) {
            return std::isxdigit(c) != 0;
        })) {
        bson_oid_t oid;
        bson_oid_init_from_string(&oid, id_text.c_str());
        out->value_type = BSON_TYPE_OID;
        std::memcpy(out->value.v_oid.bytes, oid.bytes, sizeof(oid.bytes));
        return out;
    }
    out->value_type = BSON_TYPE_UTF8;
    out->value.v_utf8.len = static_cast<int32_t>(id_text.size());
    out->value.v_utf8.str = static_cast<char*>(bson_malloc(id_text.size() + 1));
    std::memcpy(out->value.v_utf8.str, id_text.data(), id_text.size());
    out->value.v_utf8.str[id_text.size()] = '\0';
    return out;
}

bson_value_t* mongo_id_text_to_bson_value(const std::string& id_text) {
    if (id_text.empty()) {
        return nullptr;
    }
    if (id_text.front() == '{') {
        bson_error_t err;
        bson_t* doc = bson_new_from_json(
            reinterpret_cast<const uint8_t*>(id_text.data()),
            static_cast<ssize_t>(id_text.size()),
            &err);
        if (doc) {
            bson_value_t* out = nullptr;
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "v")) {
                const bson_value_t* value = bson_iter_value(&iter);
                if (value) {
                    out = new bson_value_t();
                    bson_value_copy(value, out);
                }
            }
            bson_destroy(doc);
            if (out) {
                return out;
            }
        }
    }
    return mongo_id_legacy_text_to_bson_value(id_text);
}

/**
 * Canonical Extended JSON of a single-key wrapper, e.g. {"v":{"$oid":"..."}}.
 * Carrying the BSON type keeps _id values that are not ObjectId or string resumable, and
 * stops a 24-hex string _id from coming back as an ObjectId, which would sort past every
 * remaining string key and silently copy nothing.
 */
std::string mongo_id_text_from_bson_value(const bson_value_t* value) {
    if (!value) {
        return {};
    }
    bson_t doc = BSON_INITIALIZER;
    if (!bson_append_value(&doc, "v", 1, value)) {
        bson_destroy(&doc);
        return {};
    }
    char* json = bson_as_canonical_extended_json(&doc, nullptr);
    bson_destroy(&doc);
    if (!json) {
        return {};
    }
    std::string out(json);
    bson_free(json);
    return out;
}

/**
 * Column set shared by the copy workers.
 * Mongo evolves the lake schema mid-copy, so the map and the DDL that follows it have to
 * be serialized: two workers seeing the same new field would both issue ADD COLUMN, and
 * ALTER COLUMN TYPE takes ACCESS EXCLUSIVE on a partitioned table, which must not land
 * while another worker sits inside a COPY.
 */
struct MongoSchemaState {
    std::shared_mutex ddl_mtx;
    std::mutex meta_mtx;
    std::map<std::string, std::string> pg_cols;
    std::vector<std::string> all_cols;
    unsigned long long version{0};
};

long long fetch_mongo_collection_count(mongoc_collection_t* coll) {
    bson_t empty = BSON_INITIALIZER;
    bson_error_t err;
    const int64_t count = mongoc_collection_count_documents(coll, &empty, nullptr, 0, nullptr, &err);
    bson_destroy(&empty);
    if (count < 0) {
        throw std::runtime_error(std::string("MongoDB count failed: ") + err.message);
    }
    return static_cast<long long>(count);
}

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
          AND (
            status <> 'full_load_in_progress'
            OR EXISTS (
                SELECT 1 FROM cdc_catalog.full_load_checkpoint cp
                WHERE cp.catalog_id = catalog.catalog_id AND cp.phase = 'copy'
            )
          )
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
    PGconn* log_pg,
    std::mutex* log_mtx,
    const MongoCatalogTableRow& target,
    const std::string& pg_schema,
    const std::string& pg_table,
    MongoSchemaState& schema,
    std::size_t batch_size,
    int source_sleep_ms,
    const std::string& snapshot_id,
    const full_load::PkSlice& id_slice = {},
    full_load::CopyCheckpointContext* checkpoint_ctx = nullptr) {
    const std::string load_ts = utc_now_ts();
    const std::string load_date = utc_now_date();

    auto rebuild_copy_sql = [&](const std::vector<std::string>& cols) {
        std::ostringstream copy_cols;
        for (const auto& c : cols) {
            copy_cols << pg_ident(c) << ", ";
        }
        copy_cols << pg_ident("_dl_load_timestamp") << ", " << pg_ident("_dl_load_date") << ", "
                  << pg_ident("_dl_source_system") << ", " << pg_ident("_dl_snapshot_id");
        const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
        return std::string("COPY ") + fq + " (" + copy_cols.str() + ") FROM STDIN WITH (FORMAT csv)";
    };

    // Local view of the shared column set, refreshed only when another worker moved it.
    std::vector<std::string> local_cols;
    unsigned long long local_version = 0;
    std::string copy_sql;
    auto refresh_local_schema = [&]() {
        std::lock_guard<std::mutex> meta(schema.meta_mtx);
        if (!copy_sql.empty() && local_version == schema.version) {
            return;
        }
        local_version = schema.version;
        local_cols = schema.all_cols;
        copy_sql = rebuild_copy_sql(local_cols);
    };
    refresh_local_schema();

    long long total_rows = 0;
    bson_value_t* last_id_value = nullptr;
    std::string last_mongo_id_text;

    if (checkpoint_ctx) {
        std::vector<std::string> resume_pk = checkpoint_ctx->initial_last_pk;
        full_load::adopt_lake_copy_position(*checkpoint_ctx, pg, resume_pk);
        if (!resume_pk.empty() && !resume_pk.front().empty()) {
            last_mongo_id_text = resume_pk.front();
            last_id_value = mongo_id_text_to_bson_value(last_mongo_id_text);
        }
    }

    bson_value_t* slice_begin_value =
        id_slice.has_begin && !id_slice.begin.empty() ? mongo_id_text_to_bson_value(id_slice.begin.front()) : nullptr;
    bson_value_t* slice_end_value =
        id_slice.has_end && !id_slice.end.empty() ? mongo_id_text_to_bson_value(id_slice.end.front()) : nullptr;

    auto destroy_value = [](bson_value_t*& value) {
        if (value) {
            bson_value_destroy(value);
            delete value;
            value = nullptr;
        }
    };
    auto cleanup_last_id = [&]() {
        destroy_value(last_id_value);
    };
    auto cleanup_slice = [&]() {
        destroy_value(slice_begin_value);
        destroy_value(slice_end_value);
    };

    try {
    while (true) {
        if (g_shutdown.load()) {
            break;
        }
        bson_t query = BSON_INITIALIZER;
        // Resume position wins over the slice lower bound; both are the same edge, but the
        // resume one is exclusive because that document is already in the lake.
        const bson_value_t* lower = last_id_value ? last_id_value : slice_begin_value;
        const char* lower_op = last_id_value ? "$gt" : "$gte";
        if (lower || slice_end_value) {
            bson_t id_wrap;
            bson_append_document_begin(&query, "_id", 3, &id_wrap);
            if (lower) {
                bson_append_value(&id_wrap, lower_op, static_cast<int>(std::strlen(lower_op)), lower);
            }
            if (slice_end_value) {
                bson_append_value(&id_wrap, "$lt", 3, slice_end_value);
            }
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
        std::map<std::string, std::string> cols_after_merge;
        {
            std::lock_guard<std::mutex> meta(schema.meta_mtx);
            for (const auto& [name, pg_type] : batch_schema) {
                if (name == "mongo_id") {
                    continue;
                }
                const auto it = schema.pg_cols.find(name);
                if (it == schema.pg_cols.end()) {
                    schema.pg_cols[name] = pg_type;
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
                cols_after_merge = schema.pg_cols;
            }
        }
        if (schema_changed) {
            // Exclusive: ALTER TABLE must not run while any worker holds a COPY open.
            std::unique_lock<std::shared_mutex> ddl(schema.ddl_mtx);
            sync_missing_mongo_columns(pg, pg_schema, pg_table, cols_after_merge);
            sync_mongo_column_types(pg, pg_schema, pg_table, cols_after_merge);
            std::lock_guard<std::mutex> meta(schema.meta_mtx);
            schema.all_cols = {"mongo_id"};
            for (const auto& [name, _] : schema.pg_cols) {
                if (name != "mongo_id") {
                    schema.all_cols.push_back(name);
                }
            }
            schema.version += 1;
        }
        refresh_local_schema();

        for (const auto& flat : batch_flats) {
            std::ostringstream line;
            for (std::size_t i = 0; i < local_cols.size(); ++i) {
                if (i) {
                    line << ',';
                }
                const auto it = flat.find(local_cols[i]);
                if (it != flat.end()) {
                    line << json_cell_csv(it->second);
                }
            }
            line << ',' << csv_escape(load_ts) << ',' << csv_escape(load_date) << ",MongoDB," << csv_escape(snapshot_id);
            batch_lines.push_back(line.str());
        }

        if (last_id_value) {
            last_mongo_id_text = mongo_id_text_from_bson_value(last_id_value);
        }
        // The position has to land in the same transaction as the rows it points at,
        // otherwise a crash in between makes a resume replay this batch.
        const bool transactional = checkpoint_ctx && !last_mongo_id_text.empty();

        {
            // Shared: concurrent COPYs are fine, they only have to exclude schema DDL.
            std::shared_lock<std::shared_mutex> copy_guard(schema.ddl_mtx);
            if (transactional) {
                pg_exec(pg, "BEGIN");
            }
            try {
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

                if (transactional) {
                    full_load::record_lake_copy_position(
                        pg,
                        checkpoint_ctx->catalog_id,
                        checkpoint_ctx->worker_id,
                        checkpoint_ctx->batch_id,
                        {last_mongo_id_text},
                        checkpoint_ctx->rows_loaded_session_baseline + total_rows +
                            static_cast<long long>(batch_lines.size()));
                    pg_exec(pg, "COMMIT");
                }
            } catch (...) {
                pg_abort_copy_in(pg);
                if (transactional) {
                    PQclear(PQexec(pg, "ROLLBACK"));
                }
                throw;
            }
        }

        total_rows += static_cast<long long>(batch_lines.size());
        if (checkpoint_ctx && !last_mongo_id_text.empty()) {
            full_load::save_copy_batch_checkpoint(
                *checkpoint_ctx,
                {last_mongo_id_text},
                total_rows);
        }
        if (batch_lines.size() < batch_size) {
            break;
        }
        if (source_sleep_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(source_sleep_ms));
        }
    }
    } catch (...) {
        cleanup_last_id();
        cleanup_slice();
        throw;
    }

    cleanup_last_id();
    cleanup_slice();
    return total_rows;
}

/**
 * Reads the _id values sitting at even document offsets along the _id index.
 * Returns a single unbounded slice when a boundary cannot be read, so an unexpected
 * collection shape degrades to the previous single-worker behaviour.
 */
std::vector<full_load::PkSlice> sample_mongo_id_slices(
    mongoc_collection_t* coll,
    long long source_rows,
    int workers) {
    std::vector<std::vector<std::string>> boundaries;
    for (long long offset : full_load::slice_boundary_offsets(source_rows, workers)) {
        bson_t query = BSON_INITIALIZER;
        bson_t opts = BSON_INITIALIZER;
        bson_t sort_doc;
        BSON_APPEND_DOCUMENT_BEGIN(&opts, "sort", &sort_doc);
        BSON_APPEND_INT32(&sort_doc, "_id", 1);
        bson_append_document_end(&opts, &sort_doc);
        bson_t projection;
        BSON_APPEND_DOCUMENT_BEGIN(&opts, "projection", &projection);
        BSON_APPEND_INT32(&projection, "_id", 1);
        bson_append_document_end(&opts, &projection);
        BSON_APPEND_INT64(&opts, "skip", offset);
        BSON_APPEND_INT32(&opts, "limit", 1);

        mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
        bson_destroy(&query);
        bson_destroy(&opts);

        std::string boundary;
        const bson_t* doc = nullptr;
        if (cursor && mongoc_cursor_next(cursor, &doc)) {
            bson_iter_t iter;
            if (bson_iter_init_find(&iter, doc, "_id")) {
                boundary = mongo_id_text_from_bson_value(bson_iter_value(&iter));
            }
        }
        if (cursor) {
            mongoc_cursor_destroy(cursor);
        }
        if (boundary.empty()) {
            return {full_load::PkSlice{}};
        }
        boundaries.push_back({boundary});
    }
    return full_load::slices_from_boundaries(boundaries);
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

    const std::size_t batch_size = pipeline_defaults::kFullLoadBatchSizeDefault;
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
    std::vector<FullLoadCheckpoint> resume_checkpoints;
    const bool resume_copy = full_load::should_resume_from_copy_checkpoint(
        app_pg.raw,
        lake_pg.raw,
        target.catalog_id,
        pg_schema,
        pg_table,
        resume_checkpoints);

    long long source_rows = -1;
    try {
        source_rows = fetch_mongo_collection_count(coll);
    } catch (const std::exception& ex) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "source row count unavailable",
            {{"error", ex.what()}},
            target.conn_id,
            target.source_database,
            target.source_table);
    }

    if (resume_copy) {
        long long checkpoint_rows = 0;
        for (const auto& cp : resume_checkpoints) {
            if (cp.phase == FullLoadPhase::Copy) {
                checkpoint_rows += cp.rows_loaded;
            }
        }
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            batch_id,
            "full load resumed from checkpoint",
            {{"checkpoint_rows", checkpoint_rows},
             {"worker_checkpoints", static_cast<int>(resume_checkpoints.size())}},
            target.conn_id,
            target.source_database,
            target.source_table);
    } else {
        clear_full_load_checkpoints(app_pg.raw, target.catalog_id);
        full_load::clear_lake_copy_positions(lake_pg.raw, target.catalog_id);
        const auto trunc = full_load::truncate_lake_table_verified(
            lake_pg.raw,
            pg_schema,
            pg_table,
            pipeline_defaults::kFullLoadTruncateMaxRetries);
        if (!trunc.ok) {
            log_fl(
                log_pg,
                log_mtx,
                LogLevel::Error,
                batch_id,
                "lake table truncate failed",
                {{"rows_after_truncate", trunc.rows_after},
                 {"attempts", trunc.attempts},
                 {"error", trunc.error}},
                target.conn_id,
                target.source_database,
                target.source_table);
            release_lock();
            mark_catalog_failed(
                app_pg.raw,
                target.catalog_id,
                "truncate failed: " + trunc.error);
            return false;
        }
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Info,
            batch_id,
            "lake table truncated",
            {{"rows_after_truncate", trunc.rows_after}, {"attempts", trunc.attempts}},
            target.conn_id,
            target.source_database,
            target.source_table);
        FullLoadCheckpoint trunc_cp;
        trunc_cp.catalog_id = target.catalog_id;
        trunc_cp.worker_id = 0;
        trunc_cp.batch_id = batch_id;
        trunc_cp.phase = FullLoadPhase::Truncate;
        save_full_load_checkpoint(app_pg.raw, trunc_cp);
    }

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

    {
        FullLoadCheckpoint ddl_cp;
        ddl_cp.catalog_id = target.catalog_id;
        ddl_cp.worker_id = 0;
        ddl_cp.batch_id = batch_id;
        ddl_cp.phase = FullLoadPhase::Ddl;
        if (source_rows >= 0) {
            ddl_cp.source_rows = source_rows;
        }
        save_full_load_checkpoint(app_pg.raw, ddl_cp);
    }

    MongoSchemaState schema_state;
    schema_state.pg_cols = pg_cols;
    schema_state.all_cols = {"mongo_id"};
    for (const auto& [name, _] : pg_cols) {
        if (name != "mongo_id") {
            schema_state.all_cols.push_back(name);
        }
    }

    const std::optional<long long> source_rows_opt =
        source_rows >= 0 ? std::optional<long long>(source_rows) : std::nullopt;

    // A resumed load reuses the persisted split: re-sampling a collection that changed
    // since would move the boundaries and duplicate or skip documents.
    std::vector<full_load::PkSlice> slices;
    bool plan_persisted = false;
    if (resume_copy) {
        slices = full_load::slice_plan_from_checkpoints(resume_checkpoints);
        plan_persisted = !slices.empty();
    }
    std::string split_mode = "resumed_plan";
    if (slices.empty()) {
        const int workers = pipeline_defaults::kMongoFullLoadWorkers;
        if (workers <= 1) {
            split_mode = "single_worker";
            slices.emplace_back();
        } else if (source_rows >= pipeline_defaults::kFullLoadSliceSampleMinRows) {
            split_mode = "sampled_boundaries";
            slices = sample_mongo_id_slices(coll, source_rows, workers);
        } else {
            split_mode = source_rows < 0 ? "single_worker_unknown_row_count" : "single_worker_small_table";
            slices.emplace_back();
        }
    }
    if (slices.empty()) {
        slices.emplace_back();
    }

    const int worker_count = static_cast<int>(slices.size());
    if (!plan_persisted && worker_count > 1) {
        full_load::save_copy_slice_plan(
            app_pg.raw, target.catalog_id, batch_id, slices, source_rows_opt);
    }

    std::mutex checkpoint_mtx;
    auto checkpoint_for_worker = [&](int worker_id) -> full_load::CopyCheckpointContext {
        full_load::CopyCheckpointContext ctx;
        ctx.app_pg = app_pg.raw;
        ctx.log_pg = log_pg;
        ctx.log_mtx = log_mtx;
        ctx.catalog_id = target.catalog_id;
        ctx.worker_id = worker_id;
        ctx.batch_id = batch_id;
        ctx.conn_id = target.conn_id;
        ctx.source_schema = target.source_database;
        ctx.source_table = target.source_table;
        ctx.log_component = "mongo_load";
        ctx.checkpoint_mtx = &checkpoint_mtx;
        ctx.progress_log_interval = pipeline_defaults::kFullLoadCopyProgressLogInterval;
        ctx.source_rows = source_rows_opt;
        if (resume_copy) {
            for (const auto& cp : resume_checkpoints) {
                if (cp.worker_id == worker_id && cp.phase == FullLoadPhase::Copy) {
                    ctx.initial_last_pk = last_pk_from_json(cp.last_pk);
                    ctx.rows_loaded_session_baseline = cp.rows_loaded;
                    break;
                }
            }
        }
        return ctx;
    };

    log_fl(
        log_pg,
        log_mtx,
        LogLevel::Info,
        batch_id,
        worker_count > 1 ? "parallel copy workers started" : "copy running on a single worker",
        {{"workers", worker_count},
         {"workers_requested", pipeline_defaults::kMongoFullLoadWorkers},
         {"split_mode", split_mode},
         {"source_rows", source_rows}},
        target.conn_id,
        target.source_database,
        target.source_table);

    if (worker_count == 1) {
        auto ctx = checkpoint_for_worker(0);
        rows_out = copy_collection_batches(
            coll,
            lake_pg.raw,
            log_pg,
            log_mtx,
            target,
            pg_schema,
            pg_table,
            schema_state,
            batch_size,
            source_sleep_ms,
            batch_id,
            slices[0],
            &ctx);
    } else {
        std::vector<long long> row_counts(static_cast<std::size_t>(worker_count), 0);
        std::vector<std::exception_ptr> errors(static_cast<std::size_t>(worker_count));
        std::vector<std::thread> threads;
        threads.reserve(static_cast<std::size_t>(worker_count));

        for (int w = 0; w < worker_count; ++w) {
            threads.emplace_back([&, w]() {
                try {
                    MongoConn worker_mongo(source);
                    PgConn worker_lake(cfg.datalake.conn_string());
                    // Destroyed inside this scope: the collection borrows worker_mongo's client.
                    mongoc_collection_t* worker_coll =
                        worker_mongo.collection(target.source_database, target.source_table);
                    try {
                        auto ctx = checkpoint_for_worker(w);
                        row_counts[static_cast<std::size_t>(w)] = copy_collection_batches(
                            worker_coll,
                            worker_lake.raw,
                            log_pg,
                            log_mtx,
                            target,
                            pg_schema,
                            pg_table,
                            schema_state,
                            batch_size,
                            source_sleep_ms,
                            batch_id,
                            slices[static_cast<std::size_t>(w)],
                            &ctx);
                    } catch (...) {
                        mongoc_collection_destroy(worker_coll);
                        throw;
                    }
                    mongoc_collection_destroy(worker_coll);
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
        rows_out = 0;
        for (long long n : row_counts) {
            rows_out += n;
        }
    }

    destroy_coll();

    const long long lake_rows = full_load::lake_table_row_count(lake_pg.raw, pg_schema, pg_table);
    const auto verify = full_load::verify_full_load_row_counts(
        full_load::build_row_count_verify_request(
            app_pg.raw, target.catalog_id, source_rows, lake_rows, rows_out));
    if (!verify.ok) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Error,
            batch_id,
            "full load row count verify failed",
            full_load::row_count_verify_log_context(verify, rows_out),
            target.conn_id,
            target.source_database,
            target.source_table);
        release_lock();
        mark_catalog_failed(
            app_pg.raw,
            target.catalog_id,
            "row count verify failed: " + verify.message);
        return false;
    }

    mark_catalog_success(app_pg.raw, target.catalog_id);
    clear_full_load_checkpoints(app_pg.raw, target.catalog_id);
    full_load::clear_lake_copy_positions(lake_pg.raw, target.catalog_id);

    if (!onboard_table_after_full_load(
            app_pg.raw,
            target.conn_id,
            "mongodb",
            target.catalog_id,
            batch_id,
            target.source_database,
            target.source_table)) {
        log_fl(
            log_pg,
            log_mtx,
            LogLevel::Warning,
            batch_id,
            "table full load copied; cdc enable deferred (daemon will retry onboard)",
            {},
            target.conn_id,
            target.source_database,
            target.source_table);
    }

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
    release_lock();
    return true;
    } catch (...) {
        release_lock();
        destroy_coll();
        throw;
    }
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

    log_fl(
        log_pg,
        nullptr,
        LogLevel::Info,
        batch_id,
        "full load started",
        {{"batch_size", pipeline_defaults::kFullLoadBatchSizeDefault},
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
        recover_full_load_for_checkpoint_resume(app_pg.raw, cid, "mongodb", batch_id);
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

        stats.conn_ids.insert(conn_id);
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
