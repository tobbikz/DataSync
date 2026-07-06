#include "cdc_pre_apply.hpp"

#include "capture_common.hpp"
#include "mariadb_kafka_capture.hpp"
#include "mongo_conn.hpp"
#include "mongo_kafka_capture.hpp"
#include "mongo_lake.hpp"
#include "mssql_kafka_capture.hpp"
#include "mssql_lake.hpp"
#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#include "mssql_ddl_sync.hpp"
#include "mssql_schema.hpp"
#endif

#include <chrono>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

namespace {

#ifdef HAVE_FREETDS
int sync_mssql_columns_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& batch_id,
    int daemon_round) {
    (void)runtime;
    const MssqlSource* source = find_mssql_source(cfg, conn_id);
    if (!source) {
        return 0;
    }
    const std::string round_str = std::to_string(daemon_round);
    const char* vals[] = {conn_id.c_str(), round_str.c_str()};
    PGresult* res = PQexecParams(
        log_pg,
        R"(
        SELECT source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1 AND db_engine = 'mssql' AND active = true
          AND cdc_enabled = true
          AND (last_error IS NOT NULL OR (catalog_id % 20) = ($2::bigint % 20))
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return 0;
    }
    const bool has_res = true;

    int synced = 0;
    try {
        MssqlConn mssql(*source);
        PgConn lake_pg(cfg.datalake.conn_string());
        for (int i = 0; i < PQntuples(res); ++i) {
            const std::string db = PQgetvalue(res, i, 0);
            const std::string schema = PQgetvalue(res, i, 1);
            const std::string table = PQgetvalue(res, i, 2);
            if (!pipeline_defaults::kDdlSyncColumns) {
                continue;
            }
            const auto ddl = sync_mssql_columns_to_lake(
                lake_pg.raw, mssql, db, schema, table, runtime, conn_id);
            if (ddl.columns_added > 0 || ddl.columns_widened > 0) {
                synced += 1;
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_mssql_ddl",
                    .message = "mssql ddl sync columns updated",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = schema,
                    .source_table = table,
                    .context = {{"columns_added", ddl.columns_added}, {"columns_widened", ddl.columns_widened}},
                });
            }
        }
        PQclear(res);
    } catch (const std::exception& ex) {
        if (has_res) PQclear(res);
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_mssql_ddl",
            .message = "mssql connect failed during pre-apply ddl sync",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}},
        });
        return -1;
    }
    return synced;
}
#endif

#ifdef HAVE_MONGOC
int sync_mongo_columns_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& batch_id,
    int daemon_round) {
    const MongoSource* source = find_mongo_source(cfg, conn_id);
    if (!source) {
        return 0;
    }
    if (!pipeline_defaults::kDdlSyncColumns) {
        return 0;
    }

    const std::string round_str = std::to_string(daemon_round);
    const char* vals[] = {conn_id.c_str(), round_str.c_str()};
    PGresult* res = PQexecParams(
        log_pg,
        R"(
        SELECT source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1 AND db_engine = 'mongodb' AND active = true
          AND cdc_enabled = true
          AND (last_error IS NOT NULL OR (catalog_id % 20) = ($2::bigint % 20))
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return 0;
    }

    const std::size_t sample_limit =
        pipeline_defaults::kDdlSyncSampleSize;

    int synced = 0;
    try {
        MongoConn mongo(*source);
        PgConn lake_pg(cfg.datalake.conn_string());
        if (!lake_pg.raw) {
            PQclear(res);
            return 0;
        }

        for (int i = 0; i < PQntuples(res); ++i) {
            const std::string db = PQgetvalue(res, i, 0);
            const std::string schema = mongo_catalog_source_schema(db, PQgetvalue(res, i, 1));
            const std::string coll = PQgetvalue(res, i, 2);
            const std::string pg_schema = mongo_pg_schema_name(db);
            const std::string pg_table = mongo_pg_table_name(coll);

            mongoc_collection_t* collection = mongo.collection(db, coll);
            const auto ddl = sync_mongo_lake_columns_from_collection(
                lake_pg.raw, collection, pg_schema, pg_table, sample_limit);
            mongoc_collection_destroy(collection);

            if (ddl.columns_added > 0 || ddl.columns_widened > 0) {
                synced += 1;
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_mongo_ddl",
                    .message = "mongo ddl sync columns updated",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = schema,
                    .source_table = coll,
                    .context = {
                        {"columns_added", ddl.columns_added},
                        {"columns_widened", ddl.columns_widened},
                    },
                });
            }
        }
        PQclear(res);
    } catch (...) {
        PQclear(res);
        throw;
    }
    return synced;
}
#endif

PreApplyCycleResult run_mssql_pre_apply(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int daemon_round) {
    PreApplyCycleResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"conn_id", conn_id},
        {"db_engine", "mssql"},
        {"daemon_round", daemon_round},
        {"ddl_sync_mode", "incremental"},
    };

#ifdef HAVE_FREETDS
    RuntimeConfig runtime;
    const int ddl_synced =
        sync_mssql_columns_for_conn(cfg, log_pg, runtime, conn_id, batch_id, daemon_round);
    if (ddl_synced < 0) {
        result.errors += 1;
    } else if (ddl_synced > 0) {
        result.payload["ddl_sync"] = {{"tables", ddl_synced}};
    }
#endif
    return result;
}

PreApplyCycleResult run_mongo_pre_apply(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int daemon_round) {
    PreApplyCycleResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"conn_id", conn_id},
        {"db_engine", "mongodb"},
        {"daemon_round", daemon_round},
        {"ddl_sync_mode", "incremental"},
    };

#ifdef HAVE_MONGOC
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    try {
        const int ddl_synced =
            sync_mongo_columns_for_conn(cfg, log_pg, runtime, conn_id, batch_id, daemon_round);
        if (ddl_synced > 0) {
            result.payload["ddl_sync"] = {{"tables", ddl_synced}};
        }
    } catch (const std::exception& ex) {
        result.errors += 1;
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_mongo_ddl",
            .message = "mongo pre-apply ddl sync failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}},
        });
    }
#endif
    return result;
}

}  // namespace

int run_conn_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id) {
    const std::string db_engine = conn_engine(cfg, conn_id);

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_daemon",
        .message = "conn capture started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix", topic_prefix_for_conn(conn_id)},
        },
    });

    int errors = 0;
    if (db_engine == "mssql") {
        try {
            const auto stats = run_mssql_kafka_capture_slice(cfg, log_pg, conn_id, batch_id);
            errors = stats.errors;
        } catch (const std::exception& ex) {
            errors = 1;
            mark_capture_position_failed(log_pg, conn_id, ex.what());
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_mssql_capture",
                .message = "mssql capture slice failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"error", ex.what()}},
            });
        }
    } else if (db_engine == "mongodb") {
        try {
            const auto stats = run_mongo_kafka_capture_slice(cfg, log_pg, conn_id, batch_id);
            errors = stats.errors;
        } catch (const std::exception& ex) {
            errors = 1;
            mark_capture_position_failed(log_pg, conn_id, ex.what());
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_mongo_capture",
                .message = "mongo capture slice failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"error", ex.what()}},
            });
        }
    } else {
        try {
            const auto stats = run_mariadb_kafka_capture_slice(cfg, log_pg, conn_id, batch_id);
            errors = stats.errors;
        } catch (const std::exception& ex) {
            errors = 1;
            mark_capture_position_failed(log_pg, conn_id, ex.what());
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_capture",
                .message = "mariadb capture slice failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"error", ex.what()}},
            });
        }
    }

    if (errors > 0 && db_engine == "mariadb") {
        mark_capture_position_failed(log_pg, conn_id, "conn capture completed with errors");
    }

    log_write(log_pg, {
        .level = errors ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_kafka_daemon",
        .message = errors ? "conn capture completed with errors" : "conn capture completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"db_engine", db_engine}, {"errors", errors}},
    });
    return errors;
}

PreApplyCycleResult run_pre_apply_cycle(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int daemon_round) {
    const auto cycle_start = std::chrono::steady_clock::now();
    const std::string db_engine = conn_engine(cfg, conn_id);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_daemon",
        .message = "pre-apply cycle started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"phase", "pre_apply"},
            {"daemon_round", daemon_round},
            {"ddl_sync_mode", "incremental"},
        },
    });

    PreApplyCycleResult result;
    if (db_engine == "mssql") {
        result = run_mssql_pre_apply(cfg, log_pg, conn_id, batch_id, daemon_round);
    } else if (db_engine == "mongodb") {
        result = run_mongo_pre_apply(cfg, log_pg, conn_id, batch_id, daemon_round);
    } else {
        result.payload = {
            {"batch_id", batch_id},
            {"conn_id", conn_id},
            {"db_engine", "mariadb"},
            {"daemon_round", daemon_round},
        };
    }

    result.payload["duration_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - cycle_start)
                                        .count();

    log_write(log_pg, {
        .level = result.errors > 0 ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_kafka_daemon",
        .message = result.errors > 0 ? "pre-apply cycle completed with errors" : "pre-apply cycle completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = result.payload,
    });
    return result;
}
