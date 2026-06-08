#include "cdc_pre_apply.hpp"

#include "capture_common.hpp"
#include "cdc_catchup.hpp"
#include "config.hpp"
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
int sync_mssql_columns_for_tier(
    const AppConfig& cfg,
    PGconn* log_pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    (void)runtime;
    const MssqlSource* source = find_mssql_source(cfg, conn_id);
    if (!source) {
        return 0;
    }
    const char* vals[] = {conn_id.c_str(), tier.c_str()};
    PGresult* res = PQexecParams(
        log_pg,
        R"(
        SELECT source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1 AND db_engine = 'mssql' AND active = true
          AND cdc_enabled = true AND service_tier::text = lower($2)
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

    MssqlConn mssql(*source);
    PgConn lake_pg(cfg.datalake.conn_string());
    int synced = 0;
    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string db = PQgetvalue(res, i, 0);
        const std::string schema = PQgetvalue(res, i, 1);
        const std::string table = PQgetvalue(res, i, 2);
        if (!runtime.get_bool("ddl_sync_columns", true, "mssql_load", conn_id)) {
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
    return synced;
}
#endif

#ifdef HAVE_MONGOC
int sync_mongo_columns_for_tier(
    const AppConfig& cfg,
    PGconn* log_pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    const MongoSource* source = find_mongo_source(cfg, conn_id);
    if (!source) {
        return 0;
    }
    if (!runtime.get_bool("ddl_sync_columns", true, "mongo_load", conn_id)) {
        return 0;
    }

    const char* vals[] = {conn_id.c_str(), tier.c_str()};
    PGresult* res = PQexecParams(
        log_pg,
        R"(
        SELECT source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1 AND db_engine = 'mongodb' AND active = true
          AND cdc_enabled = true AND service_tier::text = lower($2)
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
        runtime.get_size_t("ddl_sync_sample_size", 1000, "mongo_load", conn_id);
    MongoConn mongo(*source);
    PgConn lake_pg(cfg.datalake.conn_string());
    if (!lake_pg.raw) {
        PQclear(res);
        return 0;
    }

    int synced = 0;
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
    return synced;
}
#endif

PreApplyCycleResult run_mariadb_pre_apply(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    PreApplyCycleResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"tier", tier},
        {"conn_id", conn_id},
        {"db_engine", "mariadb"},
    };

    const auto catchup = run_catchup_if_needed(cfg, log_pg, conn_id, tier, batch_id);
    if (!catchup.empty()) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto& item : catchup) {
            arr.push_back(item.error.empty() ? item.payload : nlohmann::json{{"error", item.error}});
            if (!item.error.empty()) {
                result.errors += 1;
            }
        }
        result.payload["catchup"] = arr;
    }

    return result;
}

PreApplyCycleResult run_mssql_pre_apply(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    PreApplyCycleResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"tier", tier},
        {"conn_id", conn_id},
        {"db_engine", "mssql"},
    };

    const auto catchup = run_catchup_if_needed(cfg, log_pg, conn_id, tier, batch_id);
    if (!catchup.empty()) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto& item : catchup) {
            arr.push_back(item.error.empty() ? item.payload : nlohmann::json{{"error", item.error}});
            if (!item.error.empty()) {
                result.errors += 1;
            }
        }
        result.payload["catchup"] = arr;
    }

#ifdef HAVE_FREETDS
    RuntimeConfig runtime;
    const int ddl_synced = sync_mssql_columns_for_tier(cfg, log_pg, runtime, conn_id, tier, batch_id);
    if (ddl_synced > 0) {
        result.payload["ddl_sync"] = {{"tables", ddl_synced}};
    }
#endif
    return result;
}

PreApplyCycleResult run_mongo_pre_apply(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    PreApplyCycleResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"tier", tier},
        {"conn_id", conn_id},
        {"db_engine", "mongodb"},
    };

    const auto catchup = run_catchup_if_needed(cfg, log_pg, conn_id, tier, batch_id);
    if (!catchup.empty()) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto& item : catchup) {
            arr.push_back(item.error.empty() ? item.payload : nlohmann::json{{"error", item.error}});
            if (!item.error.empty()) {
                result.errors += 1;
            }
        }
        result.payload["catchup"] = arr;
    }

#ifdef HAVE_MONGOC
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int ddl_synced = sync_mongo_columns_for_tier(cfg, log_pg, runtime, conn_id, tier, batch_id);
    if (ddl_synced > 0) {
        result.payload["ddl_sync"] = {{"tables", ddl_synced}};
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
    const std::optional<std::string> all_tiers;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_daemon",
        .message = "conn capture started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"db_engine", db_engine}, {"tier", "all"}},
    });

    int errors = 0;
    if (db_engine == "mssql") {
        const auto stats = run_mssql_kafka_capture_slice(cfg, log_pg, conn_id, all_tiers, batch_id);
        errors = stats.errors;
    } else if (db_engine == "mongodb") {
        const auto stats = run_mongo_kafka_capture_slice(cfg, log_pg, conn_id, all_tiers, batch_id);
        errors = stats.errors;
    } else {
        const auto stats = run_mariadb_kafka_capture_slice(cfg, log_pg, conn_id, all_tiers, batch_id);
        errors = stats.errors;
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
    const std::string& tier,
    const std::string& batch_id) {
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
        .context = {{"tier", tier}, {"db_engine", db_engine}, {"phase", "pre_apply"}},
    });

    PreApplyCycleResult result;
    if (db_engine == "mssql") {
        result = run_mssql_pre_apply(cfg, log_pg, conn_id, tier, batch_id);
    } else if (db_engine == "mongodb") {
        result = run_mongo_pre_apply(cfg, log_pg, conn_id, tier, batch_id);
    } else {
        result = run_mariadb_pre_apply(cfg, log_pg, conn_id, tier, batch_id);
    }

    result.payload["duration_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - cycle_start)
                                        .count();
    return result;
}
