#include "capture_common.hpp"

#ifdef HAVE_FREETDS
#include "mssql_kafka_capture.hpp"
#endif
#ifdef HAVE_MONGOC
#include "mongo_kafka_capture.hpp"
#endif
#include "kafka_topics.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"

#include <sstream>
#include <chrono>
#include <map>
#include <set>
#include <thread>
#include <vector>
#include <stdexcept>

#ifdef HAVE_RDKAFKA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop
#endif

std::string runtime_topic_prefix(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    runtime.reload(pg);
    if (db_engine == "mssql") {
        return runtime.get_string("capture_topic_prefix", "MSSQL_LOCAL", "cdc_kafka_mssql_capture", conn_id);
    }
    if (db_engine == "mongodb") {
        return runtime.get_string("capture_topic_prefix", "MONGO_LOCAL", "cdc_kafka_mongo_capture", conn_id);
    }
    const std::string apply_prefix = runtime.get_string("kafka_topic_prefix", "", "cdc_kafka_apply", conn_id);
    if (!apply_prefix.empty()) {
        return apply_prefix;
    }
    return runtime.get_string("capture_topic_prefix", conn_id, "cdc_kafka_capture", conn_id);
}

std::string kafka_apply_consumer_group(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier) {
    runtime.reload(pg);
    std::string group = runtime.get_string("kafka_consumer_group", "datalake-cdc-apply", "cdc_kafka_apply", conn_id);
    if (!tier.empty()) {
        group += '-';
        group += tier;
    }
    return group;
}

namespace {

void apply_cdc_slice_limits(CaptureRuntimeConfig& cfg, const CdcConfig* cdc) {
    if (!cdc) {
        return;
    }
    if (cdc->slice_max_seconds > 0) {
        cfg.max_seconds = cdc->slice_max_seconds;
    }
    if (cdc->slice_max_events > 0) {
        cfg.max_events = cdc->slice_max_events;
    }
}

}  // namespace

CaptureRuntimeConfig load_mariadb_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    runtime.reload(pg);
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = runtime.get_int("capture_max_seconds", 300, "cdc_kafka_capture", conn_id);
    cfg.max_events = runtime.get_int("capture_max_events", 10000000, "cdc_kafka_capture", conn_id);
    cfg.topic_prefix = runtime.get_string("capture_topic_prefix", conn_id, "cdc_kafka_capture", conn_id);
    cfg.topic_mode = runtime.get_string("kafka_topic_mode", "bucketed", "cdc_kafka_capture", conn_id);
    cfg.topic_buckets = runtime.get_int("kafka_topic_buckets", 64, "cdc_kafka_capture", conn_id);
    cfg.bootstrap = runtime.get_string("kafka_bootstrap_servers", "localhost:9092", "cdc_kafka_apply", conn_id);
    cfg.linger_ms = runtime.get_int("capture_producer_linger_ms", 5, "cdc_kafka_capture", conn_id);
    cfg.producer_batch = runtime.get_int("capture_producer_batch_size", 10000, "cdc_kafka_capture", conn_id);
    cfg.topic_partitions = runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_capture", conn_id);
    cfg.idle_poll_seconds = runtime.get_int("capture_idle_poll_seconds", 3, "cdc_kafka_capture", conn_id);
    cfg.heartbeat_seconds = runtime.get_int("capture_heartbeat_seconds", 60, "cdc_kafka_capture", conn_id);
    apply_cdc_slice_limits(cfg, cdc);
    return cfg;
}

CaptureRuntimeConfig load_mssql_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    runtime.reload(pg);
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = runtime.get_int("capture_max_seconds", 300, "cdc_kafka_mssql_capture", conn_id);
    cfg.max_events = runtime.get_int("capture_max_events", 10000000, "cdc_kafka_mssql_capture", conn_id);
    cfg.topic_prefix = runtime.get_string("capture_topic_prefix", conn_id, "cdc_kafka_mssql_capture", conn_id);
    cfg.topic_mode = runtime.get_string("kafka_topic_mode", "bucketed", "cdc_kafka_capture", conn_id);
    cfg.topic_buckets = runtime.get_int("kafka_topic_buckets", 64, "cdc_kafka_mssql_capture", conn_id);
    cfg.bootstrap = runtime.get_string("kafka_bootstrap_servers", "localhost:9092", "cdc_kafka_apply", conn_id);
    cfg.linger_ms = runtime.get_int("capture_producer_linger_ms", 5, "cdc_kafka_mssql_capture", conn_id);
    cfg.producer_batch = runtime.get_int("capture_producer_batch_size", 10000, "cdc_kafka_mssql_capture", conn_id);
    cfg.topic_partitions = runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mssql_capture", conn_id);
    cfg.idle_poll_seconds =
        runtime.get_int("capture_idle_poll_seconds", 3, "cdc_kafka_mssql_capture", conn_id);
    cfg.heartbeat_seconds =
        runtime.get_int("capture_heartbeat_seconds", 60, "cdc_kafka_mssql_capture", conn_id);
    cfg.mssql_replay_on_idle =
        runtime.get_bool("mssql_capture_replay_on_idle", false, "cdc_kafka_mssql_capture", conn_id);
    apply_cdc_slice_limits(cfg, cdc);
    return cfg;
}

CaptureRuntimeConfig load_mongo_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    runtime.reload(pg);
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = runtime.get_int("capture_max_seconds", 60, "cdc_kafka_mongo_capture", conn_id);
    cfg.max_events = runtime.get_int("capture_max_events", 10000000, "cdc_kafka_mongo_capture", conn_id);
    cfg.topic_prefix = runtime.get_string("capture_topic_prefix", conn_id, "cdc_kafka_mongo_capture", conn_id);
    cfg.topic_mode = runtime.get_string("kafka_topic_mode", "bucketed", "cdc_kafka_capture", conn_id);
    cfg.topic_buckets = runtime.get_int("kafka_topic_buckets", 64, "cdc_kafka_mongo_capture", conn_id);
    cfg.bootstrap = runtime.get_string("kafka_bootstrap_servers", "localhost:9092", "cdc_kafka_apply", conn_id);
    cfg.linger_ms = runtime.get_int("capture_producer_linger_ms", 5, "cdc_kafka_mongo_capture", conn_id);
    cfg.producer_batch = runtime.get_int("capture_producer_batch_size", 10000, "cdc_kafka_mongo_capture", conn_id);
    cfg.topic_partitions = runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mongo_capture", conn_id);
    cfg.idle_poll_seconds =
        runtime.get_int("capture_idle_poll_seconds", 3, "cdc_kafka_mongo_capture", conn_id);
    cfg.heartbeat_seconds =
        runtime.get_int("capture_heartbeat_seconds", 60, "cdc_kafka_mongo_capture", conn_id);
    apply_cdc_slice_limits(cfg, cdc);
    return cfg;
}

void bump_mariadb_capture_heartbeat(MYSQL* mysql) {
    if (!mysql) {
        return;
    }
    (void)mysql_query(mysql, "UPDATE cdc_meta.heartbeat SET note = 'idle' WHERE id = 1");
}

void bump_capture_heartbeat_pg(PGconn* pg, const std::string& conn_id, const std::string& db_engine) {
    if (!pg || conn_id.empty()) {
        return;
    }
    if (db_engine == "mssql") {
        const char* vals[] = {conn_id.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            UPDATE cdc_catalog.cdc_mssql_lsn
            SET updated_at = now()
            WHERE conn_id = $1
            )",
            1,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        if (res) {
            PQclear(res);
        }
        return;
    }
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.capture_position
        SET last_event_ts = now(), updated_at = now()
        WHERE conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

std::vector<std::string> split_pk_columns(const std::string& pk_columns) {
    std::vector<std::string> out;
    std::string part;
    std::istringstream iss(pk_columns);
    while (std::getline(iss, part, ',')) {
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.front()))) {
            part.erase(part.begin());
        }
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.back()))) {
            part.pop_back();
        }
        if (!part.empty()) {
            out.push_back(part);
        }
    }
    return out;
}

std::vector<CaptureCatalogTable> fetch_capture_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    int worker_id,
    int worker_count,
    const std::string& db_engine) {
    std::ostringstream sql;
    std::vector<const char*> vals;
    vals.push_back(conn_id.c_str());
    int param_count = 1;
    std::string tier_val;
    std::string worker_count_str;
    std::string worker_id_str;

    if (db_engine == "mssql") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns, engine_meta::text
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mssql'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else if (db_engine == "mongodb") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns, '{}'::text
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mongodb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else {
        sql << R"(
            SELECT catalog_id, conn_id, '' AS source_database, source_schema, source_table, pk_columns, '{}'::text
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mariadb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    }

    if (tier && !tier->empty()) {
        sql << " AND service_tier::text = lower($" << ++param_count << ")";
        tier_val = *tier;
        vals.push_back(tier_val.c_str());
    }
    if (worker_count > 1) {
        const int mod_param = ++param_count;
        const int eq_param = ++param_count;
        sql << " AND mod(catalog_id, $" << mod_param << ") = $" << eq_param;
        worker_count_str = std::to_string(worker_count);
        worker_id_str = std::to_string(worker_id);
        vals.push_back(worker_count_str.c_str());
        vals.push_back(worker_id_str.c_str());
    }
    sql << " ORDER BY source_schema, source_table";

    PGresult* res = PQexecParams(
        pg,
        sql.str().c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.data(),
        nullptr,
        nullptr,
        0);

    std::vector<CaptureCatalogTable> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        CaptureCatalogTable row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.source_database = PQgetvalue(res, i, 2);
        row.source_schema = PQgetvalue(res, i, 3);
        row.source_table = PQgetvalue(res, i, 4);
        row.pk_columns = PQgetvalue(res, i, 5);
        const char* meta_txt = PQgetvalue(res, i, 6);
        if (meta_txt && *meta_txt) {
            try {
                row.engine_meta = nlohmann::json::parse(meta_txt);
            } catch (...) {
                row.engine_meta = nlohmann::json::object();
            }
        }
        if (db_engine == "mssql") {
            row.lake_schema = mssql_pg_schema_name(row.source_database, row.source_schema);
            row.lake_table = mssql_pg_table_name(row.source_table);
        } else if (db_engine == "mongodb") {
            row.lake_schema = mongo_pg_schema_name(row.source_database);
            row.lake_table = mongo_pg_table_name(row.source_table);
        } else {
            row.lake_schema = row.source_schema;
            row.lake_table = row.source_table;
        }
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

void flag_table_for_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table,
    const std::string& db_engine) {
    const char* vals1[] = {conn_id.c_str(), db_engine.c_str(), schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = false, updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND NOT (source_schema = $3 AND source_table = $4)
        )",
        4,
        nullptr,
        vals1,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }

    const char* vals2[] = {conn_id.c_str(), db_engine.c_str(), schema.c_str(), table.c_str()};
    res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET active = true,
            needs_full_load = true,
            cdc_enabled = false,
            status = 'pending',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND source_schema = $3
          AND source_table = $4
        )",
        4,
        nullptr,
        vals2,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void enable_cdc_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& db_engine,
    const std::string& batch_id) {
    std::string sql = R"(
        UPDATE cdc_catalog.catalog
        SET cdc_enabled = true,
            needs_full_load = false,
            status = 'success',
            last_full_load_at = COALESCE(last_full_load_at, now()),
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = false
          AND has_pk = true
          AND status = 'success'
          AND NOT cdc_enabled
    )";
    std::vector<const char*> vals = {conn_id.c_str(), db_engine.c_str()};
    std::string tier_val;
    if (service_tier && !service_tier->empty()) {
        sql += " AND service_tier::text = lower($3)";
        tier_val = *service_tier;
        vals.push_back(tier_val.c_str());
    }

    PGresult* res = PQexecParams(
        pg,
        sql.c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.data(),
        nullptr,
        nullptr,
        0);
    const int count = (res && PQresultStatus(res) == PGRES_COMMAND_OK) ? std::atoi(PQcmdTuples(res)) : 0;
    if (res) {
        PQclear(res);
    }

    log_write(pg, {
        .level = LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = "enabled cdc for loaded tables",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"count", count}, {"db_engine", db_engine}, {"tier", service_tier.value_or("")}},
    });
}

void mark_catalog_full_load_in_progress(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'full_load_in_progress',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_skipped(PGconn* pg, long long catalog_id, const std::string& reason) {
    const std::string id = std::to_string(catalog_id);
    const std::string trunc = reason.substr(0, 1000);
    const char* vals[] = {id.c_str(), trunc.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'skipped',
            needs_full_load = false,
            cdc_enabled = false,
            last_error = $2,
            last_error_at = now(),
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        vals);
}

void mark_catalog_cdc_in_progress(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'cdc_in_progress',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_cdc_success(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET last_cdc_at = now(),
            status = 'success',
            last_error_at = NULL,
            last_error = NULL,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void clear_stale_full_load_in_progress(PGconn* pg, const std::string& conn_id, const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'pending',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND status = 'full_load_in_progress'
          AND needs_full_load = true
        )",
        2,
        vals);
}

void clear_stale_cdc_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& db_engine) {
    std::string sql = R"(
        UPDATE cdc_catalog.catalog
        SET status = 'success',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND status = 'cdc_in_progress'
          AND cdc_enabled = true
    )";
    std::vector<const char*> vals = {conn_id.c_str(), db_engine.c_str()};
    std::string tier_val;
    if (service_tier && !service_tier->empty()) {
        sql += " AND service_tier::text = lower($3)";
        tier_val = *service_tier;
        vals.push_back(tier_val.c_str());
    }
    pg_exec_params_simple(pg, sql.c_str(), static_cast<int>(vals.size()), vals.data());
}

int count_full_load_pending(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), tier.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND service_tier::text = lower($2)
          AND db_engine = $3::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

int count_full_load_pending_any_tier(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

std::vector<std::string> list_full_load_pending_tiers(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT DISTINCT service_tier::text
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        ORDER BY 1
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<std::string> tiers;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res); ++i) {
            const char* t = PQgetvalue(res, i, 0);
            if (t && *t) {
                tiers.emplace_back(t);
            }
        }
    }
    if (res) {
        PQclear(res);
    }
    return tiers;
}

ApplySkipReasonCounts fetch_apply_skip_reasons(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    const std::string& db_engine) {
    ApplySkipReasonCounts out;
    std::string sql = R"(
        SELECT
            count(*)::int AS active_total,
            count(*) FILTER (WHERE needs_full_load)::int AS needs_full_load,
            count(*) FILTER (WHERE NOT cdc_enabled)::int AS cdc_disabled,
            count(*) FILTER (WHERE NOT has_pk)::int AS no_pk,
            count(*) FILTER (WHERE status IN ('skipped', 'disabled'))::int AS skipped_status,
            count(*) FILTER (
                WHERE cdc_enabled
                  AND NOT needs_full_load
                  AND has_pk
                  AND status NOT IN ('skipped', 'disabled')
            )::int AS apply_ready
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
    )";
    std::vector<const char*> vals = {conn_id.c_str(), db_engine.c_str()};
    std::string tier_val;
    if (tier && !tier->empty()) {
        sql += " AND service_tier::text = lower($3)";
        tier_val = *tier;
        vals.push_back(tier_val.c_str());
    }

    PGresult* res = PQexecParams(
        pg,
        sql.c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.data(),
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out.active_total = std::atoi(PQgetvalue(res, 0, 0));
        out.needs_full_load = std::atoi(PQgetvalue(res, 0, 1));
        out.cdc_disabled = std::atoi(PQgetvalue(res, 0, 2));
        out.no_pk = std::atoi(PQgetvalue(res, 0, 3));
        out.skipped_status = std::atoi(PQgetvalue(res, 0, 4));
        out.apply_ready = std::atoi(PQgetvalue(res, 0, 5));
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

void ensure_apply_positions_for_tier(
    PGconn* pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine) {
    const std::string topic_prefix = runtime_topic_prefix(runtime, pg, conn_id, db_engine);
    const std::string topic_mode = runtime.get_string("kafka_topic_mode", "bucketed", "cdc_kafka_capture", conn_id);
    const int topic_buckets = runtime.get_int("kafka_topic_buckets", 64, "cdc_kafka_capture", conn_id);

    const char* vals[] = {conn_id.c_str(), tier.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT catalog_id, source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND service_tier::text = lower($2)
          AND db_engine = $3::cdc_catalog.db_engine
          AND active = true
          AND has_pk = true
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string catalog_id = PQgetvalue(res, i, 0);
        const std::string source_database = PQgetvalue(res, i, 1);
        const std::string source_schema = PQgetvalue(res, i, 2);
        const std::string source_table = PQgetvalue(res, i, 3);
        std::string lake_schema;
        std::string lake_table;
        if (db_engine == "mssql") {
            lake_schema = mssql_pg_schema_name(source_database, source_schema);
            lake_table = mssql_pg_table_name(source_table);
        } else if (db_engine == "mongodb") {
            lake_schema = mongo_pg_schema_name(source_database);
            lake_table = mongo_pg_table_name(source_table);
        } else {
            lake_schema = source_schema;
            lake_table = source_table;
        }
        const std::string topic =
            topic_for_catalog(topic_prefix, lake_schema, lake_table, topic_mode, topic_buckets);
        const std::string pos_schema = (db_engine == "mongodb")
            ? mongo_catalog_source_schema(source_database, source_schema)
            : (db_engine == "mssql" ? source_schema : lake_schema);
        const char* ins_vals[] = {
            catalog_id.c_str(),
            conn_id.c_str(),
            pos_schema.c_str(),
            lake_table.c_str(),
            topic.c_str(),
        };
        pg_exec_params_simple(
            pg,
            R"(
            INSERT INTO cdc_catalog.apply_position
                (catalog_id, conn_id, source_schema, source_table, kafka_topic, status)
            VALUES ($1::bigint, $2, $3, $4, $5, 'healthy')
            ON CONFLICT (catalog_id) DO UPDATE SET
                conn_id = EXCLUDED.conn_id,
                source_schema = EXCLUDED.source_schema,
                source_table = EXCLUDED.source_table,
                kafka_topic = EXCLUDED.kafka_topic,
                updated_at = now()
            )",
            5,
            ins_vals);
    }
    PQclear(res);
}

FullLoadKafkaResetStats reset_kafka_apply_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine,
    const std::string& batch_id) {
    FullLoadKafkaResetStats stats;

    RuntimeConfig runtime;
    runtime.reload(pg);
    const std::string bootstrap = runtime.get_string("kafka_bootstrap_servers", "localhost:9092", "cdc_kafka_apply", conn_id);
    const std::string consumer_group = kafka_apply_consumer_group(runtime, pg, conn_id, tier);
    ensure_apply_positions_for_tier(pg, runtime, conn_id, tier, db_engine);

    const char* vals[] = {conn_id.c_str(), tier.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT ap.catalog_id, ap.source_schema, ap.source_table, ap.kafka_topic, ap.kafka_partition
        FROM cdc_catalog.apply_position ap
        JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
        WHERE ap.conn_id = $1
          AND c.service_tier::text = lower($2)
          AND c.db_engine = $3::cdc_catalog.db_engine
          AND c.active = true
        ORDER BY ap.source_schema, ap.source_table
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "full-load kafka reset skipped: apply_position query failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"tier", tier}, {"db_engine", db_engine}},
        });
        stats.errors += 1;
        return stats;
    }

    struct ApplyRow {
        long long catalog_id;
        std::string schema;
        std::string table;
        std::string topic;
        int partition;
    };
    std::vector<ApplyRow> rows;
    rows.reserve(static_cast<size_t>(PQntuples(res)));
    for (int i = 0; i < PQntuples(res); ++i) {
        rows.push_back({
            std::atoll(PQgetvalue(res, i, 0)),
            PQgetvalue(res, i, 1),
            PQgetvalue(res, i, 2),
            PQgetvalue(res, i, 3),
            std::stoi(PQgetvalue(res, i, 4)),
        });
        stats.tables += 1;
    }
    PQclear(res);

    if (rows.empty()) {
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_catalog_onboard",
            .message = "full-load kafka reset skipped: no apply_position rows",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"tier", tier}, {"db_engine", db_engine}},
        });
        return stats;
    }

#ifdef HAVE_RDKAFKA
    const int topic_partitions = (db_engine == "mssql")
        ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mssql_capture", conn_id)
        : (db_engine == "mongodb")
            ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mongo_capture", conn_id)
            : runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_capture", conn_id);

    std::set<std::string> unique_topic_names;
    for (const auto& row : rows) {
        unique_topic_names.insert(row.topic);
    }

    std::set<std::pair<std::string, int>> unique_topics;
    for (const auto& topic : unique_topic_names) {
        for (int partition = 0; partition < topic_partitions; ++partition) {
            unique_topics.emplace(topic, partition);
        }
    }

    std::map<std::pair<std::string, int>, long long> topic_offsets;
    for (const auto& topic_key : unique_topics) {
        const auto& topic = topic_key.first;
        const int partition = topic_key.second;
        bool reset_ok = false;
        for (int attempt = 0; attempt < 3; ++attempt) {
            try {
                const long long end_offset =
                    reset_apply_offset_to_end(bootstrap, consumer_group, topic, partition);
                if (end_offset >= 0) {
                    topic_offsets.emplace(topic_key, end_offset);
                    stats.topics_reset += 1;
                }
                reset_ok = true;
                break;
            } catch (const std::exception& ex) {
                if (attempt == 2) {
                    stats.errors += 1;
                    log_write(pg, {
                        .level = LogLevel::Warning,
                        .component = "cdc_catalog_onboard",
                        .message = "full-load kafka offset reset failed after retries",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {{"topic", topic}, {"partition", partition}, {"error", ex.what()}},
                    });
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                }
            }
        }
        if (!reset_ok) {
            log_write(pg, {
                .level = LogLevel::Warning,
                .component = "cdc_catalog_onboard",
                .message = "full-load kafka reset aborted: dedup preserved",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"tier", tier},
                    {"db_engine", db_engine},
                    {"topics_reset", stats.topics_reset},
                    {"topics_expected", static_cast<int>(unique_topics.size())},
                    {"errors", stats.errors},
                },
            });
            return stats;
        }
    }

    for (const auto& row : rows) {
        const auto topic_key = std::make_pair(row.topic, row.partition);
        const auto it = topic_offsets.find(topic_key);
        if (it == topic_offsets.end()) {
            stats.errors += 1;
            continue;
        }
        const std::string offset_str = std::to_string(it->second);
        const std::string catalog_id_str = std::to_string(row.catalog_id);
        const char* upd_vals[] = {offset_str.c_str(), catalog_id_str.c_str()};
        PGresult* upd = PQexecParams(
            pg,
            R"(
            UPDATE cdc_catalog.apply_position
            SET kafka_offset = $1::bigint,
                last_applied_at = now(),
                apply_lag_seconds = 0,
                status = 'healthy'::cdc_catalog.cdc_health_status,
                last_error = NULL,
                updated_at = now()
            WHERE catalog_id = $2::bigint
            )",
            2,
            nullptr,
            upd_vals,
            nullptr,
            nullptr,
            0);
        if (!upd || PQresultStatus(upd) != PGRES_COMMAND_OK) {
            stats.errors += 1;
        }
        if (upd) {
            PQclear(upd);
        }
    }
#else
    (void)bootstrap;
    (void)consumer_group;
#endif

    if (stats.errors > 0) {
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "full-load kafka reset aborted: dedup preserved",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", tier},
                {"db_engine", db_engine},
                {"tables", stats.tables},
                {"topics_reset", stats.topics_reset},
                {"errors", stats.errors},
            },
        });
        return stats;
    }

    PGresult* dedup = PQexecParams(
        pg,
        R"(
        DELETE FROM cdc_catalog.cdc_applied_events ae
        USING cdc_catalog.catalog c
        WHERE ae.conn_id = $1
          AND c.conn_id = ae.conn_id
          AND c.source_schema = ae.source_schema
          AND c.source_table = ae.source_table
          AND c.conn_id = $1
          AND c.service_tier::text = lower($2)
          AND c.db_engine = $3::cdc_catalog.db_engine
          AND c.active = true
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (dedup && PQresultStatus(dedup) == PGRES_COMMAND_OK) {
        stats.dedup_deleted = std::atoll(PQcmdTuples(dedup));
    } else {
        stats.errors += 1;
    }
    if (dedup) {
        PQclear(dedup);
    }

    log_write(pg, {
        .level = stats.errors > 0 ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = stats.errors > 0 ? "full-load kafka reset completed with errors"
                                     : "full-load kafka reset completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tier", tier},
            {"db_engine", db_engine},
            {"tables", stats.tables},
            {"topics_reset", stats.topics_reset},
            {"dedup_deleted", stats.dedup_deleted},
            {"errors", stats.errors},
        },
    });

    return stats;
}

bool onboard_conn_after_full_load(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine,
    const std::string& batch_id) {
    enable_cdc_after_full_load(pg, conn_id, tier, db_engine, batch_id);
#ifdef HAVE_FREETDS
    if (db_engine == "mssql") {
        seed_mssql_cdc_lsn_for_conn(cfg, pg, conn_id, tier, batch_id);
    }
#endif
#ifdef HAVE_MONGOC
    if (db_engine == "mongodb") {
        seed_mongo_cdc_resume_for_conn(cfg, pg, conn_id, tier, batch_id);
    }
#endif
    const auto stats = reset_kafka_apply_after_full_load(pg, conn_id, tier, db_engine, batch_id);
    return stats.errors == 0;
}

#ifdef HAVE_RDKAFKA

long long kafka_backlog_messages(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", consumer_group.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));

    rd_kafka_t* consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        throw std::runtime_error(std::string("kafka consumer create failed: ") + errstr);
    }
    rd_kafka_poll_set_consumer(consumer);

    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(parts, topic.c_str(), partition);

    int64_t low = 0;
    int64_t high = 0;
    rd_kafka_resp_err_t err =
        rd_kafka_query_watermark_offsets(consumer, topic.c_str(), partition, &low, &high, 15000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(consumer);
        throw std::runtime_error(std::string("watermark query failed: ") + rd_kafka_err2str(err));
    }

    err = rd_kafka_committed(consumer, parts, 15000);
    long long pos = low;
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR && parts->cnt > 0) {
        const int64_t committed = parts->elems[0].offset;
        if (committed >= 0) {
            pos = committed;
        }
    }

    rd_kafka_topic_partition_list_destroy(parts);
    rd_kafka_destroy(consumer);
    return std::max<int64_t>(0, high - pos);
}

long long reset_apply_offset_to_end(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        throw std::runtime_error(std::string("kafka client create failed: ") + errstr);
    }

    int64_t low = 0;
    int64_t high = 0;
    rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(rk, topic.c_str(), partition, &low, &high, 15000);
    if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
        rd_kafka_destroy(rk);
        return -1;
    }
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(rk);
        throw std::runtime_error(std::string("watermark query failed: ") + rd_kafka_err2str(err));
    }

    const long long new_offset = std::max<int64_t>(0, high - 1);
    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(parts, topic.c_str(), partition)->offset = high;

    rd_kafka_AlterConsumerGroupOffsets_t* alter =
        rd_kafka_AlterConsumerGroupOffsets_new(consumer_group.c_str(), parts);
    rd_kafka_AdminOptions_t* options =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
    if (rd_kafka_AdminOptions_set_request_timeout(options, 30000, errstr, sizeof(errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(rk);
        throw std::runtime_error(std::string("admin options failed: ") + errstr);
    }

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_AlterConsumerGroupOffsets(rk, &alter, 1, options, queue);
    rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_topic_partition_list_destroy(parts);

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 30000);
    rd_kafka_queue_destroy(queue);
    rd_kafka_destroy(rk);

    if (!event) {
        throw std::runtime_error("kafka alter consumer group offsets timed out");
    }
    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error(std::string("kafka alter offsets failed: ") + msg);
    }

    const rd_kafka_AlterConsumerGroupOffsets_result_t* result =
        rd_kafka_event_AlterConsumerGroupOffsets_result(event);
    if (result) {
        size_t group_count = 0;
        const rd_kafka_group_result_t* const* groups =
            rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &group_count);
        for (size_t i = 0; i < group_count; ++i) {
            const rd_kafka_error_t* group_err = rd_kafka_group_result_error(groups[i]);
            if (group_err) {
                const std::string msg = rd_kafka_error_string(group_err);
                rd_kafka_event_destroy(event);
                throw std::runtime_error(std::string("kafka alter offsets group failed: ") + msg);
            }
        }
    }
    rd_kafka_event_destroy(event);

    return new_offset;
}

void ensure_kafka_topics_exist(
    const std::string& bootstrap,
    const std::vector<std::string>& topics,
    int partition_count,
    int replication_factor) {
    if (topics.empty() || partition_count <= 0) {
        return;
    }

    char errstr[512]{0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("kafka ensure topics conf failed: ") + errstr);
    }

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        throw std::runtime_error(std::string("kafka ensure topics client failed: ") + errstr);
    }

    std::vector<rd_kafka_NewTopic_t*> new_topics;
    new_topics.reserve(topics.size());
    for (const auto& topic : topics) {
        rd_kafka_NewTopic_t* nt =
            rd_kafka_NewTopic_new(topic.c_str(), partition_count, replication_factor, errstr, sizeof(errstr));
        if (!nt) {
            rd_kafka_NewTopic_destroy_array(new_topics.data(), new_topics.size());
            rd_kafka_destroy(rk);
            throw std::runtime_error(
                std::string("NewTopic failed for ") + topic + ": " + errstr);
        }
        new_topics.push_back(nt);
    }

    rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    rd_kafka_AdminOptions_set_request_timeout(options, 30000, errstr, sizeof(errstr));
    rd_kafka_AdminOptions_set_operation_timeout(options, 30000, errstr, sizeof(errstr));

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_CreateTopics(rk, new_topics.data(), new_topics.size(), options, queue);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_NewTopic_destroy_array(new_topics.data(), new_topics.size());

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 60000);
    rd_kafka_queue_destroy(queue);
    rd_kafka_destroy(rk);

    if (!event) {
        throw std::runtime_error("ensure_kafka_topics_exist: CreateTopics timed out");
    }

    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error("ensure_kafka_topics_exist: " + msg);
    }

    const rd_kafka_CreateTopics_result_t* result = rd_kafka_event_CreateTopics_result(event);
    if (result) {
        size_t res_cnt = 0;
        const rd_kafka_topic_result_t** res = rd_kafka_CreateTopics_result_topics(result, &res_cnt);
        for (size_t i = 0; i < res_cnt; ++i) {
            const rd_kafka_resp_err_t code = rd_kafka_topic_result_error(res[i]);
            if (code == RD_KAFKA_RESP_ERR_NO_ERROR || code == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                continue;
            }
            const char* msg = rd_kafka_topic_result_error_string(res[i]);
            rd_kafka_event_destroy(event);
            throw std::runtime_error(
                std::string("ensure_kafka_topics_exist: ") + (msg ? msg : rd_kafka_err2str(code)));
        }
    }
    rd_kafka_event_destroy(event);
}

#else

long long kafka_backlog_messages(
    const std::string&,
    const std::string&,
    const std::string&,
    int) {
    return 0;
}

long long reset_apply_offset_to_end(
    const std::string&,
    const std::string&,
    const std::string&,
    int) {
    return 0;
}

void ensure_kafka_topics_exist(
    const std::string&,
    const std::vector<std::string>&,
    int,
    int) {}

#endif
