#include "cdc_catchup.hpp"

#include "capture_common.hpp"
#include "config.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_full_load.hpp"
#include "mariadb_schema.hpp"
#include "mongo_full_load.hpp"
#include "mssql_full_load.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "runtime_config.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#include "mssql_kafka_capture.hpp"
#endif

#include <algorithm>
#include <chrono>
#include <optional>
#include <set>
#include <sstream>

namespace {

struct ApplyPositionRow {
    long long catalog_id{0};
    std::string kafka_topic;
    int kafka_partition{0};
    std::string service_tier;
    std::string source_database;
    nlohmann::json engine_meta = nlohmann::json::object();
};

int table_lag_seconds(PGconn* pg, const char* last_applied, int lag_col, int default_stale) {
    (void)pg;
    if (lag_col >= 0) {
        return std::max(0, lag_col);
    }
    if (!last_applied || !*last_applied) {
        return default_stale + 1;
    }
    return default_stale + 1;
}

ApplyPositionRow fetch_apply_position(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table) {
    const char* vals[] = {conn_id.c_str(), schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT ap.catalog_id, ap.kafka_topic, ap.kafka_partition, c.service_tier::text,
               c.source_database, c.engine_meta::text
        FROM cdc_catalog.apply_position ap
        JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
        WHERE ap.conn_id = $1 AND ap.source_schema = $2 AND ap.source_table = $3
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    ApplyPositionRow out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("no apply_position for " + schema + "." + table);
    }
    out.catalog_id = std::atoll(PQgetvalue(res, 0, 0));
    out.kafka_topic = PQgetvalue(res, 0, 1);
    out.kafka_partition = std::stoi(PQgetvalue(res, 0, 2));
    out.service_tier = PQgetvalue(res, 0, 3);
    out.source_database = PQgetvalue(res, 0, 4);
    const char* meta = PQgetvalue(res, 0, 5);
    if (meta && *meta) {
        out.engine_meta = nlohmann::json::parse(meta, nullptr, false);
        if (out.engine_meta.is_discarded()) {
            out.engine_meta = nlohmann::json::object();
        }
    }
    PQclear(res);
    return out;
}

void update_apply_position_after_catchup(
    PGconn* pg,
    long long catalog_id,
    long long new_offset,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table) {
    const std::string offset_str = std::to_string(new_offset);
    const char* vals1[] = {offset_str.c_str(), std::to_string(catalog_id).c_str()};
    pg_exec_params_simple(
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
        vals1);

    const char* vals2[] = {conn_id.c_str(), schema.c_str(), table.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        DELETE FROM cdc_catalog.cdc_applied_events
        WHERE conn_id = $1 AND source_schema = $2 AND source_table = $3
        )",
        3,
        vals2);
}

CatchupResult run_table_catchup(
    const AppConfig& cfg,
    PGconn* log_pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table,
    const std::optional<std::string>& tier,
    const std::string& batch_id,
    const std::string& db_engine) {
    CatchupResult result;
    result.payload = {
        {"batch_id", batch_id},
        {"conn_id", conn_id},
        {"source_schema", schema},
        {"source_table", table},
        {"action", "catchup_full_load"},
        {"db_engine", db_engine},
    };

    runtime.reload(log_pg);
    const std::string bootstrap = resolve_kafka_bootstrap(runtime, conn_id).bootstrap;

    const ApplyPositionRow pos = fetch_apply_position(log_pg, conn_id, schema, table);
    const std::string svc_tier = tier && !tier->empty() ? *tier : pos.service_tier;
    const std::string consumer_group = kafka_apply_consumer_group(runtime, log_pg, conn_id, svc_tier);

#ifdef HAVE_RDKAFKA
    const long long backlog_before =
        kafka_backlog_messages(bootstrap, consumer_group, pos.kafka_topic, pos.kafka_partition);
#else
    const long long backlog_before = 0;
#endif
    result.payload["kafka_backlog_before"] = backlog_before;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = db_engine == "mssql" ? "catchup mssql full-load started"
                    : db_engine == "mongodb" ? "catchup mongo full-load started"
                                             : "catchup full-load started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = schema,
        .source_table = table,
        .context = {{"kafka_backlog", backlog_before}, {"tier", svc_tier}, {"db_engine", db_engine}},
    });

    flag_table_for_full_load(log_pg, conn_id, schema, table, db_engine);

    if (db_engine == "mssql") {
        run_mssql_full_load(cfg, log_pg, batch_id, svc_tier);
    } else if (db_engine == "mongodb") {
        run_mongo_full_load(cfg, log_pg, batch_id, svc_tier);
    } else {
        run_mariadb_full_load(cfg, log_pg, batch_id, svc_tier);
    }

#ifdef HAVE_RDKAFKA
    const int topic_partitions = (db_engine == "mssql")
        ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mssql_capture", conn_id)
        : (db_engine == "mongodb")
            ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mongo_capture", conn_id)
            : runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_capture", conn_id);
    long long new_offset = 0;
    for (int partition = 0; partition < topic_partitions; ++partition) {
        const long long off =
            reset_apply_offset_to_end(bootstrap, consumer_group, pos.kafka_topic, partition);
        if (partition == pos.kafka_partition) {
            new_offset = off;
        }
    }
#else
    const long long new_offset = 0;
#endif

    update_apply_position_after_catchup(log_pg, pos.catalog_id, new_offset, conn_id, schema, table);
    enable_cdc_after_full_load(log_pg, conn_id, svc_tier, db_engine, batch_id);

#ifdef HAVE_RDKAFKA
    const long long backlog_after =
        kafka_backlog_messages(bootstrap, consumer_group, pos.kafka_topic, pos.kafka_partition);
#else
    const long long backlog_after = 0;
#endif

    result.payload["kafka_backlog_after"] = backlog_after;
    result.payload["kafka_offset"] = new_offset;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = db_engine == "mssql" ? "catchup mssql full-load completed"
                    : db_engine == "mongodb" ? "catchup mongo full-load completed"
                                             : "catchup full-load completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = schema,
        .source_table = table,
        .context = {
            {"kafka_backlog_before", backlog_before},
            {"kafka_backlog_after", backlog_after},
            {"kafka_offset", new_offset},
            {"db_engine", db_engine},
        },
    });

    return result;
}

}  // namespace

std::vector<CatchupCandidate> find_catchup_candidates(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id) {
    (void)cfg;
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int default_stale = runtime.get_int("apply_max_table_staleness_seconds", 900, "cdc_kafka_apply", conn_id);
    const int lag_seconds_threshold = runtime.get_int("apply_catchup_lag_seconds", 300, "cdc_kafka_apply", conn_id);
    const int kafka_lag_threshold = runtime.get_int("apply_catchup_kafka_messages", 500000, "cdc_kafka_apply", conn_id);
    const std::string bootstrap = resolve_kafka_bootstrap(runtime, conn_id).bootstrap;

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        log_pg,
        R"(
        SELECT c.source_schema, c.source_table, ap.last_applied_at::text,
               ap.apply_lag_seconds, ap.status::text, ap.kafka_topic, ap.kafka_partition,
               c.service_tier::text
        FROM cdc_catalog.catalog c
        JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.conn_id = $1
          AND c.active = true
          AND c.cdc_enabled = true
        ORDER BY c.source_schema, c.source_table
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);

    std::vector<CatchupCandidate> candidates;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return candidates;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        const char* status = PQgetvalue(res, i, 4);
        if (status && std::string(status) == "quarantined") {
            continue;
        }
        const std::string schema = PQgetvalue(res, i, 0);
        const std::string table = PQgetvalue(res, i, 1);
        const int lag_col = PQgetisnull(res, i, 3) ? -1 : std::stoi(PQgetvalue(res, i, 3));
        const int lag_s = table_lag_seconds(log_pg, PQgetvalue(res, i, 2), lag_col, default_stale);
        const std::string topic = PQgetvalue(res, i, 5);
        const int partition = std::stoi(PQgetvalue(res, i, 6));
        const std::string table_tier = PQgetvalue(res, i, 7);
        const std::string consumer_group = kafka_apply_consumer_group(runtime, log_pg, conn_id, table_tier);

#ifdef HAVE_RDKAFKA
        long long kafka_lag = 0;
        try {
            kafka_lag = kafka_backlog_messages(bootstrap, consumer_group, topic, partition);
        } catch (const std::exception&) {
            kafka_lag = 0;
        }
#else
        const long long kafka_lag = 0;
#endif

        if (lag_s >= lag_seconds_threshold || kafka_lag >= kafka_lag_threshold) {
            candidates.push_back({schema, table, lag_s, kafka_lag});
        }
    }
    PQclear(res);

    std::sort(candidates.begin(), candidates.end(), [](const CatchupCandidate& a, const CatchupCandidate& b) {
        if (a.kafka_backlog != b.kafka_backlog) {
            return a.kafka_backlog > b.kafka_backlog;
        }
        return a.lag_s > b.lag_s;
    });
    return candidates;
}

std::vector<CatchupResult> run_catchup_if_needed(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    const std::string& batch_id) {
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    if (!runtime.get_bool("apply_catchup_enabled", true, "cdc_kafka_apply", conn_id)) {
        return {};
    }
    const int max_tables = runtime.get_int("apply_catchup_max_tables", 1, "cdc_kafka_apply", conn_id);
    const std::string db_engine = conn_engine(cfg, conn_id);

    auto candidates = find_catchup_candidates(cfg, log_pg, conn_id);
    if (tier && !tier->empty()) {
        const char* vals[] = {conn_id.c_str(), tier->c_str()};
        PGresult* res = PQexecParams(
            log_pg,
            R"(
            SELECT source_schema, source_table FROM cdc_catalog.catalog
            WHERE conn_id = $1 AND service_tier::text = lower($2) AND cdc_enabled = true
            )",
            2,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        std::set<std::pair<std::string, std::string>> tier_keys;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
            for (int i = 0; i < PQntuples(res); ++i) {
                tier_keys.emplace(PQgetvalue(res, i, 0), PQgetvalue(res, i, 1));
            }
        }
        if (res) {
            PQclear(res);
        }
        std::vector<CatchupCandidate> filtered;
        for (const auto& cand : candidates) {
            if (tier_keys.count({cand.source_schema, cand.source_table})) {
                filtered.push_back(cand);
            }
        }
        candidates = std::move(filtered);
    }

    std::vector<CatchupResult> results;
    const int limit = std::min<int>(max_tables, static_cast<int>(candidates.size()));
    for (int i = 0; i < limit; ++i) {
        const auto& cand = candidates[static_cast<std::size_t>(i)];
        try {
            results.push_back(run_table_catchup(
                cfg,
                log_pg,
                runtime,
                conn_id,
                cand.source_schema,
                cand.source_table,
                tier,
                batch_id,
                db_engine));
        } catch (const std::exception& ex) {
            CatchupResult failed;
            failed.error = ex.what();
            failed.payload = {
                {"source_schema", cand.source_schema},
                {"source_table", cand.source_table},
                {"error", ex.what()},
            };
            results.push_back(std::move(failed));
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_apply",
                .message = "catchup full-load failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = cand.source_schema,
                .source_table = cand.source_table,
                .context = {{"error", std::string(ex.what()).substr(0, 500)}},
            });
        }
    }
    return results;
}
