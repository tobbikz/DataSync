#include "pipeline_health.hpp"

#include "kafka_lag.hpp"

#include <libpq-fe.h>

#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

constexpr const char* kTotalConnId = "__TOTAL__";

namespace {

void exec_params_or_throw(PGconn* pg, const char* sql, int n, const char* const* vals) {
    PGresult* res = PQexecParams(pg, sql, n, nullptr, vals, nullptr, nullptr, 0);
    if (!res) {
        throw std::runtime_error("pipeline_health: query failed (no result)");
    }
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error("pipeline_health: " + err);
    }
    PQclear(res);
}

PipelineCatalogCounts read_catalog_counts(PGresult* res) {
    PipelineCatalogCounts out;
    if (!res || PQntuples(res) < 1) {
        return out;
    }
    out.total_tables = std::atoi(PQgetvalue(res, 0, 0));
    out.cdc_ready = std::atoi(PQgetvalue(res, 0, 1));
    out.pending_full_load = std::atoi(PQgetvalue(res, 0, 2));
    out.failed_tables = std::atoi(PQgetvalue(res, 0, 3));
    out.success_tables = std::atoi(PQgetvalue(res, 0, 4));
    out.apply_healthy = std::atoi(PQgetvalue(res, 0, 5));
    out.apply_lagging = std::atoi(PQgetvalue(res, 0, 6));
    out.apply_quarantined = std::atoi(PQgetvalue(res, 0, 7));
    out.max_apply_lag_seconds = std::atoi(PQgetvalue(res, 0, 8));
    return out;
}

const char* opt_or_null(const std::string& value) {
    return value.empty() ? nullptr : value.c_str();
}

std::mutex g_pipeline_health_mu;
std::map<std::string, std::chrono::steady_clock::time_point> g_pipeline_health_last;

bool pipeline_health_should_throttle(
    const std::string& conn_id,
    const std::string& service_tier,
    int throttle_seconds,
    bool force) {
    if (force || throttle_seconds <= 0) {
        return false;
    }
    const std::string key = conn_id + "|" + service_tier;
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(g_pipeline_health_mu);
    const auto it = g_pipeline_health_last.find(key);
    if (it != g_pipeline_health_last.end()) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count();
        if (elapsed < throttle_seconds) {
            return true;
        }
    }
    g_pipeline_health_last[key] = now;
    return false;
}

void exec_refresh_sql(PGconn* pg, const char* sql, int n, const char* const* vals) {
    PGresult* res = PQexecParams(pg, sql, n, nullptr, vals, nullptr, nullptr, 0);
    if (!res) {
        throw std::runtime_error("pipeline_health refresh failed (no result)");
    }
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error("pipeline_health refresh: " + err);
    }
    PQclear(res);
}

}  // namespace

void refresh_pipeline_health_live(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine,
    int throttle_seconds,
    bool force) {
    if (!pg || PQstatus(pg) != CONNECTION_OK || conn_id.empty() || service_tier.empty() ||
        conn_id == kTotalConnId) {
        return;
    }
    if (pipeline_health_should_throttle(conn_id, service_tier, throttle_seconds, force)) {
        return;
    }

    const char* vals[] = {conn_id.c_str(), service_tier.c_str(), db_engine.c_str()};
    exec_refresh_sql(
        pg,
        "SELECT cdc_catalog.refresh_pipeline_health_live($1, $2::cdc_catalog.service_tier, $3::cdc_catalog.db_engine)",
        3,
        vals);
    refresh_pipeline_health_totals(pg, service_tier, db_engine);
}

void ensure_pipeline_health_rows(PGconn* pg, const std::string& conn_id, const std::string& db_engine) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return;
    }
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    exec_params_or_throw(
        pg,
        R"(
        INSERT INTO cdc_catalog.pipeline_health (conn_id, service_tier, db_engine)
        SELECT DISTINCT c.conn_id, c.service_tier, c.db_engine
        FROM cdc_catalog.catalog c
        WHERE c.conn_id = $1
          AND c.db_engine = $2::cdc_catalog.db_engine
          AND c.active = true
        ON CONFLICT (conn_id, service_tier, db_engine) DO NOTHING
        )",
        2,
        vals);
}

PipelineCatalogCounts fetch_pipeline_catalog_counts(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine) {
    PipelineCatalogCounts out;
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return out;
    }
    const char* vals[] = {conn_id.c_str(), service_tier.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            count(*)::integer AS total_tables,
            count(*) FILTER (
                WHERE c.active AND c.cdc_enabled AND NOT c.needs_full_load
            )::integer AS cdc_ready,
            count(*) FILTER (WHERE c.needs_full_load AND c.active)::integer AS pending_full_load,
            count(*) FILTER (WHERE c.status = 'failed'::cdc_catalog.replication_status)::integer AS failed_tables,
            count(*) FILTER (WHERE c.status = 'success'::cdc_catalog.replication_status)::integer AS success_tables,
            count(*) FILTER (
                WHERE ap.status = 'healthy'::cdc_catalog.cdc_health_status
            )::integer AS apply_healthy,
            count(*) FILTER (
                WHERE ap.status = ANY (
                    ARRAY[
                        'stale'::cdc_catalog.cdc_health_status,
                        'lagging'::cdc_catalog.cdc_health_status
                    ]
                )
            )::integer AS apply_lagging,
            count(*) FILTER (
                WHERE ap.status = 'quarantined'::cdc_catalog.cdc_health_status
            )::integer AS apply_quarantined,
            coalesce(max(ap.apply_lag_seconds), 0)::integer AS max_apply_lag_seconds
        FROM cdc_catalog.catalog c
        LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.conn_id = $1
          AND c.service_tier = $2::cdc_catalog.service_tier
          AND c.db_engine = $3::cdc_catalog.db_engine
          AND c.active = true
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
        return out;
    }
    out = read_catalog_counts(res);
    PQclear(res);
    return out;
}

#ifdef HAVE_RDKAFKA

PipelineKafkaLagSummary compute_pipeline_kafka_lag(
    PGconn* pg,
    rd_kafka_t* rk,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine) {
    PipelineKafkaLagSummary out;
    if (!pg || PQstatus(pg) != CONNECTION_OK || !rk) {
        return out;
    }

    const char* vals[] = {conn_id.c_str(), service_tier.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            c.source_schema,
            c.source_table,
            ap.kafka_topic,
            ap.kafka_partition,
            ap.kafka_offset
        FROM cdc_catalog.catalog c
        JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.conn_id = $1
          AND c.service_tier = $2::cdc_catalog.service_tier
          AND c.db_engine = $3::cdc_catalog.db_engine
          AND c.active = true
          AND c.cdc_enabled = true
          AND NOT c.needs_full_load
          AND ap.kafka_topic <> ''
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
        return out;
    }

    std::map<std::pair<std::string, int>, long long> partition_lag;
    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string schema = PQgetvalue(res, i, 0);
        const std::string table = PQgetvalue(res, i, 1);
        const std::string topic = PQgetvalue(res, i, 2);
        const int partition = std::atoi(PQgetvalue(res, i, 3));
        const long long offset = std::atoll(PQgetvalue(res, i, 4));

        const auto key = std::make_pair(topic, partition);
        long long lag = 0;
        const auto it = partition_lag.find(key);
        if (it == partition_lag.end()) {
            lag = compute_kafka_consumer_lag(rk, topic, partition, offset);
            if (lag < 0) {
                lag = 0;
            }
            partition_lag.emplace(key, lag);
        } else {
            lag = it->second;
        }

        if (lag > out.max_lag) {
            out.max_lag = lag;
            out.max_schema = schema;
            out.max_table = table;
        }
    }
    PQclear(res);

    for (const auto& [key, lag] : partition_lag) {
        (void)key;
        out.total += lag;
        if (lag > 0) {
            out.partitions_with_lag += 1;
        }
    }
    return out;
}

#endif

void update_pipeline_health_capture(PGconn* pg, const std::string& conn_id, const std::string& db_engine) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return;
    }
    ensure_pipeline_health_rows(pg, conn_id, db_engine);

    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT DISTINCT c.service_tier::text
        FROM cdc_catalog.catalog c
        WHERE c.conn_id = $1
          AND c.db_engine = $2::cdc_catalog.db_engine
          AND c.active = true
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
        return;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        const char* tier = PQgetvalue(res, i, 0);
        if (!tier || !tier[0]) {
            continue;
        }
        try {
            refresh_pipeline_health_live(pg, conn_id, tier, db_engine, 0, true);
        } catch (...) {
        }
    }
    PQclear(res);
}

void update_pipeline_health_apply(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine,
    const PipelineCatalogCounts& counts,
    const PipelineKafkaLagSummary& kafka_lag,
    const PipelineApplySliceStats& slice) {
    (void)counts;
    (void)kafka_lag;
    if (!pg || PQstatus(pg) != CONNECTION_OK || service_tier.empty()) {
        return;
    }
    try {
        refresh_pipeline_health_live(pg, conn_id, service_tier, db_engine, 0, true);
    } catch (...) {
    }

    const std::string events_seen = std::to_string(slice.events_seen);
    const std::string events_applied = std::to_string(slice.events_applied);
    const std::string slice_errors = std::to_string(slice.errors);
    const std::string duration_ms = std::to_string(slice.duration_ms);

    const char* vals[] = {
        conn_id.c_str(),
        service_tier.c_str(),
        db_engine.c_str(),
        events_seen.c_str(),
        events_applied.c_str(),
        slice_errors.c_str(),
        opt_or_null(slice.stop_reason),
        duration_ms.c_str(),
    };

    exec_params_or_throw(
        pg,
        R"(
        UPDATE cdc_catalog.pipeline_health
        SET
            last_slice_events_seen = $4::bigint,
            last_slice_events_applied = $5::bigint,
            last_slice_errors = $6::integer,
            last_slice_stop_reason = $7,
            last_slice_duration_ms = $8::bigint,
            updated_at = now(),
            updated_by = 'apply'
        WHERE conn_id = $1
          AND service_tier = $2::cdc_catalog.service_tier
          AND db_engine = $3::cdc_catalog.db_engine
        )",
        8,
        vals);
}

void refresh_pipeline_health_totals(
    PGconn* pg,
    const std::string& service_tier,
    const std::string& db_engine) {
    if (!pg || PQstatus(pg) != CONNECTION_OK || service_tier.empty()) {
        return;
    }

    const char* vals[] = {service_tier.c_str(), db_engine.c_str()};
    exec_refresh_sql(
        pg,
        "SELECT cdc_catalog.refresh_pipeline_health_totals($1::cdc_catalog.service_tier, $2::cdc_catalog.db_engine)",
        2,
        vals);
}
