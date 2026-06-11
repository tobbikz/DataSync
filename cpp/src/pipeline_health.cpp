#include "pipeline_health.hpp"

#include "kafka_lag.hpp"

#include <libpq-fe.h>

#include <map>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

constexpr const char* kTotalConnId = "__TOTAL__";

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

}  // namespace

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
    exec_params_or_throw(
        pg,
        R"(
        UPDATE cdc_catalog.pipeline_health ph
        SET
            capture_lag_seconds = cp.capture_lag_seconds,
            capture_status = cp.status,
            binlog_file = cp.binlog_file,
            binlog_position = cp.binlog_position,
            last_capture_at = cp.last_event_ts,
            updated_at = now(),
            updated_by = 'capture'
        FROM cdc_catalog.capture_position cp
        WHERE ph.conn_id = $1
          AND ph.db_engine = $2::cdc_catalog.db_engine
          AND cp.conn_id = $1
        )",
        2,
        vals);
}

void update_pipeline_health_apply(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine,
    const PipelineCatalogCounts& counts,
    const PipelineKafkaLagSummary& kafka_lag,
    const PipelineApplySliceStats& slice) {
    if (!pg || PQstatus(pg) != CONNECTION_OK || service_tier.empty()) {
        return;
    }
    ensure_pipeline_health_rows(pg, conn_id, db_engine);

    const std::string total_tables = std::to_string(counts.total_tables);
    const std::string cdc_ready = std::to_string(counts.cdc_ready);
    const std::string pending_full_load = std::to_string(counts.pending_full_load);
    const std::string failed_tables = std::to_string(counts.failed_tables);
    const std::string success_tables = std::to_string(counts.success_tables);
    const std::string apply_healthy = std::to_string(counts.apply_healthy);
    const std::string apply_lagging = std::to_string(counts.apply_lagging);
    const std::string apply_quarantined = std::to_string(counts.apply_quarantined);
    const std::string max_apply_lag = std::to_string(counts.max_apply_lag_seconds);
    const std::string kafka_total = std::to_string(kafka_lag.total);
    const std::string kafka_parts = std::to_string(kafka_lag.partitions_with_lag);
    const std::string max_kafka = std::to_string(kafka_lag.max_lag);
    const std::string events_seen = std::to_string(slice.events_seen);
    const std::string events_applied = std::to_string(slice.events_applied);
    const std::string slice_errors = std::to_string(slice.errors);
    const std::string duration_ms = std::to_string(slice.duration_ms);

    const char* vals[] = {
        conn_id.c_str(),
        service_tier.c_str(),
        db_engine.c_str(),
        total_tables.c_str(),
        cdc_ready.c_str(),
        pending_full_load.c_str(),
        failed_tables.c_str(),
        success_tables.c_str(),
        apply_healthy.c_str(),
        apply_lagging.c_str(),
        apply_quarantined.c_str(),
        max_apply_lag.c_str(),
        kafka_total.c_str(),
        kafka_parts.c_str(),
        max_kafka.c_str(),
        opt_or_null(kafka_lag.max_schema),
        opt_or_null(kafka_lag.max_table),
        events_seen.c_str(),
        events_applied.c_str(),
        slice_errors.c_str(),
        opt_or_null(slice.stop_reason),
        duration_ms.c_str(),
    };

    exec_params_or_throw(
        pg,
        R"(
        INSERT INTO cdc_catalog.pipeline_health (
            conn_id, service_tier, db_engine,
            total_tables, cdc_ready, pending_full_load, failed_tables, success_tables,
            apply_healthy, apply_lagging, apply_quarantined, max_apply_lag_seconds,
            kafka_lag_total, kafka_partitions_with_lag, max_kafka_lag,
            max_kafka_lag_schema, max_kafka_lag_table,
            last_apply_at, last_slice_events_seen, last_slice_events_applied,
            last_slice_errors, last_slice_stop_reason, last_slice_duration_ms,
            updated_at, updated_by
        ) VALUES (
            $1, $2::cdc_catalog.service_tier, $3::cdc_catalog.db_engine,
            $4::integer, $5::integer, $6::integer, $7::integer, $8::integer,
            $9::integer, $10::integer, $11::integer, $12::integer,
            $13::bigint, $14::integer, $15::bigint,
            $16, $17,
            now(), $18::bigint, $19::bigint,
            $20::integer, $21, $22::bigint,
            now(), 'apply'
        )
        ON CONFLICT (conn_id, service_tier, db_engine) DO UPDATE SET
            total_tables = EXCLUDED.total_tables,
            cdc_ready = EXCLUDED.cdc_ready,
            pending_full_load = EXCLUDED.pending_full_load,
            failed_tables = EXCLUDED.failed_tables,
            success_tables = EXCLUDED.success_tables,
            apply_healthy = EXCLUDED.apply_healthy,
            apply_lagging = EXCLUDED.apply_lagging,
            apply_quarantined = EXCLUDED.apply_quarantined,
            max_apply_lag_seconds = EXCLUDED.max_apply_lag_seconds,
            kafka_lag_total = EXCLUDED.kafka_lag_total,
            kafka_partitions_with_lag = EXCLUDED.kafka_partitions_with_lag,
            max_kafka_lag = EXCLUDED.max_kafka_lag,
            max_kafka_lag_schema = EXCLUDED.max_kafka_lag_schema,
            max_kafka_lag_table = EXCLUDED.max_kafka_lag_table,
            last_apply_at = EXCLUDED.last_apply_at,
            last_slice_events_seen = EXCLUDED.last_slice_events_seen,
            last_slice_events_applied = EXCLUDED.last_slice_events_applied,
            last_slice_errors = EXCLUDED.last_slice_errors,
            last_slice_stop_reason = EXCLUDED.last_slice_stop_reason,
            last_slice_duration_ms = EXCLUDED.last_slice_duration_ms,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
        )",
        22,
        vals);
}

void refresh_pipeline_health_totals(
    PGconn* pg,
    const std::string& service_tier,
    const std::string& db_engine) {
    if (!pg || PQstatus(pg) != CONNECTION_OK || service_tier.empty()) {
        return;
    }

    const char* vals[] = {service_tier.c_str(), db_engine.c_str(), kTotalConnId};
    exec_params_or_throw(
        pg,
        R"(
        INSERT INTO cdc_catalog.pipeline_health (
            conn_id, service_tier, db_engine,
            capture_lag_seconds, capture_status,
            kafka_lag_total, kafka_partitions_with_lag, max_kafka_lag,
            max_kafka_lag_schema, max_kafka_lag_table,
            total_tables, cdc_ready, pending_full_load, failed_tables, success_tables,
            apply_healthy, apply_lagging, apply_quarantined, max_apply_lag_seconds,
            last_capture_at, last_apply_at,
            last_slice_events_seen, last_slice_events_applied,
            last_slice_errors, last_slice_stop_reason, last_slice_duration_ms,
            updated_at, updated_by
        )
        SELECT
            $3,
            $1::cdc_catalog.service_tier,
            $2::cdc_catalog.db_engine,
            coalesce(max(ph.capture_lag_seconds), 0),
            CASE
                WHEN count(*) FILTER (WHERE ph.capture_status <> 'healthy'::cdc_catalog.cdc_health_status) > 0
                    THEN 'lagging'::cdc_catalog.cdc_health_status
                ELSE 'healthy'::cdc_catalog.cdc_health_status
            END,
            coalesce(sum(ph.kafka_lag_total), 0),
            coalesce(sum(ph.kafka_partitions_with_lag), 0),
            coalesce(max(ph.max_kafka_lag), 0),
            (
                SELECT ph2.max_kafka_lag_schema
                FROM cdc_catalog.pipeline_health ph2
                WHERE ph2.service_tier = $1::cdc_catalog.service_tier
                  AND ph2.db_engine = $2::cdc_catalog.db_engine
                  AND ph2.conn_id <> $3
                ORDER BY ph2.max_kafka_lag DESC NULLS LAST, ph2.conn_id
                LIMIT 1
            ),
            (
                SELECT ph2.max_kafka_lag_table
                FROM cdc_catalog.pipeline_health ph2
                WHERE ph2.service_tier = $1::cdc_catalog.service_tier
                  AND ph2.db_engine = $2::cdc_catalog.db_engine
                  AND ph2.conn_id <> $3
                ORDER BY ph2.max_kafka_lag DESC NULLS LAST, ph2.conn_id
                LIMIT 1
            ),
            coalesce(sum(ph.total_tables), 0),
            coalesce(sum(ph.cdc_ready), 0),
            coalesce(sum(ph.pending_full_load), 0),
            coalesce(sum(ph.failed_tables), 0),
            coalesce(sum(ph.success_tables), 0),
            coalesce(sum(ph.apply_healthy), 0),
            coalesce(sum(ph.apply_lagging), 0),
            coalesce(sum(ph.apply_quarantined), 0),
            coalesce(max(ph.max_apply_lag_seconds), 0),
            max(ph.last_capture_at),
            max(ph.last_apply_at),
            coalesce(sum(ph.last_slice_events_seen), 0),
            coalesce(sum(ph.last_slice_events_applied), 0),
            coalesce(sum(ph.last_slice_errors), 0),
            'aggregate',
            coalesce(sum(ph.last_slice_duration_ms), 0),
            now(),
            'daemon'
        FROM cdc_catalog.pipeline_health ph
        WHERE ph.service_tier = $1::cdc_catalog.service_tier
          AND ph.db_engine = $2::cdc_catalog.db_engine
          AND ph.conn_id <> $3
        ON CONFLICT (conn_id, service_tier, db_engine) DO UPDATE SET
            capture_lag_seconds = EXCLUDED.capture_lag_seconds,
            capture_status = EXCLUDED.capture_status,
            kafka_lag_total = EXCLUDED.kafka_lag_total,
            kafka_partitions_with_lag = EXCLUDED.kafka_partitions_with_lag,
            max_kafka_lag = EXCLUDED.max_kafka_lag,
            max_kafka_lag_schema = EXCLUDED.max_kafka_lag_schema,
            max_kafka_lag_table = EXCLUDED.max_kafka_lag_table,
            total_tables = EXCLUDED.total_tables,
            cdc_ready = EXCLUDED.cdc_ready,
            pending_full_load = EXCLUDED.pending_full_load,
            failed_tables = EXCLUDED.failed_tables,
            success_tables = EXCLUDED.success_tables,
            apply_healthy = EXCLUDED.apply_healthy,
            apply_lagging = EXCLUDED.apply_lagging,
            apply_quarantined = EXCLUDED.apply_quarantined,
            max_apply_lag_seconds = EXCLUDED.max_apply_lag_seconds,
            last_capture_at = EXCLUDED.last_capture_at,
            last_apply_at = EXCLUDED.last_apply_at,
            last_slice_events_seen = EXCLUDED.last_slice_events_seen,
            last_slice_events_applied = EXCLUDED.last_slice_events_applied,
            last_slice_errors = EXCLUDED.last_slice_errors,
            last_slice_stop_reason = EXCLUDED.last_slice_stop_reason,
            last_slice_duration_ms = EXCLUDED.last_slice_duration_ms,
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by
        )",
        3,
        vals);
}
