#pragma once

#include <libpq-fe.h>

#include <string>

struct PipelineCatalogCounts {
    int total_tables{0};
    int cdc_ready{0};
    int pending_full_load{0};
    int failed_tables{0};
    int success_tables{0};
    int apply_healthy{0};
    int apply_lagging{0};
    int apply_quarantined{0};
    int max_apply_lag_seconds{0};
};

struct PipelineKafkaLagSummary {
    long long total{0};
    int partitions_with_lag{0};
    long long max_lag{0};
    std::string max_schema;
    std::string max_table;
};

struct PipelineApplySliceStats {
    long long events_seen{0};
    long long events_applied{0};
    int errors{0};
    std::string stop_reason;
    long long duration_ms{0};
};

/** Ensure one pipeline_health row exists per active catalog tier for conn. */
void ensure_pipeline_health_rows(PGconn* pg, const std::string& conn_id, const std::string& db_engine);

PipelineCatalogCounts fetch_pipeline_catalog_counts(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine);

#ifdef HAVE_RDKAFKA
struct rd_kafka_s;
typedef struct rd_kafka_s rd_kafka_t;

PipelineKafkaLagSummary compute_pipeline_kafka_lag(
    PGconn* pg,
    rd_kafka_t* rk,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine);
#endif

/** Refresh capture columns on all tier rows for conn (after capture slice). */
void update_pipeline_health_capture(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

void refresh_pipeline_health_live(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine,
    int throttle_seconds = 15,
    bool force = false);

/** Refresh apply/kafka/catalog columns for conn+tier (end-of-slice summary). */
void update_pipeline_health_apply(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& service_tier,
    const std::string& db_engine,
    const PipelineCatalogCounts& counts,
    const PipelineKafkaLagSummary& kafka_lag,
    const PipelineApplySliceStats& slice);

/** Recompute __TOTAL__ row for tier+engine from per-conn rows. */
void refresh_pipeline_health_totals(
    PGconn* pg,
    const std::string& service_tier,
    const std::string& db_engine);
