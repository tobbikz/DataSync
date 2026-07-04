#pragma once

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>
#include <vector>

struct CatalogReconcileRow {
    long long catalog_id{0};
    std::string conn_id;
    std::string db_engine;
    std::string source_database;
    std::string source_schema;
    std::string source_table;
    std::string lake_schema;
    std::string lake_table;
    std::string pk_columns;
    std::string catalog_status;
    std::string last_error;
    std::string engine_meta_json;
    bool active{true};
    bool cdc_enabled{true};
    bool needs_full_load{false};
    bool has_pk{true};
    bool hot{false};
};

struct PrevReconcileSnapshot {
    long long row_count_delta{0};
    std::string status;
    std::string drift_kind;
};

struct ApplyEventsAggregate {
    long long inserts{0};
    long long updates{0};
    long long deletes{0};
    long long total{0};
    bool has_flow{false};
};

struct FreshnessInput {
    std::optional<std::string> watermark_column;
    std::optional<std::string> source_max_ts;
    std::optional<std::string> lake_max_ts;
};

struct FreshnessSnapshot {
    bool computed{false};
    std::optional<std::string> watermark_column;
    std::optional<std::string> source_max_ts;
    std::optional<std::string> lake_max_ts;
    double lag_minutes{0.0};
    std::string status{"skip"};
};

struct SchemaDriftSnapshot {
    int source_column_count{-1};
    int lake_column_count{-1};
    bool match{false};
    std::string status{"skip"};
};

struct TierDecision {
    std::string tier{"standard"};
    bool skip_row_count{false};
    std::string skip_reason;
};

struct EnrichmentBundle {
    std::optional<PrevReconcileSnapshot> prev;
    ApplyEventsAggregate events_7d;
    FreshnessSnapshot freshness;
    SchemaDriftSnapshot schema;
    TierDecision tier;
    bool timing_skew_suspected{false};
    bool suggest_full_load{false};
    bool quarantined{false};
    std::string recommended_action;
    std::string root_cause_label;
    std::string semaphore_reason;
};

namespace reconcile_enrichments {

std::optional<std::string> resolve_watermark_column(PGconn* log_pg, const CatalogReconcileRow& row);

FreshnessSnapshot compute_freshness_lag_sql(
    PGconn* lake_pg,
    const std::optional<std::string>& source_max,
    const std::optional<std::string>& lake_max,
    const std::optional<std::string>& watermark,
    int warn_minutes,
    int fail_minutes);

SchemaDriftSnapshot compute_schema_drift_lake_only(PGconn* lake_pg, const CatalogReconcileRow& row);

void set_schema_source_count(SchemaDriftSnapshot& drift, int source_columns);

std::optional<PrevReconcileSnapshot> fetch_prev_reconcile_snapshot(PGconn* pg, long long catalog_id);

ApplyEventsAggregate fetch_apply_events_aggregate(PGconn* pg, long long catalog_id, int lookback_seconds);

TierDecision eval_reconcile_tier(
    const CatalogReconcileRow& row,
    bool apply_inactive,
    bool has_recent_flow,
    const std::optional<PrevReconcileSnapshot>& prev);

bool eval_timing_skew_suspected(
    bool has_recent_apply_flow,
    long long abs_delta,
    int row_warn_abs_tolerance,
    long long source_rows);

EnrichmentBundle build_enrichment_bundle(
    PGconn* log_pg,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int row_warn_abs_tolerance,
    int freshness_warn_minutes,
    int freshness_fail_minutes,
    int events_lookback_seconds,
    const FreshnessInput& freshness_in,
    long long source_rows,
    long long lake_rows,
    const std::string& row_status,
    const std::string& drift_kind,
    const std::string& lag_status,
    const std::string& apply_status,
    const std::string& overall,
    bool apply_inactive,
    bool static_gap,
    bool catalog_has_error);

std::string derive_recommended_action(
    const std::string& overall,
    const std::string& row_status,
    const std::string& drift_kind,
    long long abs_delta,
    bool suggest_full_load,
    bool quarantined);

std::string derive_root_cause_label(
    const std::string& overall,
    const std::string& row_status,
    const std::string& drift_kind,
    const std::string& semaphore_reason,
    bool static_gap,
    bool timing_skew_suspected,
    bool quarantined,
    const std::string& event_loss_status = "ok",
    const std::string& slice_stale_status = "ok");

/** Latest apply_batch_stats slice: parse/drop loss and slice staleness. */
std::string eval_event_loss_status(long long parse_skipped, long long dropped_unrecoverable);

std::string eval_slice_stale_status(
    bool slice_is_stale,
    const std::string& apply_status,
    const std::string& lag_status);

void apply_enrichment_to_checks(nlohmann::json& checks, const EnrichmentBundle& bundle);

void upsert_daily_snapshots(PGconn* pg, long long run_id);

void append_table_timing_ms(std::vector<long long>& timings_ms, long long duration_ms);

nlohmann::json build_cycle_sla_json(const std::vector<long long>& timings_ms, long long cycle_duration_ms);

}  // namespace reconcile_enrichments
