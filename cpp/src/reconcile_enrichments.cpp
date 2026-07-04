#include "reconcile_enrichments.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>

namespace reconcile_enrichments {

namespace {

int pg_int(PGresult* res, int row, int col, int fallback = -1) {
    if (!res || PQgetisnull(res, row, col)) {
        return fallback;
    }
    const char* v = PQgetvalue(res, row, col);
    return v ? std::atoi(v) : fallback;
}

long long pg_bigint(PGresult* res, int row, int col, long long fallback = 0) {
    if (!res || PQgetisnull(res, row, col)) {
        return fallback;
    }
    const char* v = PQgetvalue(res, row, col);
    return v ? std::atoll(v) : fallback;
}

}  // namespace

std::optional<PrevReconcileSnapshot> fetch_prev_reconcile_snapshot(PGconn* pg, long long catalog_id) {
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT rr.row_count_delta, rr.status, COALESCE(rr.checks->>'drift_kind', 'unknown')
        FROM cdc_catalog.reconciliation_result rr
        JOIN cdc_catalog.reconciliation_run rn ON rn.run_id = rr.run_id
        WHERE rr.catalog_id = $1::bigint
          AND rn.finished_at IS NOT NULL
        ORDER BY rn.finished_at DESC, rr.result_id DESC
        LIMIT 1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return std::nullopt;
    }
    PrevReconcileSnapshot out;
    out.row_count_delta = pg_bigint(res, 0, 0);
    out.status = PQgetvalue(res, 0, 1) ? PQgetvalue(res, 0, 1) : "";
    out.drift_kind = PQgetvalue(res, 0, 2) ? PQgetvalue(res, 0, 2) : "";
    PQclear(res);
    return out;
}

ApplyEventsAggregate fetch_apply_events_aggregate(PGconn* pg, long long catalog_id, int lookback_seconds) {
    ApplyEventsAggregate out;
    if (catalog_id <= 0 || lookback_seconds <= 0) {
        return out;
    }
    const std::string cid = std::to_string(catalog_id);
    const std::string lookback = std::to_string(lookback_seconds);
    const char* vals[] = {cid.c_str(), lookback.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            COALESCE(SUM(events_inserts), 0)::bigint,
            COALESCE(SUM(events_updates), 0)::bigint,
            COALESCE(SUM(events_deletes), 0)::bigint,
            COALESCE(SUM(events_total), 0)::bigint
        FROM cdc_catalog.apply_batch_stats
        WHERE catalog_id = $1::bigint
          AND logged_at >= now() - ($2::text || ' seconds')::interval
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    out.inserts = pg_bigint(res, 0, 0);
    out.updates = pg_bigint(res, 0, 1);
    out.deletes = pg_bigint(res, 0, 2);
    out.total = pg_bigint(res, 0, 3);
    out.has_flow = out.total > 0;
    PQclear(res);
    return out;
}

std::optional<std::string> resolve_watermark_column(PGconn* log_pg, const CatalogReconcileRow& row) {
    if (!row.engine_meta_json.empty()) {
        try {
            const auto meta = nlohmann::json::parse(row.engine_meta_json);
            if (meta.contains("reconcile_watermark_column") && meta["reconcile_watermark_column"].is_string()) {
                const std::string col = meta["reconcile_watermark_column"].get<std::string>();
                if (!col.empty()) {
                    return col;
                }
            }
        } catch (...) {
        }
    }
    static const char* kCandidates[] = {
        "updated_at", "modified_at", "last_modified", "last_updated", "changed_at", "created_at"};
    for (const char* candidate : kCandidates) {
        const char* vals[] = {row.lake_schema.c_str(), row.lake_table.c_str(), candidate};
        PGresult* res = PQexecParams(
            log_pg,
            R"(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            LIMIT 1
            )",
            3,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        const bool found = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
        if (res) {
            PQclear(res);
        }
        if (found) {
            return std::string(candidate);
        }
    }
    return std::nullopt;
}

std::string eval_freshness_status(double lag_minutes, int warn_minutes, int fail_minutes) {
    const double abs_lag = std::abs(lag_minutes);
    if (abs_lag > static_cast<double>(fail_minutes)) {
        return "fail";
    }
    if (abs_lag > static_cast<double>(warn_minutes)) {
        return "warn";
    }
    return "ok";
}

FreshnessSnapshot compute_freshness_from_max_ts(
    const FreshnessInput& input,
    int warn_minutes,
    int fail_minutes) {
    FreshnessSnapshot out;
    out.watermark_column = input.watermark_column;
    out.source_max_ts = input.source_max_ts;
    out.lake_max_ts = input.lake_max_ts;
    if (!input.watermark_column || !input.source_max_ts || !input.lake_max_ts) {
        out.status = "skip";
        return out;
    }
    out.computed = true;
    out.status = eval_freshness_status(0.0, warn_minutes, fail_minutes);
    return out;
}

FreshnessSnapshot compute_freshness_lag_sql(
    PGconn* lake_pg,
    const std::optional<std::string>& source_max,
    const std::optional<std::string>& lake_max,
    const std::optional<std::string>& watermark,
    int warn_minutes,
    int fail_minutes) {
    FreshnessSnapshot out;
    out.watermark_column = watermark;
    out.source_max_ts = source_max;
    out.lake_max_ts = lake_max;
    if (!source_max || !lake_max || source_max->empty() || lake_max->empty()) {
        out.status = "skip";
        return out;
    }
    const char* vals[] = {source_max->c_str(), lake_max->c_str()};
    PGresult* res = PQexecParams(
        lake_pg,
        R"(
        SELECT extract(epoch FROM ($1::timestamptz - $2::timestamptz)) / 60.0
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        out.status = "skip";
        return out;
    }
    const char* lag_str = PQgetvalue(res, 0, 0);
    out.lag_minutes = lag_str ? std::atof(lag_str) : 0.0;
    PQclear(res);
    out.computed = true;
    out.status = eval_freshness_status(out.lag_minutes, warn_minutes, fail_minutes);
    return out;
}

SchemaDriftSnapshot compute_schema_drift_lake_only(PGconn* lake_pg, const CatalogReconcileRow& row) {
    SchemaDriftSnapshot out;
    const char* vals[] = {row.lake_schema.c_str(), row.lake_table.c_str()};
    PGresult* res = PQexecParams(
        lake_pg,
        R"(
        SELECT COUNT(*)::int
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out.lake_column_count = pg_int(res, 0, 0);
    }
    if (res) {
        PQclear(res);
    }
    out.status = out.lake_column_count >= 0 ? "ok" : "skip";
    out.match = true;
    return out;
}

void set_schema_source_count(SchemaDriftSnapshot& drift, int source_columns) {
    drift.source_column_count = source_columns;
    if (source_columns >= 0 && drift.lake_column_count >= 0) {
        drift.match = source_columns == drift.lake_column_count;
        drift.status = drift.match ? "ok" : "warn";
    }
}

TierDecision eval_reconcile_tier(
    const CatalogReconcileRow& row,
    bool apply_inactive,
    bool has_recent_flow,
    const std::optional<PrevReconcileSnapshot>& prev) {
    TierDecision out;
    if (row.hot) {
        out.tier = "hot";
        return out;
    }
    if (apply_inactive && !has_recent_flow && prev && prev->status == "ok") {
        out.tier = "cold_lag_only";
        out.skip_row_count = true;
        out.skip_reason = "cold_inactive_prev_ok";
        return out;
    }
    out.tier = "standard";
    return out;
}

bool eval_timing_skew_suspected(
    bool has_recent_apply_flow,
    long long abs_delta,
    int row_warn_abs_tolerance,
    long long source_rows) {
    if (!has_recent_apply_flow || source_rows < 0) {
        return false;
    }
    return abs_delta > 0 && abs_delta <= static_cast<long long>(row_warn_abs_tolerance);
}

std::string derive_recommended_action(
    const std::string& overall,
    const std::string& row_status,
    const std::string& drift_kind,
    long long abs_delta,
    bool suggest_full_load,
    bool quarantined) {
    if (quarantined) {
        return "investigate_quarantine";
    }
    if (suggest_full_load || row_status == "fail" ||
        (drift_kind == "source_ahead" && abs_delta > 50) ||
        (drift_kind == "append_zombie" && abs_delta > 50)) {
        return "full_load";
    }
    if (row_status == "warn") {
        return "monitor";
    }
    if (overall == "fail") {
        return "investigate_pipeline";
    }
    if (overall == "warn") {
        return "monitor";
    }
    return "none";
}

std::string eval_event_loss_status(long long parse_skipped, long long dropped_unrecoverable) {
    if (dropped_unrecoverable > 0) {
        return "fail";
    }
    if (parse_skipped > 0) {
        return "warn";
    }
    return "ok";
}

std::string eval_slice_stale_status(
    bool slice_is_stale,
    const std::string& apply_status,
    const std::string& lag_status) {
    if (!slice_is_stale) {
        return "ok";
    }
    const bool apply_unhealthy = apply_status == "stale" || apply_status == "lagging" ||
                                 apply_status == "gap_detected" || apply_status == "failed" ||
                                 apply_status == "quarantined" || lag_status == "fail";
    if (apply_unhealthy) {
        return "fail";
    }
    if (lag_status == "warn") {
        return "warn";
    }
    return "warn";
}

std::string derive_root_cause_label(
    const std::string& overall,
    const std::string& row_status,
    const std::string& drift_kind,
    const std::string& semaphore_reason,
    bool static_gap,
    bool timing_skew_suspected,
    bool quarantined,
    const std::string& event_loss_status,
    const std::string& slice_stale_status) {
    if (overall == "ok" || overall == "skip") {
        return "Healthy";
    }
    if (quarantined) {
        return "Apply quarantined";
    }
    if (event_loss_status == "fail") {
        return "Unrecoverable apply drops";
    }
    if (event_loss_status == "warn") {
        return "Parse skipped in apply slice";
    }
    if (slice_stale_status == "fail") {
        return "Apply stale (slice + position)";
    }
    if (slice_stale_status == "warn") {
        return "Apply slice stale";
    }
    if (timing_skew_suspected && row_status != "fail") {
        return "Timing skew (reconcile snapshot)";
    }
    if (drift_kind == "source_ahead" && static_gap) {
        return "Mirror incomplete (source ahead)";
    }
    if (drift_kind == "append_zombie") {
        return "Zombie rows in lake";
    }
    if (semaphore_reason == "apply_lag_fail" || semaphore_reason == "apply_lag_warn") {
        return "Apply lag";
    }
    if (semaphore_reason == "row_count_warn") {
        return "Small row-count drift";
    }
    if (semaphore_reason == "freshness_lag_fail" || semaphore_reason == "freshness_lag_warn") {
        return "Freshness lag (MAX watermark)";
    }
    return "Reconcile warning";
}

std::string derive_semaphore_reason_ext(
    const std::string& overall,
    const std::string& row_status,
    const std::string& lag_status,
    const std::string& drift_kind,
    bool quarantined,
    bool freshness_fail) {
    if (overall == "ok" || overall == "skip") {
        return "";
    }
    if (quarantined) {
        return "apply_quarantined";
    }
    if (freshness_fail) {
        return "freshness_lag_fail";
    }
    if (row_status == "fail") {
        if (drift_kind == "source_ahead") {
            return "row_drift_source_ahead";
        }
        if (drift_kind == "append_zombie") {
            return "row_drift_append_zombie";
        }
        return "row_count_fail";
    }
    if (row_status == "warn") {
        if (drift_kind == "append_zombie") {
            return "row_drift_append_zombie";
        }
        if (drift_kind == "source_ahead") {
            return "row_drift_source_ahead";
        }
        return "row_count_warn";
    }
    if (lag_status == "fail") {
        return "apply_lag_fail";
    }
    if (lag_status == "warn") {
        return "apply_lag_warn";
    }
    return "reconcile_warn";
}

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
    bool catalog_has_error) {
    EnrichmentBundle bundle;
    bundle.prev = fetch_prev_reconcile_snapshot(log_pg, row.catalog_id);
    bundle.events_7d = fetch_apply_events_aggregate(log_pg, row.catalog_id, events_lookback_seconds);
    bundle.freshness = compute_freshness_lag_sql(
        lake_pg,
        freshness_in.source_max_ts,
        freshness_in.lake_max_ts,
        freshness_in.watermark_column,
        freshness_warn_minutes,
        freshness_fail_minutes);
    bundle.schema = compute_schema_drift_lake_only(lake_pg, row);
    bundle.tier = eval_reconcile_tier(row, apply_inactive, bundle.events_7d.has_flow, bundle.prev);
    const long long abs_delta =
        source_rows >= 0 && lake_rows >= 0 ? std::llabs(source_rows - lake_rows) : 0;
    bundle.timing_skew_suspected = eval_timing_skew_suspected(
        bundle.events_7d.has_flow, abs_delta, row_warn_abs_tolerance, source_rows);
    bundle.quarantined = apply_status == "quarantined";
    bundle.suggest_full_load =
        static_gap && row_status == "fail" &&
        (drift_kind == "source_ahead" || drift_kind == "append_zombie");
    if (catalog_has_error && row_status != "ok") {
        bundle.suggest_full_load = true;
    }
    bundle.semaphore_reason = derive_semaphore_reason_ext(
        overall,
        row_status,
        lag_status,
        drift_kind,
        bundle.quarantined,
        bundle.freshness.status == "fail");
    bundle.recommended_action = derive_recommended_action(
        overall,
        row_status,
        drift_kind,
        abs_delta,
        bundle.suggest_full_load,
        bundle.quarantined);
    bundle.root_cause_label = derive_root_cause_label(
        overall,
        row_status,
        drift_kind,
        bundle.semaphore_reason,
        static_gap,
        bundle.timing_skew_suspected,
        bundle.quarantined,
        "ok",
        "ok");
    return bundle;
}

void apply_enrichment_to_checks(nlohmann::json& checks, const EnrichmentBundle& bundle) {
    if (bundle.prev) {
        checks["prev_row_count_delta"] = bundle.prev->row_count_delta;
        checks["prev_status"] = bundle.prev->status;
        checks["prev_drift_kind"] = bundle.prev->drift_kind;
        const long long cur_delta = checks.value("row_count_delta", 0);
        checks["delta_change"] = cur_delta - bundle.prev->row_count_delta;
    }
    checks["events_inserts_7d"] = bundle.events_7d.inserts;
    checks["events_updates_7d"] = bundle.events_7d.updates;
    checks["events_deletes_7d"] = bundle.events_7d.deletes;
    checks["events_total_7d"] = bundle.events_7d.total;
    checks["has_recent_apply_flow"] = bundle.events_7d.has_flow;
    if (bundle.freshness.computed) {
        if (bundle.freshness.watermark_column) {
            checks["freshness_watermark_column"] = *bundle.freshness.watermark_column;
        }
        if (bundle.freshness.source_max_ts) {
            checks["source_max_ts"] = *bundle.freshness.source_max_ts;
        }
        if (bundle.freshness.lake_max_ts) {
            checks["lake_max_ts"] = *bundle.freshness.lake_max_ts;
        }
        checks["freshness_lag_minutes"] = bundle.freshness.lag_minutes;
        checks["freshness_lag_status"] = bundle.freshness.status;
    }
    checks["lake_column_count"] = bundle.schema.lake_column_count;
    if (bundle.schema.source_column_count >= 0) {
        checks["source_column_count"] = bundle.schema.source_column_count;
        checks["schema_column_match"] = bundle.schema.match;
        checks["schema_drift_status"] = bundle.schema.status;
    }
    checks["reconcile_tier"] = bundle.tier.tier;
    if (!bundle.tier.skip_reason.empty()) {
        checks["reconcile_tier_skip_reason"] = bundle.tier.skip_reason;
    }
    checks["reconcile_timing_skew_suspected"] = bundle.timing_skew_suspected;
    checks["suggest_full_load"] = bundle.suggest_full_load;
    if (bundle.suggest_full_load) {
        checks["suggest_full_load_reason"] =
            bundle.quarantined ? "quarantine_or_catalog_error" : "static_row_drift";
    }
    checks["recommended_action"] = bundle.recommended_action;
    checks["root_cause_label"] = bundle.root_cause_label;
    if (!bundle.semaphore_reason.empty()) {
        checks["semaphore_reason"] = bundle.semaphore_reason;
    }
    const long long source_rows = checks.value("source_row_count", 0LL);
    checks["reconcile_scope"] = source_rows >= 100000 ? "large_full_count" : "full";
}

void upsert_daily_snapshots(PGconn* pg, long long run_id) {
    const std::string rid = std::to_string(run_id);
    const char* vals[] = {rid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation_result_daily (
            snapshot_date, catalog_id, conn_id, source_schema, source_table,
            status, row_count_delta, drift_kind, semaphore_reason, recommended_action,
            source_row_count, lake_row_count, run_id, checks
        )
        SELECT
            CURRENT_DATE,
            rr.catalog_id,
            rr.conn_id,
            rr.source_schema,
            rr.source_table,
            rr.status,
            rr.row_count_delta,
            rr.checks->>'drift_kind',
            rr.checks->>'semaphore_reason',
            rr.checks->>'recommended_action',
            rr.source_row_count,
            rr.lake_row_count,
            rr.run_id,
            rr.checks
        FROM cdc_catalog.reconciliation_result rr
        WHERE rr.run_id = $1::bigint
        ON CONFLICT (snapshot_date, catalog_id) DO UPDATE SET
            status = EXCLUDED.status,
            row_count_delta = EXCLUDED.row_count_delta,
            drift_kind = EXCLUDED.drift_kind,
            semaphore_reason = EXCLUDED.semaphore_reason,
            recommended_action = EXCLUDED.recommended_action,
            source_row_count = EXCLUDED.source_row_count,
            lake_row_count = EXCLUDED.lake_row_count,
            run_id = EXCLUDED.run_id,
            checks = EXCLUDED.checks,
            updated_at = now()
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

void append_table_timing_ms(std::vector<long long>& timings_ms, long long duration_ms) {
    if (duration_ms >= 0) {
        timings_ms.push_back(duration_ms);
    }
}

nlohmann::json build_cycle_sla_json(const std::vector<long long>& timings_ms, long long cycle_duration_ms) {
    nlohmann::json out = nlohmann::json::object();
    out["cycle_duration_ms"] = cycle_duration_ms;
    out["tables_timed"] = static_cast<int>(timings_ms.size());
    if (timings_ms.empty()) {
        return out;
    }
    std::vector<long long> sorted = timings_ms;
    std::sort(sorted.begin(), sorted.end());
    const auto pct = [&](double p) -> long long {
        const std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(sorted.size() - 1));
        return sorted[std::min(idx, sorted.size() - 1)];
    };
    long long sum = 0;
    for (long long v : sorted) {
        sum += v;
    }
    out["table_duration_ms_p50"] = pct(0.50);
    out["table_duration_ms_p95"] = pct(0.95);
    out["table_duration_ms_max"] = sorted.back();
    out["table_duration_ms_avg"] = sum / static_cast<long long>(sorted.size());
    return out;
}

}  // namespace reconcile_enrichments
