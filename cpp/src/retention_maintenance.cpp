#include "retention_maintenance.hpp"

#include "obs_log.hpp"
#include "pipeline_defaults.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <string>
#include <thread>

namespace {

long long call_prune_fn(
    PGconn* pg,
    const char* sql,
    int nargs,
    int arg1,
    int arg2,
    int arg3) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return -1;
    }
    const std::string a1 = std::to_string(arg1);
    const std::string a2 = std::to_string(arg2);
    const std::string a3 = std::to_string(arg3);
    const char* vals[] = {a1.c_str(), a2.c_str(), a3.c_str()};
    PGresult* res = PQexecParams(pg, sql, nargs, nullptr, vals, nullptr, nullptr, 0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return -1;
    }
    const long long deleted =
        PQgetisnull(res, 0, 0) ? 0 : std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return deleted;
}

/**
 * Call prune_*_batched with p_max_batches=1 repeatedly so each DELETE commits
 * in its own transaction (function body otherwise holds one long txn).
 */
long long prune_in_committed_batches(
    PGconn* pg,
    const char* sql,
    int retention_days,
    int batch_size,
    int max_batches,
    int pause_ms) {
    long long total = 0;
    for (int i = 0; i < max_batches; ++i) {
        const long long deleted = call_prune_fn(pg, sql, 3, retention_days, batch_size, 1);
        if (deleted < 0) {
            return -1;
        }
        total += deleted;
        if (deleted == 0) {
            break;
        }
        if (pause_ms > 0 && i + 1 < max_batches) {
            std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
        }
    }
    return total;
}

long long prune_idle_in_committed_batches(
    PGconn* pg,
    int batch_size,
    int max_batches,
    int pause_ms) {
    long long total = 0;
    for (int i = 0; i < max_batches; ++i) {
        const long long deleted = call_prune_fn(
            pg,
            "SELECT cdc_catalog.prune_apply_batch_stats_idle_batched($1::integer, $2::integer)",
            2,
            batch_size,
            1,
            0);
        if (deleted < 0) {
            return -1;
        }
        total += deleted;
        if (deleted == 0) {
            break;
        }
        if (pause_ms > 0 && i + 1 < max_batches) {
            std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
        }
    }
    return total;
}

bool try_advisory_lock(PGconn* pg, long long lock_key) {
    const std::string key = std::to_string(lock_key);
    const char* vals[] = {key.c_str()};
    PGresult* res = PQexecParams(
        pg, "SELECT pg_try_advisory_lock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    bool locked = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        locked = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return locked;
}

void advisory_unlock(PGconn* pg, long long lock_key) {
    const std::string key = std::to_string(lock_key);
    const char* vals[] = {key.c_str()};
    PGresult* res = PQexecParams(
        pg, "SELECT pg_advisory_unlock($1::bigint)", 1, nullptr, vals, nullptr, nullptr, 0);
    if (res) {
        PQclear(res);
    }
}

std::string today_costa_rica_date(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        "SELECT to_char((now() AT TIME ZONE 'America/Costa_Rica')::date, 'YYYY-MM-DD')");
    std::string out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out = PQgetvalue(res, 0, 0);
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

bool retention_ran_this_hour(PGconn* pg) {
    if (!pg) {
        return false;
    }
    PGresult* res = PQexec(
        pg,
        R"(
        SELECT 1
        FROM cdc_catalog.logs
        WHERE component = 'retention_maintenance'
          AND message LIKE 'scheduled batched retention prune completed%'
          AND date_trunc('hour', logged_at AT TIME ZONE 'America/Costa_Rica')
            = date_trunc('hour', now() AT TIME ZONE 'America/Costa_Rica')
        LIMIT 1
        )");
    const bool done = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return done;
}

bool retention_maintenance_due(PGconn* pg) { return !retention_ran_this_hour(pg); }

}  // namespace

void maybe_run_scheduled_retention_maintenance(PGconn* log_pg) {
    if (!log_pg || PQstatus(log_pg) != CONNECTION_OK) {
        return;
    }
    if (!retention_maintenance_due(log_pg)) {
        return;
    }
    if (!try_advisory_lock(log_pg, pipeline_defaults::kRetentionMaintenanceAdvisoryLockKey)) {
        return;
    }

    const std::string batch_id = make_batch_id();
    const std::string today = today_costa_rica_date(log_pg);
    const int stats_retention = pipeline_defaults::kApplyBatchStatsRetentionDays;
    const int stats_batch_size = pipeline_defaults::kApplyBatchStatsPruneBatchSizeDefault;
    const int stats_max_batches = pipeline_defaults::kApplyBatchStatsPruneMaxBatchesDefault;
    const int logs_retention = pipeline_defaults::kLogsRetentionDaysDefault;
    const int logs_batch_size = pipeline_defaults::kLogsPurgeBatchSizeDefault;
    const int logs_max_batches = pipeline_defaults::kLogsPurgeMaxBatchesDefault;
    const int pause_ms = pipeline_defaults::kRetentionPruneBatchPauseMs;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "retention_maintenance",
        .message = "scheduled batched retention prune started",
        .batch_id = batch_id,
        .context = {
            {"cadence", "hourly"},
            {"run_date", today},
            {"row_cap_per_prune", pipeline_defaults::kRetentionPruneMaxRowsPerRun},
            {"apply_batch_stats_retention_days", stats_retention},
            {"logs_retention_days", logs_retention},
            {"apply_batch_stats_batch_size", stats_batch_size},
            {"apply_batch_stats_max_batches", stats_max_batches},
            {"logs_batch_size", logs_batch_size},
            {"logs_max_batches", logs_max_batches},
            {"commit_per_batch", true},
            {"batch_pause_ms", pause_ms},
        },
    });

    const long long idle_raw = prune_idle_in_committed_batches(
        log_pg, stats_batch_size, stats_max_batches, pause_ms);
    const bool idle_unavailable = idle_raw < 0;
    const long long idle_deleted = idle_unavailable ? 0 : idle_raw;
    int age_batches = stats_max_batches;
    if (idle_deleted > 0 && stats_batch_size > 0) {
        const int idle_batches = static_cast<int>(
            (idle_deleted + stats_batch_size - 1) / stats_batch_size);
        age_batches = std::max(0, stats_max_batches - idle_batches);
    }
    const long long stats_deleted = age_batches > 0
        ? prune_in_committed_batches(
              log_pg,
              "SELECT cdc_catalog.prune_apply_batch_stats_batched($1::integer, $2::integer, $3::integer)",
              stats_retention,
              stats_batch_size,
              age_batches,
              pause_ms)
        : 0;
    const long long logs_deleted = prune_in_committed_batches(
        log_pg,
        "SELECT cdc_catalog.purge_logs_batched($1::integer, $2::integer, $3::integer)",
        logs_retention,
        logs_batch_size,
        logs_max_batches,
        pause_ms);

    // One run per CST hour (even with backlog) — next hour continues catch-up.
    const long long stats_cap =
        static_cast<long long>(stats_batch_size) * static_cast<long long>(stats_max_batches);
    const long long logs_cap =
        static_cast<long long>(logs_batch_size) * static_cast<long long>(logs_max_batches);
    const bool any_error = stats_deleted < 0 || logs_deleted < 0;
    const bool backlog_remaining =
        (!any_error) &&
        ((idle_deleted + std::max(0LL, stats_deleted) >= stats_cap && stats_cap > 0) ||
         (logs_deleted >= logs_cap && logs_cap > 0));
    advisory_unlock(log_pg, pipeline_defaults::kRetentionMaintenanceAdvisoryLockKey);

    log_write(log_pg, {
        .level = any_error || backlog_remaining ? LogLevel::Warning : LogLevel::Info,
        .component = "retention_maintenance",
        .message = any_error
            ? "scheduled batched retention prune completed with errors"
            : (backlog_remaining
                   ? "scheduled batched retention prune completed with backlog remaining"
                   : "scheduled batched retention prune completed"),
        .batch_id = batch_id,
        .context = {
            {"apply_batch_stats_idle_deleted", idle_deleted},
            {"apply_batch_stats_idle_fn_missing", idle_unavailable},
            {"apply_batch_stats_deleted", stats_deleted},
            {"apply_batch_stats_age_batches", age_batches},
            {"logs_deleted", logs_deleted},
            {"backlog_remaining", backlog_remaining},
            {"row_cap_per_prune", pipeline_defaults::kRetentionPruneMaxRowsPerRun},
        },
    });
}
