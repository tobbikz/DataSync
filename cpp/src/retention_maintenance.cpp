#include "retention_maintenance.hpp"

#include "obs_log.hpp"
#include "pipeline_defaults.hpp"
#include "runtime_config.hpp"

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <string>
#include <thread>

namespace {

long long call_prune_fn(
    PGconn* pg,
    const char* sql,
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
    PGresult* res = PQexecParams(pg, sql, 3, nullptr, vals, nullptr, nullptr, 0);
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
        const long long deleted = call_prune_fn(pg, sql, retention_days, batch_size, 1);
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

bool try_advisory_lock(PGconn* pg, int lock_key) {
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

void advisory_unlock(PGconn* pg, int lock_key) {
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

int local_hour_costa_rica(PGconn* pg) {
    PGresult* res = PQexec(
        pg,
        "SELECT EXTRACT(HOUR FROM (now() AT TIME ZONE 'America/Costa_Rica'))::int");
    int hour = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        hour = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return hour;
}

std::string last_retention_run_date(PGconn* pg) {
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COALESCE(config_value #>> '{}', '')
        FROM cdc_catalog.runtime_config
        WHERE config_key = 'retention_maintenance_last_run_date'
          AND component = 'global'
          AND conn_id = ''
        )",
        0,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0);
    std::string out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        const char* v = PQgetvalue(res, 0, 0);
        if (v) {
            out = v;
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

bool mark_retention_run_date(PGconn* pg, const std::string& date) {
    const std::string json = "\"" + date + "\"";
    const char* vals[] = {json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.runtime_config (
            config_key, component, conn_id, config_value, description
        ) VALUES (
            'retention_maintenance_last_run_date', 'global', '', $1::jsonb,
            'Last calendar date (America/Costa_Rica) batched retention prune completed'
        )
        ON CONFLICT (config_key, component, conn_id) DO UPDATE SET
            config_value = EXCLUDED.config_value,
            description  = EXCLUDED.description,
            updated_at   = now()
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool ok = res && PQresultStatus(res) == PGRES_COMMAND_OK;
    if (res) {
        PQclear(res);
    }
    return ok;
}

bool retention_maintenance_due(PGconn* pg) {
    const int hour = local_hour_costa_rica(pg);
    if (hour != pipeline_defaults::kRetentionMaintenanceLocalHour) {
        return false;
    }
    const std::string today = today_costa_rica_date(pg);
    if (today.empty()) {
        return false;
    }
    return last_retention_run_date(pg) != today;
}

}  // namespace

void maybe_run_scheduled_retention_maintenance(PGconn* log_pg, RuntimeConfig& runtime) {
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
    const int stats_retention = runtime.get_int(
        "apply_batch_stats_retention_days",
        pipeline_defaults::kApplyBatchStatsRetentionDays,
        "global");
    const int applied_retention = runtime.get_int(
        "applied_events_retention_days",
        pipeline_defaults::kAppliedEventsRetentionDaysDefault,
        "cdc_kafka_apply");
    const int stats_batch_size = runtime.get_int(
        "apply_batch_stats_prune_batch_size",
        pipeline_defaults::kApplyBatchStatsPruneBatchSizeDefault,
        "global");
    const int stats_max_batches = runtime.get_int(
        "apply_batch_stats_prune_max_batches",
        pipeline_defaults::kApplyBatchStatsPruneMaxBatchesDefault,
        "global");
    const int events_batch_size = runtime.get_int(
        "applied_events_prune_batch_size",
        pipeline_defaults::kAppliedEventsPruneBatchSizeDefault,
        "global");
    const int events_max_batches = runtime.get_int(
        "applied_events_prune_max_batches",
        pipeline_defaults::kAppliedEventsPruneMaxBatchesDefault,
        "global");
    const int logs_retention = runtime.get_int(
        "logs_retention_days",
        pipeline_defaults::kLogsRetentionDaysDefault,
        "global");
    const int logs_batch_size = runtime.get_int(
        "logs_purge_batch_size",
        pipeline_defaults::kLogsPurgeBatchSizeDefault,
        "global");
    const int logs_max_batches = runtime.get_int(
        "logs_purge_max_batches",
        pipeline_defaults::kLogsPurgeMaxBatchesDefault,
        "global");
    const int pause_ms = pipeline_defaults::kRetentionPruneBatchPauseMs;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "retention_maintenance",
        .message = "scheduled batched retention prune started",
        .batch_id = batch_id,
        .context = {
            {"local_hour_cst", pipeline_defaults::kRetentionMaintenanceLocalHour},
            {"run_date", today},
            {"apply_batch_stats_retention_days", stats_retention},
            {"applied_events_retention_days", applied_retention},
            {"logs_retention_days", logs_retention},
            {"apply_batch_stats_batch_size", stats_batch_size},
            {"apply_batch_stats_max_batches", stats_max_batches},
            {"applied_events_batch_size", events_batch_size},
            {"applied_events_max_batches", events_max_batches},
            {"logs_batch_size", logs_batch_size},
            {"logs_max_batches", logs_max_batches},
            {"commit_per_batch", true},
            {"batch_pause_ms", pause_ms},
        },
    });

    const long long stats_deleted = prune_in_committed_batches(
        log_pg,
        "SELECT cdc_catalog.prune_apply_batch_stats_batched($1::integer, $2::integer, $3::integer)",
        stats_retention,
        stats_batch_size,
        stats_max_batches,
        pause_ms);
    const long long events_deleted = prune_in_committed_batches(
        log_pg,
        "SELECT cdc_catalog.prune_applied_events_batched($1::integer, $2::integer, $3::integer)",
        applied_retention,
        events_batch_size,
        events_max_batches,
        pause_ms);
    const long long logs_deleted = prune_in_committed_batches(
        log_pg,
        "SELECT cdc_catalog.purge_logs_batched($1::integer, $2::integer, $3::integer)",
        logs_retention,
        logs_batch_size,
        logs_max_batches,
        pause_ms);

    // Hit batch cap ⇒ backlog remains; do not mark the day so another pass can run
    // while still inside the 03:00 CST hour (or tomorrow if the window ended).
    const long long stats_cap =
        static_cast<long long>(stats_batch_size) * static_cast<long long>(stats_max_batches);
    const long long events_cap =
        static_cast<long long>(events_batch_size) * static_cast<long long>(events_max_batches);
    const long long logs_cap =
        static_cast<long long>(logs_batch_size) * static_cast<long long>(logs_max_batches);
    const bool any_error = stats_deleted < 0 || events_deleted < 0 || logs_deleted < 0;
    const bool backlog_remaining =
        (!any_error) &&
        ((stats_deleted >= stats_cap && stats_cap > 0) ||
         (events_deleted >= events_cap && events_cap > 0) ||
         (logs_deleted >= logs_cap && logs_cap > 0));
    const bool marked =
        !today.empty() && !backlog_remaining && !any_error &&
        mark_retention_run_date(log_pg, today);
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
            {"apply_batch_stats_deleted", stats_deleted},
            {"applied_events_deleted", events_deleted},
            {"logs_deleted", logs_deleted},
            {"backlog_remaining", backlog_remaining},
            {"run_date_marked", marked},
            {"applied_events_cap", events_cap},
        },
    });
}
