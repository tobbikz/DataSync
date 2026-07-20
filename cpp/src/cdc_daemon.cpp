#include "cdc_daemon.hpp"
#include "capture_common.hpp"
#include "catalog_sync.hpp"
#include "cdc_pre_apply.hpp"
#include "config.hpp"
#include "connections.hpp"
#include "daemon_full_load.hpp"
#include "full_load_checkpoint.hpp"
#include "kafka_apply.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "retention_maintenance.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>
#include <vector>

namespace {

AppConfig snapshot_app_config(const AppConfig& cfg);
int round_idle_seconds(const CdcConfig& cdc);

std::atomic<int> g_catalog_sync_round{0};
std::vector<std::thread> g_background_threads;
std::mutex g_background_threads_mu;
std::mutex g_apply_workers_mu;
std::unordered_set<std::string> g_apply_workers_spawned;

void join_background_threads() {
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(g_background_threads_mu);
        threads.swap(g_background_threads);
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

extern "C" void on_signal(int) {
    g_shutdown.store(true, std::memory_order_relaxed);
}

void sleep_interruptible(int seconds) {
    for (int i = 0; i < seconds && !g_shutdown.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

std::vector<std::string> all_conn_ids(const AppConfig& cfg) {
    std::vector<std::string> ids;
    for (const auto& source : cfg.mariadb_sources) {
        ids.push_back(source.conn_id);
    }
    for (const auto& source : cfg.mssql_sources) {
        ids.push_back(source.conn_id);
    }
    for (const auto& source : cfg.mongo_sources) {
        ids.push_back(source.conn_id);
    }
    return ids;
}

int run_apply_workers(const AppConfig& cfg, const std::string& conn_id) {
    int failures = 0;
    auto run_pool = [&](CatalogHotTier tier, int workers) {
        if (workers <= 1) {
            PgConn pg(cfg.datasync.conn_string());
            if (!pg.raw) {
                failures += 1;
                return;
            }
            if (run_kafka_apply_native_cli(cfg, pg.raw, conn_id, 0, 1, tier) != 0) {
                failures += 1;
            }
            return;
        }
        std::vector<std::thread> threads;
        std::atomic<int> pool_failures{0};
        threads.reserve(static_cast<std::size_t>(workers));
        for (int worker_id = 0; worker_id < workers; ++worker_id) {
            threads.emplace_back([&, worker_id]() {
                try {
                    PgConn pg(cfg.datasync.conn_string());
                    if (!pg.raw) {
                        pool_failures.fetch_add(1);
                        return;
                    }
                    const int rc =
                        run_kafka_apply_native_cli(cfg, pg.raw, conn_id, worker_id, workers, tier);
                    if (rc != 0) {
                        pool_failures.fetch_add(1);
                    }
                } catch (const std::exception&) {
                    pool_failures.fetch_add(1);
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        failures += pool_failures.load();
    };
    run_pool(CatalogHotTier::ColdOnly, kApplyWorkerCount);
    run_pool(CatalogHotTier::HotOnly, pipeline_defaults::kHotApplyConsumerCount);
    return failures > 0 ? 1 : 0;
}

void apply_worker_loop(
    AppConfig cfg,
    std::string conn_id,
    int worker_id,
    int worker_count,
    CatalogHotTier hot_tier) {
    const bool hot_path = hot_tier == CatalogHotTier::HotOnly;
    while (!g_shutdown.load()) {
        try {
            PgConn pg(cfg.datasync.conn_string());
            if (!pg.raw) {
                sleep_interruptible(round_idle_seconds(cfg.cdc));
                continue;
            }
            const int rc =
                run_kafka_apply_native_cli(cfg, pg.raw, conn_id, worker_id, worker_count, hot_tier);
            if (rc != 0) {
                log_write(pg.raw, {
                    .level = LogLevel::Warning,
                    .component = "cdc_daemon",
                    .message = "apply worker slice finished with errors",
                    .batch_id = make_batch_id(),
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"worker_id", worker_id},
                        {"worker_count", worker_count},
                        {"hot_path", hot_path},
                        {"exit_code", rc},
                    },
                });
            }
        } catch (const std::exception& ex) {
            try {
                PgConn pg(cfg.datasync.conn_string());
                if (pg.raw) {
                    log_write(pg.raw, {
                        .level = LogLevel::Error,
                        .component = "cdc_daemon",
                        .message = "apply worker loop failed",
                        .batch_id = make_batch_id(),
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"worker_id", worker_id},
                            {"worker_count", worker_count},
                            {"hot_path", hot_path},
                            {"error", ex.what()},
                        },
                    });
                }
            } catch (...) {
            }
        }
        if (g_shutdown.load()) {
            break;
        }
        sleep_interruptible(round_idle_seconds(cfg.cdc));
    }
}

void spawn_apply_worker_pool(
    const AppConfig& worker_cfg,
    const std::string& conn_id,
    CatalogHotTier hot_tier,
    int worker_count,
    const std::string& spawn_key) {
    std::lock_guard<std::mutex> lock(g_apply_workers_mu);
    if (!g_apply_workers_spawned.insert(spawn_key).second) {
        return;
    }
    const bool hot_path = hot_tier == CatalogHotTier::HotOnly;
    for (int worker_id = 0; worker_id < worker_count; ++worker_id) {
        std::thread t([worker_cfg, conn_id, worker_id, worker_count, hot_tier]() {
            apply_worker_loop(worker_cfg, conn_id, worker_id, worker_count, hot_tier);
        });
        std::lock_guard<std::mutex> bg_lock(g_background_threads_mu);
        g_background_threads.push_back(std::move(t));
    }
    try {
        PgConn pg(worker_cfg.datasync.conn_string());
        if (pg.raw) {
            log_write(pg.raw, {
                .level = LogLevel::Info,
                .component = "cdc_daemon",
                .message = hot_path ? "hot apply worker loops started" : "apply worker loops started",
                .batch_id = make_batch_id(),
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"worker_count", worker_count},
                    {"hot_path", hot_path},
                    {"mode", "background"},
                },
            });
        }
    } catch (...) {
    }
}

void ensure_apply_worker_loops(const AppConfig& cfg, const std::vector<std::string>& conn_ids) {
    {
        std::unordered_set<std::string> valid;
        for (const auto& conn_id : conn_ids) {
            valid.insert(conn_id + ":cold");
            valid.insert(conn_id + ":hot");
        }
        std::lock_guard<std::mutex> lock(g_apply_workers_mu);
        for (auto it = g_apply_workers_spawned.begin(); it != g_apply_workers_spawned.end();) {
            if (!valid.count(*it)) {
                it = g_apply_workers_spawned.erase(it);
            } else {
                ++it;
            }
        }
    }
    const AppConfig worker_cfg = snapshot_app_config(cfg);
    int cold_workers = pipeline_defaults::kApplyWorkerCount;
    try {
        PgConn pg(worker_cfg.datasync.conn_string());
        if (pg.raw) {
            RuntimeConfig runtime;
            runtime.reload(pg.raw);
            cold_workers = runtime.get_int(
                "apply_worker_count",
                pipeline_defaults::kApplyWorkerCount,
                "cdc_kafka_apply",
                "");
        }
    } catch (...) {
    }
    for (const auto& conn_id : conn_ids) {
        spawn_apply_worker_pool(
            worker_cfg, conn_id, CatalogHotTier::ColdOnly, cold_workers, conn_id + ":cold");
        spawn_apply_worker_pool(
            worker_cfg,
            conn_id,
            CatalogHotTier::HotOnly,
            pipeline_defaults::kHotApplyConsumerCount,
            conn_id + ":hot");
    }
}

void spawn_daemon_full_load_background(
    AppConfig cfg,
    std::string conn_id,
    std::string batch_id,
    int pending_before,
    std::string db_engine) {
    std::thread t(
        [cfg = std::move(cfg),
         conn_id = std::move(conn_id),
         batch_id = std::move(batch_id),
         pending_before,
         db_engine = std::move(db_engine)]() {
            try {
                PgConn pg(cfg.datasync.conn_string());
                if (!pg.raw) {
                    return;
                }
                const auto outcome = try_run_daemon_full_load_isolated(cfg, pg.raw, conn_id, batch_id);
                if (!outcome) {
                    log_write(pg.raw, {
                        .level = LogLevel::Info,
                        .component = "cdc_daemon",
                        .message = "daemon full-load skipped: conn lock held by another full-load",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .context = {
                            {"db_engine", db_engine},
                            {"pending_tables", pending_before},
                            {"phase", "full_load_background"},
                        },
                    });
                    return;
                }
                if (!outcome->ran) {
                    return;
                }
                const bool partial_ok = outcome->tables_loaded > 0 && outcome->exit_code != 0;
                log_write(pg.raw, {
                    .level = outcome->exit_code == 0 ? LogLevel::Info : LogLevel::Warning,
                    .component = "cdc_daemon",
                    .message = outcome->exit_code == 0
                                   ? "daemon full-load background finished"
                                   : (partial_ok ? "daemon full-load background partial"
                                                 : "daemon full-load background failed"),
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .context = {
                        {"db_engine", db_engine},
                        {"full_load_exit", outcome->exit_code},
                        {"pending_tables", outcome->pending_tables},
                        {"pending_after", outcome->pending_after},
                        {"tables_loaded", outcome->tables_loaded},
                        {"phase", "full_load_background"},
                    },
                });
            } catch (const std::exception& ex) {
                try {
                    PgConn pg(cfg.datasync.conn_string());
                    if (pg.raw) {
                        log_write(pg.raw, {
                            .level = LogLevel::Error,
                            .component = "cdc_daemon",
                            .message = "daemon full-load background failed",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .context = {{"error", ex.what()}},
                        });
                    }
                } catch (...) {
                }
            }
        });
    std::lock_guard<std::mutex> lock(g_background_threads_mu);
    g_background_threads.push_back(std::move(t));
}

void scan_apply_health_alerts(PGconn* pg, const std::string& batch_id) {
    const int lookback = pipeline_defaults::kApplyHealthAlertLookbackMinutes;
    const std::string lookback_str = std::to_string(lookback);
    const char* vals[] = {lookback_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT DISTINCT ON (conn_id, source_schema, source_table)
            conn_id,
            source_schema,
            source_table,
            apply_health_rag,
            health_reason,
            kafka_consumer_lag,
            is_stale,
            logged_at
        FROM cdc_catalog.apply_batch_stats
        WHERE apply_health_rag IN ('RED', 'AMBER')
          AND logged_at >= now() - ($1::int * interval '1 minute')
        ORDER BY conn_id, source_schema, source_table, logged_at DESC
        )",
        1,
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
        const std::string rag = PQgetvalue(res, i, 3);
        log_write(pg, {
            .level = rag == "RED" ? LogLevel::Error : LogLevel::Warning,
            .component = "cdc_kafka_health",
            .message = rag == "RED" ? "apply health RED" : "apply health AMBER",
            .batch_id = batch_id,
            .conn_id = PQgetvalue(res, i, 0),
            .source_schema = PQgetvalue(res, i, 1),
            .source_table = PQgetvalue(res, i, 2),
            .context = {
                {"apply_health_rag", rag},
                {"health_reason", PQgetisnull(res, i, 4) ? "" : PQgetvalue(res, i, 4)},
                {"kafka_consumer_lag", PQgetisnull(res, i, 5) ? 0 : std::atoll(PQgetvalue(res, i, 5))},
                {"is_stale", PQgetisnull(res, i, 6) ? false : (PQgetvalue(res, i, 6)[0] == 't')},
                {"lookback_minutes", lookback},
            },
        });
    }
    PQclear(res);
}

void scan_capture_health_alerts(PGconn* pg, const std::string& batch_id) {
    const int warn_seconds = pipeline_defaults::kCaptureHealthAlertStaleSeconds;
    const int fail_seconds = pipeline_defaults::kCaptureHealthAlertFailSeconds;
    refresh_capture_position_health(pg, warn_seconds);

    const std::string warn = std::to_string(warn_seconds);
    const char* vals[] = {warn.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            conn_id,
            COALESCE(binlog_file, ''),
            status::text,
            COALESCE(last_error, ''),
            extract(epoch FROM (now() - updated_at))::integer AS silent_seconds,
            extract(epoch FROM (now() - COALESCE(last_event_ts, updated_at)))::integer AS event_silent_seconds
        FROM cdc_catalog.capture_position
        WHERE extract(epoch FROM (now() - updated_at)) > $1::integer
        ORDER BY silent_seconds DESC
        )",
        1,
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
        const std::string conn_id = PQgetvalue(res, i, 0);
        const int silent_seconds = PQgetisnull(res, i, 4) ? 0 : std::atoi(PQgetvalue(res, i, 4));
        const int event_silent_seconds =
            PQgetisnull(res, i, 5) ? silent_seconds : std::atoi(PQgetvalue(res, i, 5));
        const std::string rag = silent_seconds >= fail_seconds ? "RED" : "AMBER";
        const std::string db_engine = conn_db_engine_from_pg(pg, conn_id);
        const bool capture_gate_blocked =
            db_engine.empty()
                ? full_load_gate_blocks_onboard(pg, conn_id)
                : full_load_gate_blocks_capture(pg, conn_id, db_engine);
        const bool copy_checkpoint = conn_has_active_copy_checkpoints(pg, conn_id);
        const bool subprocess = full_load_subprocess_running(conn_id);
        std::string health_reason = "capture_stale";
        if (capture_gate_blocked && copy_checkpoint && !subprocess) {
            health_reason = "capture_gate_orphan_checkpoint";
        } else if (capture_gate_blocked && subprocess) {
            health_reason = "capture_gate_full_load";
        } else if (capture_gate_blocked) {
            health_reason = "capture_gate_blocked";
        }

        log_write(pg, {
            .level = rag == "RED" ? LogLevel::Error : LogLevel::Warning,
            .component = "cdc_kafka_health",
            .message = rag == "RED" ? "capture health RED" : "capture health AMBER",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"capture_health_rag", rag},
                {"health_reason", health_reason},
                {"silent_seconds", silent_seconds},
                {"event_silent_seconds", event_silent_seconds},
                {"capture_status", PQgetisnull(res, i, 2) ? "" : PQgetvalue(res, i, 2)},
                {"binlog_file", PQgetisnull(res, i, 1) ? "" : PQgetvalue(res, i, 1)},
                {"last_error", PQgetisnull(res, i, 3) ? "" : PQgetvalue(res, i, 3)},
                {"full_load_gate_active", capture_gate_blocked},
                {"copy_checkpoint_active", copy_checkpoint},
                {"full_load_subprocess_active", subprocess},
                {"stale_warn_seconds", warn_seconds},
                {"stale_fail_seconds", fail_seconds},
            },
        });
    }
    PQclear(res);
}

int run_one_cycle(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    bool sync_apply,
    int daemon_round) {
    const std::string batch_id = make_batch_id();
    const std::string db_engine = conn_engine(cfg, conn_id);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon cycle started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"db_engine", db_engine}},
    });

    if (!full_load_subprocess_running(conn_id)) {
        const int checkpoints_cleared = clear_stale_copy_checkpoints_blocking_cdc(
            log_pg,
            conn_id,
            pipeline_defaults::kFullLoadStaleInProgressMinutes,
            batch_id);
        if (checkpoints_cleared > 0) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_daemon",
                .message = "CDC gate recovery: stale copy checkpoints removed before cycle",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"checkpoints_cleared", checkpoints_cleared}},
            });
        }
    }

    const int pending_before = count_full_load_pending(log_pg, conn_id, db_engine);
    if (pending_before > 0) {
        try_recover_stale_full_load_lock(log_pg, conn_id, batch_id);
        recover_full_load_for_checkpoint_resume(log_pg, conn_id, db_engine, batch_id);
        if (!full_load_subprocess_running(conn_id)) {
            reset_full_load_in_progress_for_conn(log_pg, conn_id, db_engine);
        }
        if (db_engine == "mssql") {
            clear_stale_full_load_in_progress(
                log_pg,
                conn_id,
                db_engine,
                pipeline_defaults::kMssqlFullLoadStaleInProgressMinutes);
        }
    }
    const bool conn_full_load_busy = full_load_conn_busy(conn_id);
    const bool full_load_subprocess_active = full_load_subprocess_running(conn_id);
    const bool capture_gate_active = full_load_gate_blocks_capture(log_pg, conn_id, db_engine);
    const bool onboard_gate_active = full_load_gate_blocks_onboard(log_pg, conn_id);
    bool full_load_spawned = false;
    if (pending_before > 0 && !conn_full_load_busy && !full_load_subprocess_active) {
        full_load_spawned = true;
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "daemon full-load started in background; CDC slice running concurrently",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"pending_tables", pending_before},
                {"phase", "full_load_background"},
                {"non_blocking", true},
            },
        });
        spawn_daemon_full_load_background(
            snapshot_app_config(cfg), conn_id, batch_id, pending_before, db_engine);
    }

    const int pending_onboard = count_full_load_pending_onboard(log_pg, conn_id, db_engine);
    int onboard_retry_rc = 0;
    if (pending_onboard > 0 && !conn_full_load_busy && !onboard_gate_active) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "full-load onboard retry: tables awaiting kafka offset reset",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"pending_onboard", pending_onboard},
                {"phase", "onboard_retry"},
            },
        });
        if (!onboard_conn_after_full_load(cfg, log_pg, conn_id, db_engine, batch_id)) {
            onboard_retry_rc = 1;
        }
    } else if (pending_onboard > 0 && onboard_gate_active) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "onboard deferred: full load gate active",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"pending_onboard", pending_onboard},
                {"phase", "full_load_gate"},
            },
        });
    }

    if (onboard_gate_active) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = full_load_subprocess_active
                ? "pre-apply deferred: full load subprocess active"
                : "pre-apply deferred: full load copy checkpoint active",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"phase", "full_load_gate"},
                {"full_load_subprocess_active", full_load_subprocess_active},
                {"copy_checkpoint_active", onboard_gate_active && !full_load_subprocess_active},
            },
        });
    }
    const PreApplyCycleResult pre =
        onboard_gate_active
            ? PreApplyCycleResult{}
            : run_pre_apply_cycle(cfg, log_pg, conn_id, batch_id, daemon_round);
    const int pre_rc = pre.errors > 0 ? 1 : 0;
    int apply_rc = 0;
    if (sync_apply && !onboard_gate_active) {
        apply_rc = run_apply_workers(cfg, conn_id);
    } else if (sync_apply && onboard_gate_active) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = full_load_subprocess_active
                ? "apply deferred: full load subprocess active"
                : "apply deferred: full load copy checkpoint active",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"phase", "full_load_gate"},
                {"full_load_subprocess_active", full_load_subprocess_active},
                {"copy_checkpoint_active", onboard_gate_active && !full_load_subprocess_active},
            },
        });
    }
    int cycle_errors = (pre_rc != 0 ? 1 : 0) + (apply_rc != 0 ? 1 : 0) + onboard_retry_rc;

    try {
        const int cleared = clear_stale_cdc_in_progress(log_pg, conn_id, db_engine);
        if (cleared > 0) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_daemon",
                .message = "stale cdc_in_progress cleared after daemon cycle",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"cleared", cleared}, {"db_engine", db_engine}},
            });
        }
    } catch (const std::exception& ex) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_daemon",
            .message = "clear stale cdc_in_progress failed after cycle",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}},
        });
    }

    log_write(log_pg, {
        .level = cycle_errors ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_daemon",
        .message = cycle_errors ? "daemon cycle completed with errors" : "daemon cycle completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"pre_apply_exit", pre_rc},
            {"apply_exit", apply_rc},
            {"apply_mode", sync_apply ? "sync" : "background"},
            {"full_load_pending", pending_before},
            {"full_load_conn_busy", conn_full_load_busy},
            {"full_load_subprocess_active", full_load_subprocess_active},
            {"capture_gate_active", capture_gate_active},
            {"onboard_gate_active", onboard_gate_active},
            {"full_load_spawned", full_load_spawned},
            {"errors", cycle_errors},
        },
    });

    return cycle_errors == 0 ? 0 : 1;
}

int run_parallel_daemon_round(const AppConfig& cfg, const std::vector<std::string>& conn_ids, bool sync_apply) {
    std::atomic<int> failures{0};
    const std::string round_batch_id = make_batch_id();
    const int round = g_catalog_sync_round.fetch_add(1) + 1;

    {
        PgConn catalog_pg(cfg.datasync.conn_string());
        if (catalog_pg.raw) {
            const int sync_every = pipeline_defaults::kCatalogSyncIntervalRounds;
            const bool do_sync = sync_every <= 1 || round == 1 || (round % sync_every) == 0;
            if (do_sync) {
                if (sync_all_catalogs(cfg, catalog_pg.raw, round_batch_id) != 0) {
                    failures.fetch_add(1);
                }
            }
        } else {
            failures.fetch_add(1);
        }
    }

    auto make_capture_fn = [&](const std::string& conn_id) {
        return [&, conn_id]() {
            PgConn capture_pg(cfg.datasync.conn_string());
            if (!capture_pg.raw) {
                failures.fetch_add(1);
                return;
            }
            try {
                const std::string db_engine = conn_engine(cfg, conn_id);
                if (!full_load_subprocess_running(conn_id)) {
                    clear_stale_copy_checkpoints_blocking_cdc(
                        capture_pg.raw,
                        conn_id,
                        pipeline_defaults::kFullLoadStaleInProgressMinutes,
                        round_batch_id);
                }
                if (full_load_gate_blocks_capture(capture_pg.raw, conn_id, db_engine)) {
                    const bool subprocess = full_load_subprocess_running(conn_id);
                    const std::string defer_reason = subprocess
                        ? "capture deferred: full load subprocess active"
                        : "capture deferred: full load copy checkpoint active";
                    note_capture_position_deferred(capture_pg.raw, conn_id, defer_reason);
                    log_write(capture_pg.raw, {
                        .level = LogLevel::Info,
                        .component = "cdc_daemon",
                        .message = defer_reason,
                        .batch_id = round_batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"db_engine", db_engine},
                            {"phase", "full_load_gate"},
                            {"full_load_subprocess_active", subprocess},
                            {"copy_checkpoint_active", !subprocess},
                        },
                    });
                    return;
                }
                const bool subprocess = full_load_subprocess_running(conn_id);
                const bool copy_checkpoint =
                    conn_has_active_copy_checkpoints(capture_pg.raw, conn_id);
                if (subprocess || copy_checkpoint) {
                    const CaptureBranchCounts branches =
                        count_capture_branch_tables(capture_pg.raw, conn_id, db_engine);
                    log_write(capture_pg.raw, {
                        .level = LogLevel::Info,
                        .component = "cdc_daemon",
                        .message = "capture branch bypass: full load active but eligible tables remain",
                        .batch_id = round_batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"phase", "capture_branch_bypass"},
                            {"db_engine", db_engine},
                            {"main_branch_tables", branches.main_branch},
                            {"stream_branch_tables", branches.stream_branch},
                            {"full_load_subprocess_active", subprocess},
                            {"copy_checkpoint_active", copy_checkpoint},
                        },
                    });
                }
                const int cleared = clear_stale_cdc_in_progress(capture_pg.raw, conn_id, db_engine);
                if (cleared > 0) {
                    log_write(capture_pg.raw, {
                        .level = LogLevel::Info,
                        .component = "cdc_daemon",
                        .message = "stale cdc_in_progress cleared before capture",
                        .batch_id = round_batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {{"cleared", cleared}, {"db_engine", db_engine}},
                    });
                }
                if (run_conn_capture_slice(cfg, capture_pg.raw, conn_id, round_batch_id) != 0) {
                    mark_capture_position_failed(
                        capture_pg.raw, conn_id, "conn capture slice finished with errors");
                    failures.fetch_add(1);
                }
            } catch (const std::exception& ex) {
                failures.fetch_add(1);
                mark_capture_position_failed(capture_pg.raw, conn_id, ex.what());
                log_write(capture_pg.raw, {
                    .level = LogLevel::Error,
                    .component = "cdc_daemon",
                    .message = "conn capture failed",
                    .batch_id = round_batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {{"error", ex.what()}},
                });
            }
        };
    };

    std::vector<std::thread> capture_threads;
    capture_threads.reserve(conn_ids.size());
    for (const auto& conn_id : conn_ids) {
        try {
            capture_threads.emplace_back(make_capture_fn(conn_id));
        } catch (...) {
            for (auto& t : capture_threads) {
                if (t.joinable()) {
                    t.join();
                }
            }
            throw;
        }
    }

    for (auto& thread : capture_threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    std::vector<std::thread> threads;
    threads.reserve(conn_ids.size());

    for (const auto& conn_id : conn_ids) {
        try {
            threads.emplace_back([&, conn_id]() {
                PgConn pg(cfg.datasync.conn_string());
                if (!pg.raw) {
                    failures.fetch_add(1);
                    return;
                }
                try {
                    if (run_one_cycle(cfg, pg.raw, conn_id, sync_apply, round) != 0) {
                        failures.fetch_add(1);
                    }
                } catch (const std::exception& ex) {
                    failures.fetch_add(1);
                    log_write(pg.raw, {
                        .level = LogLevel::Error,
                        .component = "cdc_daemon",
                        .message = "daemon cycle failed",
                        .batch_id = make_batch_id(),
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {{"error", ex.what()}},
                    });
                }
            });
        } catch (...) {
            for (auto& t : threads) {
                if (t.joinable()) {
                    t.join();
                }
            }
            throw;
        }
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    {
        PgConn health_pg(cfg.datasync.conn_string());
        if (health_pg.raw) {
            scan_apply_health_alerts(health_pg.raw, round_batch_id);
            scan_capture_health_alerts(health_pg.raw, round_batch_id);
        }
    }

    return failures.load() > 0 ? 1 : 0;
}

int round_idle_seconds(const CdcConfig& cdc) {
    return cdc.round_idle_seconds > 0 ? cdc.round_idle_seconds : 5;
}

std::vector<std::string> reload_daemon_conn_ids(PGconn* log_pg, AppConfig& cfg) {
    std::lock_guard<std::mutex> lock(app_config_mutex());
    reload_connections_nolock(log_pg, cfg);
    return all_conn_ids(cfg);
}

AppConfig snapshot_app_config(const AppConfig& cfg) {
    std::lock_guard<std::mutex> lock(app_config_mutex());
    return cfg;
}

void run_startup_full_load_sweep(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::vector<std::string>& conn_ids,
    int& exit_code) {
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon startup full-load sweep started",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
    });
    for (const auto& conn_id : conn_ids) {
        if (g_shutdown.load()) {
            break;
        }
        try {
            const auto sweep = try_run_daemon_full_load_isolated(cfg, log_pg, conn_id, make_batch_id());
            if (sweep && sweep->ran && sweep->exit_code != 0) {
                exit_code = 1;
            }
        } catch (const std::exception& ex) {
            exit_code = 1;
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_daemon",
                .message = "daemon startup full-load sweep failed",
                .batch_id = make_batch_id(),
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"error", ex.what()}},
            });
        }
    }
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon startup full-load sweep completed",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
    });
}

std::vector<std::string> wait_for_daemon_connections(PGconn* log_pg, AppConfig& cfg) {
    auto conn_ids = reload_daemon_conn_ids(log_pg, cfg);
    while (conn_ids.empty() && !g_shutdown.load()) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_daemon",
            .message = "daemon waiting for active cdc_catalog.connections",
            .batch_id = make_batch_id(),
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"hint", "add sources to config.json and restart container, or INSERT connections"}},
        });
        sleep_interruptible(30);
        conn_ids = reload_daemon_conn_ids(log_pg, cfg);
    }
    return conn_ids;
}

}  // namespace

int run_cdc_daemon(AppConfig& cfg, PGconn* log_pg, bool once) {
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    RuntimeConfig runtime;
    runtime.reload(log_pg);

    const auto conn_ids_initial = wait_for_daemon_connections(log_pg, cfg);
    if (conn_ids_initial.empty()) {
        return 0;
    }

    const std::string batch_id = make_batch_id();

    for (const auto& conn_id : conn_ids_initial) {
        const std::string db_engine = conn_engine(cfg, conn_id);
        clear_stale_full_load_in_progress(log_pg, conn_id, db_engine, 30);
        clear_stale_cdc_in_progress(log_pg, conn_id, db_engine);
        try_recover_stale_full_load_lock(log_pg, conn_id, batch_id);
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "stale in_progress flags cleared",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"db_engine", db_engine}},
        });
    }

    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon started",
        .batch_id = batch_id,
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"conn_ids", conn_ids_initial.size()},
            {"once", once},
            {"round_idle_seconds", round_idle_seconds(cfg.cdc)},
            {"slice_max_seconds", cfg.cdc.slice_max_seconds},
            {"slice_max_events", cfg.cdc.slice_max_events},
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix_mode", "conn_id"},
            {"apply_mode", once ? "sync" : "background"},
        },
    });

    if (!once) {
        ensure_apply_worker_loops(cfg, conn_ids_initial);
    }

    int cycles = 0;
    int exit_code = 0;
    std::size_t last_conn_count = conn_ids_initial.size();

    const AppConfig startup_cfg = snapshot_app_config(cfg);
    std::thread startup_full_load_thread([startup_cfg, conn_ids_initial]() {
        try {
            PgConn pg(startup_cfg.datasync.conn_string());
            if (!pg.raw) {
                return;
            }
            int sweep_exit = 0;
            run_startup_full_load_sweep(startup_cfg, pg.raw, conn_ids_initial, sweep_exit);
        } catch (const std::exception& ex) {
            try {
                PgConn pg(startup_cfg.datasync.conn_string());
                if (pg.raw) {
                    log_write(pg.raw, {
                        .level = LogLevel::Error,
                        .component = "cdc_daemon",
                        .message = "startup full-load sweep failed",
                        .context = {{"error", ex.what()}},
                    });
                }
            } catch (...) {
            }
        }
    });

    while (!g_shutdown.load()) {
        auto conn_ids = reload_daemon_conn_ids(log_pg, cfg);
        if (conn_ids.empty()) {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_daemon",
                .message = "daemon idle: no active connections",
                .batch_id = make_batch_id(),
                .conn_id = std::nullopt,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
            });
        } else {
            if (conn_ids.size() > last_conn_count) {
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_daemon",
                    .message = "daemon connections reloaded",
                    .batch_id = make_batch_id(),
                    .conn_id = std::nullopt,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"conn_ids", static_cast<int>(conn_ids.size())},
                        {"previous_conn_ids", static_cast<int>(last_conn_count)},
                    },
                });
                const AppConfig sweep_cfg = snapshot_app_config(cfg);
                run_startup_full_load_sweep(sweep_cfg, log_pg, conn_ids, exit_code);
                last_conn_count = conn_ids.size();
            }
            const AppConfig round_cfg = snapshot_app_config(cfg);
            if (!once) {
                ensure_apply_worker_loops(round_cfg, conn_ids);
            }
            if (run_parallel_daemon_round(round_cfg, conn_ids, once) != 0) {
                exit_code = 1;
            }
        }

        if (g_shutdown.load()) {
            break;
        }

        runtime.reload(log_pg);
        maybe_run_scheduled_retention_maintenance(log_pg, runtime);
        sleep_interruptible(round_idle_seconds(cfg.cdc));

        cycles += 1;
        if (once) {
            break;
        }
    }

    if (startup_full_load_thread.joinable()) {
        startup_full_load_thread.join();
    }
    join_background_threads();

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon stopped",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"cycles", cycles}, {"once", once}, {"had_errors", exit_code != 0}},
    });

    return exit_code;
}
