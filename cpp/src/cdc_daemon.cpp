#include "cdc_daemon.hpp"
#include "capture_common.hpp"
#include "catalog_sync.hpp"
#include "cdc_pre_apply.hpp"
#include "cdc_reconcile.hpp"
#include "config.hpp"
#include "connections.hpp"
#include "daemon_full_load.hpp"
#include "kafka_apply.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "service_tiers.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <optional>
#include <thread>
#include <vector>
#include <algorithm>

namespace {

std::atomic<bool> g_shutdown{false};
std::atomic<int> g_catalog_sync_round{0};

void on_signal(int) {
    g_shutdown.store(true);
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

int run_apply_workers(
    const AppConfig& cfg,
    const std::string& conn_id,
    const std::string& tier,
    int tier_apply_worker_count) {
    const int worker_count = tier_apply_worker_count > 0 ? tier_apply_worker_count : 1;
    if (worker_count <= 1) {
        PgConn pg(cfg.datasync.conn_string());
        if (!pg.raw) {
            return 1;
        }
        return run_kafka_apply_native_cli(cfg, pg.raw, conn_id, tier, 0, 1);
    }

    std::vector<std::thread> threads;
    std::atomic<int> failures{0};
    threads.reserve(static_cast<std::size_t>(worker_count));
    for (int worker_id = 0; worker_id < worker_count; ++worker_id) {
        threads.emplace_back([&, worker_id]() {
            try {
                PgConn pg(cfg.datasync.conn_string());
                if (!pg.raw) {
                    failures.fetch_add(1);
                    return;
                }
                const int rc = run_kafka_apply_native_cli(cfg, pg.raw, conn_id, tier, worker_id, worker_count);
                if (rc != 0) {
                    failures.fetch_add(1);
                }
            } catch (const std::exception&) {
                failures.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    return failures.load() > 0 ? 1 : 0;
}

int run_one_cycle(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const ServiceTier& tier) {
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
        .context = {{"tier", tier.tier_code}, {"db_engine", db_engine}},
    });

    std::optional<DaemonFullLoadOutcome> full_load_outcome;
    std::thread full_load_thread;
    const int pending_before =
        count_full_load_pending(log_pg, conn_id, tier.tier_code, db_engine);
    if (pending_before > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "daemon full-load started in background; CDC slice running concurrently",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", tier.tier_code},
                {"db_engine", db_engine},
                {"pending_tables", pending_before},
                {"phase", "full_load_background"},
            },
        });
        full_load_thread = std::thread(
            [&cfg, &full_load_outcome, conn_id, tier_code = tier.tier_code, batch_id, pending_before]() {
                try {
                    PgConn pg(cfg.datasync.conn_string());
                    if (!pg.raw) {
                        DaemonFullLoadOutcome failed;
                        failed.ran = true;
                        failed.exit_code = 1;
                        failed.pending_tables = pending_before;
                        full_load_outcome = failed;
                        return;
                    }
                    const auto outcome =
                        try_run_daemon_full_load_isolated(cfg, pg.raw, conn_id, tier_code, batch_id);
                    if (outcome) {
                        full_load_outcome = *outcome;
                    } else {
                        DaemonFullLoadOutcome skipped;
                        skipped.pending_tables = pending_before;
                        full_load_outcome = skipped;
                    }
                } catch (const std::exception&) {
                    DaemonFullLoadOutcome failed;
                    failed.ran = true;
                    failed.exit_code = 1;
                    failed.pending_tables = pending_before;
                    full_load_outcome = failed;
                }
            });
    }

    const PreApplyCycleResult pre = run_pre_apply_cycle(cfg, log_pg, conn_id, tier.tier_code, batch_id);
    const int pre_rc = pre.errors > 0 ? 1 : 0;
    RuntimeConfig apply_runtime;
    apply_runtime.reload(log_pg);
    const int apply_workers = apply_runtime.get_int(
        "apply_worker_count", tier.apply_worker_count, "cdc_kafka_apply", conn_id);
    const int apply_rc = run_apply_workers(cfg, conn_id, tier.tier_code, apply_workers);
    int cycle_errors = (pre_rc != 0 ? 1 : 0) + (apply_rc != 0 ? 1 : 0);

    if (full_load_thread.joinable()) {
        full_load_thread.join();
    }

    try {
        const int cleared = clear_stale_cdc_in_progress(log_pg, conn_id, tier.tier_code, db_engine);
        if (cleared > 0) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_daemon",
                .message = "stale cdc_in_progress cleared after daemon cycle",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"cleared", cleared}, {"tier", tier.tier_code}, {"db_engine", db_engine}},
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
            .context = {{"error", ex.what()}, {"tier", tier.tier_code}},
        });
    }

    if (full_load_outcome && !full_load_outcome->ran && pending_before > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "daemon full-load skipped: tier lock held by another full-load",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", tier.tier_code},
                {"db_engine", db_engine},
                {"pending_tables", pending_before},
                {"phase", "full_load_background"},
            },
        });
    }

    if (full_load_outcome && full_load_outcome->ran) {
        const DaemonFullLoadOutcome& full_load = *full_load_outcome;
        const bool partial_ok = full_load.tables_loaded > 0 && full_load.exit_code != 0;
        log_write(log_pg, {
            .level = full_load.exit_code == 0 ? LogLevel::Info : LogLevel::Warning,
            .component = "cdc_daemon",
            .message = full_load.exit_code == 0
                           ? "daemon full-load background finished"
                           : (partial_ok ? "daemon full-load background partial"
                                         : "daemon full-load background failed"),
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", tier.tier_code},
                {"db_engine", db_engine},
                {"full_load_exit", full_load.exit_code},
                {"pending_tables", full_load.pending_tables},
                {"pending_after", full_load.pending_after},
                {"tables_loaded", full_load.tables_loaded},
                {"phase", "full_load_background"},
            },
        });
        if (full_load.exit_code != 0) {
            cycle_errors += 1;
        }
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
            {"tier", tier.tier_code},
            {"db_engine", db_engine},
            {"pre_apply_exit", pre_rc},
            {"apply_exit", apply_rc},
            {"full_load_concurrent", pending_before > 0},
            {"errors", cycle_errors},
        },
    });

    return cycle_errors == 0 ? 0 : 1;
}

int run_parallel_daemon_round(
    const AppConfig& cfg,
    const std::vector<std::string>& conn_ids,
    const std::vector<ServiceTier>& tiers) {
    std::atomic<int> failures{0};
    const std::string round_batch_id = make_batch_id();

    {
        PgConn catalog_pg(cfg.datasync.conn_string());
        if (catalog_pg.raw) {
            RuntimeConfig runtime;
            runtime.reload(catalog_pg.raw);
            const int sync_every =
                runtime.get_int("catalog_sync_interval_rounds", 12, "catalog", "");
            const int round = g_catalog_sync_round.fetch_add(1) + 1;
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

    for (const auto& conn_id : conn_ids) {
        PgConn capture_pg(cfg.datasync.conn_string());
        if (!capture_pg.raw) {
            failures.fetch_add(1);
            continue;
        }
        try {
            const std::string db_engine = conn_engine(cfg, conn_id);
            const int cleared = clear_stale_cdc_in_progress(capture_pg.raw, conn_id, std::nullopt, db_engine);
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
                failures.fetch_add(1);
            }
        } catch (const std::exception& ex) {
            failures.fetch_add(1);
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
    }

    std::vector<std::thread> threads;
    threads.reserve(conn_ids.size() * tiers.size());

    for (const auto& tier : tiers) {
        for (const auto& conn_id : conn_ids) {
            threads.emplace_back([&, tier, conn_id]() {
                PgConn pg(cfg.datasync.conn_string());
                if (!pg.raw) {
                    failures.fetch_add(1);
                    return;
                }
                try {
                    if (run_one_cycle(cfg, pg.raw, conn_id, tier) != 0) {
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
                        .context = {{"tier", tier.tier_code}, {"error", ex.what()}},
                    });
                }
            });
        }
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    return failures.load() > 0 ? 1 : 0;
}

int round_idle_seconds(const CdcConfig& cdc) {
    return cdc.round_idle_seconds > 0 ? cdc.round_idle_seconds : 5;
}

std::vector<std::string> reload_daemon_conn_ids(PGconn* log_pg, AppConfig& cfg) {
    reload_connections(log_pg, cfg);
    return all_conn_ids(cfg);
}

void run_startup_full_load_sweep(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::vector<std::string>& conn_ids,
    const std::vector<ServiceTier>& tiers,
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
    for (const auto& tier : tiers) {
        if (g_shutdown.load()) {
            break;
        }
        for (const auto& conn_id : conn_ids) {
            if (g_shutdown.load()) {
                break;
            }
            try {
                const auto sweep = try_run_daemon_full_load_isolated(
                    cfg, log_pg, conn_id, tier.tier_code, make_batch_id());
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
                    .context = {{"tier", tier.tier_code}, {"error", ex.what()}},
                });
            }
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

int run_cdc_daemon(
    AppConfig& cfg,
    PGconn* log_pg,
    bool once) {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int retention_days = runtime.get_int("logs_retention_days", 7, "global");
    purge_logs(log_pg, retention_days);

    const auto conn_ids_initial = wait_for_daemon_connections(log_pg, cfg);
    if (conn_ids_initial.empty()) {
        return 0;
    }

    const auto tiers = load_service_tiers(cfg);
    if (tiers.empty()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_daemon",
            .message = "daemon failed: no active tiers in config.json cdc.tiers",
            .batch_id = make_batch_id(),
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return 1;
    }

    const std::string batch_id = make_batch_id();

    for (const auto& conn_id : conn_ids_initial) {
        const std::string db_engine = conn_engine(cfg, conn_id);
        clear_stale_full_load_in_progress(log_pg, conn_id, db_engine);
        clear_stale_cdc_in_progress(log_pg, conn_id, std::nullopt, db_engine);
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

    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap(runtime, "");
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon started",
        .batch_id = batch_id,
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tiers", tiers.size()},
            {"conn_ids", conn_ids_initial.size()},
            {"once", once},
            {"parallel_tiers", true},
            {"round_idle_seconds", round_idle_seconds(cfg.cdc)},
            {"slice_max_seconds", cfg.cdc.slice_max_seconds},
            {"slice_max_events", cfg.cdc.slice_max_events},
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix_mode", "conn_id"},
            {"reconcile_embedded", true},
        },
    });

    int cycles = 0;
    int exit_code = 0;
    std::size_t last_conn_count = conn_ids_initial.size();

    std::thread startup_full_load_thread([cfg, conn_ids_initial, tiers]() {
        try {
            PgConn pg(cfg.datasync.conn_string());
            if (!pg.raw) {
                return;
            }
            int sweep_exit = 0;
            run_startup_full_load_sweep(cfg, pg.raw, conn_ids_initial, tiers, sweep_exit);
        } catch (const std::exception&) {
        }
    });

    std::thread reconcile_thread([&cfg]() {
        try {
            PgConn pg(cfg.datasync.conn_string());
            if (!pg.raw) {
                return;
            }
            (void)run_reconcile_loop(cfg, pg.raw, std::nullopt, false, &g_shutdown);
        } catch (const std::exception&) {
            // reconcile errors are logged inside run_reconcile_loop
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
                run_startup_full_load_sweep(cfg, log_pg, conn_ids, tiers, exit_code);
                last_conn_count = conn_ids.size();
            }
            if (run_parallel_daemon_round(cfg, conn_ids, tiers) != 0) {
                exit_code = 1;
            }
        }

        if (g_shutdown.load()) {
            break;
        }

        runtime.reload(log_pg);
        sleep_interruptible(round_idle_seconds(cfg.cdc));

        cycles += 1;
        if (once) {
            break;
        }
    }

    if (startup_full_load_thread.joinable()) {
        startup_full_load_thread.join();
    }
    if (reconcile_thread.joinable()) {
        reconcile_thread.join();
    }

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

    // Keep process exit 0 so systemd restart/stop is not marked failed after transient apply errors.
    (void)exit_code;
    return 0;
}
