#include "cdc_daemon.hpp"
#include "catalog_sync.hpp"
#include "cdc_pre_apply.hpp"
#include "config.hpp"
#include "daemon_full_load.hpp"
#include "kafka_apply.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "service_tiers.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include <vector>
#include <algorithm>

namespace {

std::atomic<bool> g_shutdown{false};

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

    const DaemonFullLoadOutcome full_load = run_daemon_full_load_isolated(cfg, log_pg, conn_id, tier.tier_code, batch_id);
    if (full_load.ran) {
        log_write(log_pg, {
            .level = full_load.exit_code == 0 ? LogLevel::Info : LogLevel::Warning,
            .component = "cdc_daemon",
            .message = full_load.exit_code == 0 ? "daemon cycle skipped capture/apply after full-load"
                                               : "daemon cycle skipped capture/apply after full-load errors",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", tier.tier_code},
                {"db_engine", db_engine},
                {"full_load_exit", full_load.exit_code},
                {"pending_tables", full_load.pending_tables},
            },
        });
        return full_load.exit_code == 0 ? 0 : 1;
    }

    const PreApplyCycleResult pre = run_pre_apply_cycle(cfg, log_pg, conn_id, tier.tier_code, batch_id);
    const int pre_rc = pre.errors > 0 ? 1 : 0;
    const int apply_rc = run_apply_workers(cfg, conn_id, tier.tier_code, tier.apply_worker_count);
    const int errors = (pre_rc != 0 ? 1 : 0) + (apply_rc != 0 ? 1 : 0);

    log_write(log_pg, {
        .level = errors ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_daemon",
        .message = errors ? "daemon cycle completed with errors" : "daemon cycle completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tier", tier.tier_code},
            {"db_engine", db_engine},
            {"pre_apply_exit", pre_rc},
            {"apply_exit", apply_rc},
            {"errors", errors},
        },
    });

    return errors == 0 ? 0 : 1;
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
            if (sync_all_catalogs(cfg, catalog_pg.raw, round_batch_id) != 0) {
                failures.fetch_add(1);
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

}  // namespace

int run_cdc_daemon(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once) {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int retention_days = runtime.get_int("logs_retention_days", 7, "global");
    purge_logs(log_pg, retention_days);

    const auto conn_ids = all_conn_ids(cfg);
    if (conn_ids.empty()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_daemon",
            .message = "daemon failed: no sources configured in environment",
            .batch_id = make_batch_id(),
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return 1;
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
            {"conn_ids", conn_ids.size()},
            {"once", once},
            {"parallel_tiers", true},
            {"round_idle_seconds", round_idle_seconds(cfg.cdc)},
            {"slice_max_seconds", cfg.cdc.slice_max_seconds},
            {"slice_max_events", cfg.cdc.slice_max_events},
        },
    });

    int cycles = 0;
    int exit_code = 0;

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
                const DaemonFullLoadOutcome sweep =
                    run_daemon_full_load_isolated(cfg, log_pg, conn_id, tier.tier_code, make_batch_id());
                if (sweep.ran && sweep.exit_code != 0) {
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

    while (!g_shutdown.load()) {
        if (run_parallel_daemon_round(cfg, conn_ids, tiers) != 0) {
            exit_code = 1;
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
