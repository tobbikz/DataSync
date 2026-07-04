#include "catalog_sync.hpp"
#include "capture_common.hpp"
#include "cdc_daemon.hpp"
#include "config.hpp"
#include "connections.hpp"
#include "daemon_full_load.hpp"
#include "kafka_apply.hpp"
#include "mariadb_ddl_sync.hpp"
#include "mssql_ddl_sync.hpp"
#include "mariadb_full_load.hpp"
#include "mariadb_kafka_capture.hpp"
#include "mongo_kafka_capture.hpp"
#include "mssql_full_load.hpp"
#include "mssql_kafka_capture.hpp"
#include "mongo_full_load.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "reconcile_lite.hpp"
#include "schema_migrate.hpp"
#include "pipeline_defaults.hpp"

#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"
#endif

#include <atomic>
#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

void emit_or_stderr(PGconn* pg, const LogEvent& event) {
    if (!log_write(pg, event)) {
        std::cerr << "[log_write failed] " << event.message << '\n';
    }
}

void run_log_retention(PGconn* log_pg, RuntimeConfig& runtime, const std::string& batch_id, const std::string& component) {
    const int days =
        runtime.get_int("logs_retention_days", pipeline_defaults::kLogsRetentionDaysDefault, "global");
    const long long purged = purge_logs(log_pg, days);
    if (purged >= 0) {
        emit_or_stderr(log_pg, {
            .level = LogLevel::Info,
            .component = component,
            .message = "logs retention purge completed",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"retention_days", days}, {"rows_deleted", purged}},
        });
    }
}

int run_discover(const AppConfig& cfg, PGconn* log_pg, const std::string& batch_id) {
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    run_log_retention(log_pg, runtime, batch_id, "catalog");

    emit_or_stderr(log_pg, {
        .level = LogLevel::Info,
        .component = "catalog",
        .message = "discover started",
        .batch_id = batch_id,
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"mariadb_sources", cfg.mariadb_sources.size()},
                     {"mssql_sources", cfg.mssql_sources.size()},
                     {"mongo_sources", cfg.mongo_sources.size()}},
    });

    if (cfg.mariadb_sources.empty() && cfg.mssql_sources.empty() && cfg.mongo_sources.empty()) {
        emit_or_stderr(log_pg, {
            .level = LogLevel::Warning,
            .component = "catalog",
            .message = "discover skipped: no active connections",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return 0;
    }

    int failures = sync_all_catalogs(cfg, log_pg, batch_id);

    int total = 0;
    int active = 0;
    try {
        fetch_catalog_headline_counts(cfg, total, active);
    } catch (const std::exception& ex) {
        failures += 1;
        emit_or_stderr(log_pg, {
            .level = LogLevel::Error,
            .component = "catalog",
            .message = "discover count query failed",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}},
        });
    }

    if (failures == 0) {
        emit_or_stderr(log_pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "discover completed",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"catalog_total", total}, {"catalog_active", active}, {"failures", 0}},
        });
        return 0;
    }

    emit_or_stderr(log_pg, {
        .level = LogLevel::Warning,
        .component = "catalog",
        .message = "discover completed with errors",
        .batch_id = batch_id,
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"catalog_total", total}, {"catalog_active", active}, {"failures", failures}},
    });
    return 1;
}

int run_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id,
    bool skip_onboard = false) {
    if (conn_id && !conn_id->empty()) {
        const int rc = run_conn_full_load(cfg, log_pg, batch_id, *conn_id);
        if (rc == 0 && !skip_onboard) {
            if (!onboard_conn_after_full_load(cfg, log_pg, *conn_id, conn_engine(cfg, *conn_id), batch_id)) {
                return 1;
            }
        }
        return rc;
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    run_log_retention(log_pg, runtime, batch_id, "mariadb_load");
    const auto mariadb_stats = run_mariadb_full_load(cfg, log_pg, batch_id);
    run_log_retention(log_pg, runtime, batch_id, "mssql_load");
    const auto mssql_stats = run_mssql_full_load(cfg, log_pg, batch_id);
    run_log_retention(log_pg, runtime, batch_id, "mongo_load");
    const auto mongo_stats = run_mongo_full_load(cfg, log_pg, batch_id);
    if (full_load_process_exit_code(mariadb_stats) != 0 || full_load_process_exit_code(mssql_stats) != 0 ||
        full_load_process_exit_code(mongo_stats) != 0) {
        return 1;
    }
    if (!skip_onboard) {
        std::set<std::string> affected_conn_ids = mariadb_stats.conn_ids;
        affected_conn_ids.insert(mssql_stats.conn_ids.begin(), mssql_stats.conn_ids.end());
        affected_conn_ids.insert(mongo_stats.conn_ids.begin(), mongo_stats.conn_ids.end());
        for (const auto& cid : affected_conn_ids) {
            if (!onboard_conn_after_full_load(cfg, log_pg, cid, conn_engine(cfg, cid), batch_id)) {
                return 1;
            }
        }
    }
    return 0;
}

int run_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& schema,
    const std::optional<std::string>& table) {
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const std::string engine = conn_engine(cfg, conn_id);
    if (engine == "mssql") {
        run_log_retention(log_pg, runtime, make_batch_id(), "mssql_ddl_sync");
        return run_mssql_ddl_sync_cli(cfg, log_pg, conn_id, schema, table);
    }
    run_log_retention(log_pg, runtime, make_batch_id(), "mariadb_ddl_sync");
    return run_mariadb_ddl_sync_cli(cfg, log_pg, conn_id, schema, table);
}

int run_capture_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count) {
    const std::string batch_id = make_batch_id();
    const std::string engine = conn_engine(cfg, conn_id);
    nlohmann::json out = {{"batch_id", batch_id}, {"conn_id", conn_id}, {"db_engine", engine}};

    if (engine == "mssql") {
        const auto stats =
            run_mssql_kafka_capture_slice(cfg, log_pg, conn_id, batch_id, worker_id, worker_count);
        out["events_published"] = stats.events_published;
        out["errors"] = stats.errors;
        out["tables"] = stats.tables;
        out["duration_ms"] = stats.duration_ms;
        std::cout << out.dump() << '\n';
        return stats.errors > 0 ? 1 : 0;
    }
    if (engine == "mongodb") {
        const auto stats =
            run_mongo_kafka_capture_slice(cfg, log_pg, conn_id, batch_id, worker_id, worker_count);
        out["events_published"] = stats.events_published;
        out["errors"] = stats.errors;
        out["collections"] = stats.collections;
        out["duration_ms"] = stats.duration_ms;
        std::cout << out.dump() << '\n';
        return stats.errors > 0 ? 1 : 0;
    }

    const auto stats =
        run_mariadb_kafka_capture_slice(cfg, log_pg, conn_id, batch_id, worker_id, worker_count);
    out["events_published"] = stats.events_published;
    out["errors"] = stats.errors;
    out["duration_ms"] = stats.duration_ms;
    out["binlog_file"] = stats.binlog_file;
    out["binlog_position"] = stats.binlog_position;
    std::cout << out.dump() << '\n';
    return stats.errors > 0 ? 1 : 0;
}

void print_usage(const char* prog) {
    std::cerr << "Usage:\n"
              << "  " << prog << " discover\n"
              << "  " << prog << " full-load [--conn-id ID] [--skip-onboard]\n"
              << "  " << prog << " onboard-pending [--conn-id ID] [--hot-only|--cold-only]\n"
              << "  " << prog << " reconcile-lite [--conn-id ID] [--hot-only|--cold-only] [--sample-pct N]\n"
              << "  " << prog << " ddl-sync --conn-id ID [--schema S] [--table T]\n"
              << "  " << prog << " kafka-apply --conn-id ID\n"
              << "  " << prog << " capture --conn-id ID\n"
              << "  " << prog << " migrate [--baseline] [--lake] [--diagnostics]\n"
              << "  " << prog << " daemon [--once]\n"
              << "  " << prog << " [--config PATH] <command> ...\n"
              << "  PG: config.json at project root (datasync + datalake)\n"
              << "  Sources: cdc_catalog.connections (alias = conn_id)\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string command;
    std::string conn_id;
    std::optional<std::string> source_schema;
    std::optional<std::string> source_table;
    bool once = false;
    bool skip_onboard = false;
    bool hot_only = false;
    bool cold_only = false;
    int sample_pct = 100;
    bool migrate_baseline = false;
    bool migrate_lake = false;
    bool migrate_diagnostics = false;
    std::string config_path;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--conn-id" && i + 1 < argc) {
            conn_id = argv[++i];
        } else if (arg == "--schema" && i + 1 < argc) {
            source_schema = argv[++i];
        } else if (arg == "--table" && i + 1 < argc) {
            source_table = argv[++i];
        } else if (arg == "--once") {
            once = true;
        } else if (arg == "--skip-onboard") {
            skip_onboard = true;
        } else if (arg == "--hot-only") {
            hot_only = true;
        } else if (arg == "--cold-only") {
            cold_only = true;
        } else if (arg == "--sample-pct" && i + 1 < argc) {
            sample_pct = std::atoi(argv[++i]);
        } else if (arg == "--baseline") {
            migrate_baseline = true;
        } else if (arg == "--lake") {
            migrate_lake = true;
        } else if (arg == "--diagnostics") {
            migrate_diagnostics = true;
        } else if (arg == "--config" && i + 1 < argc) {
            config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if (command.empty()) {
            command = arg;
        } else {
            std::cerr << "unknown argument: " << arg << '\n';
            print_usage(argv[0]);
            return 2;
        }
    }

    if (command != "discover" && command != "full-load" && command != "ddl-sync" &&
        command != "kafka-apply" && command != "capture" &&
        command != "daemon" && command != "onboard-pending" &&
        command != "reconcile-lite" &&
        command != "migrate") {
        print_usage(argv[0]);
        return 2;
    }

    if (hot_only && cold_only) {
        std::cerr << "use only one of --hot-only or --cold-only\n";
        return 2;
    }

    if ((command == "kafka-apply" || command == "ddl-sync" || command == "capture") &&
        conn_id.empty()) {
        std::cerr << command << " requires --conn-id\n";
        print_usage(argv[0]);
        return 2;
    }

    try {
#ifdef HAVE_MONGOC
        struct MongoLibraryShutdown {
            ~MongoLibraryShutdown() { mongo_library_cleanup(); }
        } mongo_shutdown;
#endif
        AppConfig cfg = config_path.empty() ? load_config_auto(argv[0]) : load_config(config_path);
        const std::string batch_id = make_batch_id();
        PgConn log_pg(cfg.datasync.conn_string());
        reload_connections(log_pg.raw, cfg);

        if (command == "discover") {
            return run_discover(cfg, log_pg.raw, batch_id);
        }
        if (command == "migrate") {
            SchemaMigrateOptions opts;
            opts.baseline = migrate_baseline;
            opts.lake = migrate_lake;
            opts.diagnostics = migrate_diagnostics;
            opts.incremental =
                !migrate_baseline && !migrate_lake && !migrate_diagnostics;
            std::optional<PgConn> lake_pg;
            if (opts.lake) {
                lake_pg.emplace(cfg.datalake.conn_string());
            }
            return run_schema_migrate(
                log_pg.raw,
                lake_pg ? lake_pg->raw : nullptr,
                opts);
        }
        if (command == "full-load") {
            return run_full_load(
                cfg,
                log_pg.raw,
                batch_id,
                conn_id.empty() ? std::nullopt : std::optional(conn_id),
                skip_onboard);
        }
        if (command == "onboard-pending") {
            const CatalogHotTier tier = hot_only ? CatalogHotTier::HotOnly
                                                 : (cold_only ? CatalogHotTier::ColdOnly
                                                              : CatalogHotTier::All);
            return run_onboard_pending(
                cfg,
                log_pg.raw,
                batch_id,
                conn_id.empty() ? std::nullopt : std::optional(conn_id),
                tier);
        }
        if (command == "reconcile-lite") {
            PgConn lake_pg(cfg.datalake.conn_string());
            if (!lake_pg.raw) {
                std::cerr << "reconcile-lite: datalake connection failed\n";
                return 1;
            }
            const CatalogHotTier tier = hot_only ? CatalogHotTier::HotOnly
                                                 : (cold_only ? CatalogHotTier::ColdOnly
                                                              : CatalogHotTier::All);
            return run_reconcile_lite(
                cfg,
                log_pg.raw,
                lake_pg.raw,
                batch_id,
                conn_id.empty() ? std::nullopt : std::optional(conn_id),
                tier,
                sample_pct);
        }
        if (command == "ddl-sync") {
            return run_ddl_sync(cfg, log_pg.raw, conn_id, source_schema, source_table);
        }
        if (command == "kafka-apply") {
            int failures = 0;
            auto run_pool = [&](CatalogHotTier tier, int workers) {
                if (workers <= 1) {
                    if (run_kafka_apply_native_cli(cfg, log_pg.raw, conn_id, 0, 1, tier) != 0) {
                        failures += 1;
                    }
                    return;
                }
                std::vector<std::thread> threads;
                std::atomic<int> pool_failures{0};
                threads.reserve(static_cast<std::size_t>(workers));
                for (int worker_id = 0; worker_id < workers; ++worker_id) {
                    threads.emplace_back([&, worker_id]() {
                        PgConn worker_pg(cfg.datasync.conn_string());
                        if (!worker_pg.raw) {
                            pool_failures.fetch_add(1);
                            return;
                        }
                        if (run_kafka_apply_native_cli(
                                cfg, worker_pg.raw, conn_id, worker_id, workers, tier) != 0) {
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
        if (command == "capture") {
            return run_capture_cli(cfg, log_pg.raw, conn_id, 0, kCaptureWorkerCount);
        }
        if (command == "daemon") {
            return run_cdc_daemon(cfg, log_pg.raw, once);
        }
        print_usage(argv[0]);
        return 2;
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << '\n';
        return 1;
    }
}
