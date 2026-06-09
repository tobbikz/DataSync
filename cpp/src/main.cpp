#include "catalog_sync.hpp"
#include "capture_common.hpp"
#include "cdc_daemon.hpp"
#include "cdc_reconcile.hpp"
#include "config.hpp"
#include "connections.hpp"
#include "daemon_full_load.hpp"
#include "kafka_apply.hpp"
#include "mariadb_cdc.hpp"
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
#include "runtime_config.hpp"

#include <iostream>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>

namespace {

void emit_or_stderr(PGconn* pg, const LogEvent& event) {
    if (!log_write(pg, event)) {
        std::cerr << "[log_write failed] " << event.message << '\n';
    }
}

void run_log_retention(PGconn* log_pg, RuntimeConfig& runtime, const std::string& batch_id, const std::string& component) {
    const int days = runtime.get_int("logs_retention_days", 7, "global");
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
    const std::optional<std::string>& tier,
    const std::optional<std::string>& conn_id,
    bool skip_onboard = false) {
    if (conn_id && !conn_id->empty()) {
        if (!tier || tier->empty()) {
            throw std::runtime_error("full-load with --conn-id requires --tier");
        }
        const int rc = run_conn_full_load(cfg, log_pg, batch_id, *conn_id, tier);
        if (rc == 0 && !skip_onboard) {
            if (!onboard_conn_after_full_load(cfg, log_pg, *conn_id, *tier, conn_engine(cfg, *conn_id), batch_id)) {
                return 1;
            }
        }
        return rc;
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    run_log_retention(log_pg, runtime, batch_id, "mariadb_load");
    const auto mariadb_stats = run_mariadb_full_load(cfg, log_pg, batch_id, tier);
    run_log_retention(log_pg, runtime, batch_id, "mssql_load");
    const auto mssql_stats = run_mssql_full_load(cfg, log_pg, batch_id, tier);
    run_log_retention(log_pg, runtime, batch_id, "mongo_load");
    const auto mongo_stats = run_mongo_full_load(cfg, log_pg, batch_id, tier);
    if (full_load_process_exit_code(mariadb_stats) != 0 || full_load_process_exit_code(mssql_stats) != 0 ||
        full_load_process_exit_code(mongo_stats) != 0) {
        return 1;
    }
    return 0;
}

int run_cdc(const AppConfig& cfg, PGconn* log_pg, const std::string& batch_id, const std::optional<std::string>& tier) {
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    run_log_retention(log_pg, runtime, batch_id, "mariadb_cdc");
    const auto stats = run_mariadb_cdc(cfg, log_pg, batch_id, tier);
    return stats.conns_failed == 0 ? 0 : 1;
}

int run_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    const std::optional<std::string>& schema,
    const std::optional<std::string>& table) {
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const std::string engine = conn_engine(cfg, conn_id);
    if (engine == "mssql") {
        run_log_retention(log_pg, runtime, make_batch_id(), "mssql_ddl_sync");
        return run_mssql_ddl_sync_cli(cfg, log_pg, conn_id, tier, schema, table);
    }
    run_log_retention(log_pg, runtime, make_batch_id(), "mariadb_ddl_sync");
    return run_mariadb_ddl_sync_cli(cfg, log_pg, conn_id, tier, schema, table);
}

int run_capture_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    int worker_id,
    int worker_count) {
    const std::string batch_id = make_batch_id();
    const std::string engine = conn_engine(cfg, conn_id);
    nlohmann::json out = {{"batch_id", batch_id}, {"conn_id", conn_id}, {"tier", tier}, {"db_engine", engine}};

  const std::optional<std::string> service_tier{tier};
    if (engine == "mssql") {
        const auto stats =
            run_mssql_kafka_capture_slice(cfg, log_pg, conn_id, service_tier, batch_id, worker_id, worker_count);
        out["events_published"] = stats.events_published;
        out["errors"] = stats.errors;
        out["tables"] = stats.tables;
        out["duration_ms"] = stats.duration_ms;
        std::cout << out.dump() << '\n';
        return stats.errors > 0 ? 1 : 0;
    }
    if (engine == "mongodb") {
        const auto stats =
            run_mongo_kafka_capture_slice(cfg, log_pg, conn_id, service_tier, batch_id, worker_id, worker_count);
        out["events_published"] = stats.events_published;
        out["errors"] = stats.errors;
        out["collections"] = stats.collections;
        out["duration_ms"] = stats.duration_ms;
        std::cout << out.dump() << '\n';
        return stats.errors > 0 ? 1 : 0;
    }

    const auto stats =
        run_mariadb_kafka_capture_slice(cfg, log_pg, conn_id, service_tier, batch_id, worker_id, worker_count);
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
              << "  " << prog << " full-load [--tier TIER] [--conn-id ID] [--skip-onboard]\n"
              << "  " << prog << " cdc [--tier TIER]\n"
              << "  " << prog << " ddl-sync --conn-id ID [--tier T] [--schema S] [--table T]\n"
              << "  " << prog << " kafka-apply-batch  (JSON batch on stdin)\n"
              << "  " << prog << " kafka-apply --conn-id ID [--tier T] [--worker-id N] [--worker-count M]\n"
              << "  " << prog << " capture --conn-id ID --tier T [--worker-id N] [--worker-count M]\n"
              << "  " << prog << " reconcile --conn-id ID --tier T\n"
              << "  " << prog << " reconcile-loop [--tier T] [--once]\n"
              << "  " << prog << " daemon [--once]\n"
              << "  " << prog << " [--config PATH] <command> ...\n"
              << "  PG: config.json at project root (datasync + datalake)\n"
              << "  Sources: cdc_catalog.connections (alias = conn_id)\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string command;
    std::optional<std::string> tier;
    std::string conn_id;
    int worker_id = 0;
    int worker_count = 1;
    std::optional<std::string> source_schema;
    std::optional<std::string> source_table;
    bool once = false;
    bool skip_onboard = false;
    std::string config_path;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--tier" && i + 1 < argc) {
            tier = argv[++i];
        } else if (arg == "--conn-id" && i + 1 < argc) {
            conn_id = argv[++i];
        } else if (arg == "--schema" && i + 1 < argc) {
            source_schema = argv[++i];
        } else if (arg == "--table" && i + 1 < argc) {
            source_table = argv[++i];
        } else if (arg == "--worker-id" && i + 1 < argc) {
            worker_id = std::stoi(argv[++i]);
        } else if (arg == "--worker-count" && i + 1 < argc) {
            worker_count = std::stoi(argv[++i]);
        } else if (arg == "--once") {
            once = true;
        } else if (arg == "--skip-onboard") {
            skip_onboard = true;
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

    if (command != "discover" && command != "full-load" && command != "cdc" && command != "ddl-sync" &&
        command != "kafka-apply-batch" && command != "kafka-apply" && command != "capture" &&
        command != "reconcile" && command != "reconcile-loop" && command != "daemon") {
        print_usage(argv[0]);
        return 2;
    }

    if ((command == "kafka-apply" || command == "ddl-sync" || command == "capture") && conn_id.empty()) {
        std::cerr << command << " requires --conn-id\n";
        print_usage(argv[0]);
        return 2;
    }
    if (command == "capture" && (!tier || tier->empty())) {
        std::cerr << "capture requires --tier\n";
        print_usage(argv[0]);
        return 2;
    }
    if (command == "reconcile" && conn_id.empty()) {
        std::cerr << "reconcile requires --conn-id\n";
        print_usage(argv[0]);
        return 2;
    }
    if (command == "reconcile" && (!tier || tier->empty())) {
        std::cerr << "reconcile requires --tier\n";
        print_usage(argv[0]);
        return 2;
    }

    try {
        AppConfig cfg = config_path.empty() ? load_config_auto(argv[0]) : load_config(config_path);
        const std::string batch_id = make_batch_id();
        PgConn log_pg(cfg.datasync.conn_string());
        reload_connections(log_pg.raw, cfg);

        if (command == "discover") {
            return run_discover(cfg, log_pg.raw, batch_id);
        }
        if (command == "full-load") {
            return run_full_load(
                cfg,
                log_pg.raw,
                batch_id,
                tier,
                conn_id.empty() ? std::nullopt : std::optional(conn_id),
                skip_onboard);
        }
        if (command == "ddl-sync") {
            return run_ddl_sync(cfg, log_pg.raw, conn_id, tier, source_schema, source_table);
        }
        if (command == "kafka-apply-batch") {
            return run_kafka_apply_stdin_batch(
                cfg,
                log_pg.raw,
                cfg.mariadb_sources.empty() ? "MARIADB_LOCAL" : cfg.mariadb_sources.front().conn_id);
        }
        if (command == "kafka-apply") {
            return run_kafka_apply_native_cli(cfg, log_pg.raw, conn_id, tier, worker_id, worker_count);
        }
        if (command == "capture") {
            return run_capture_cli(cfg, log_pg.raw, conn_id, *tier, worker_id, worker_count);
        }
        if (command == "reconcile") {
            RuntimeConfig runtime;
            runtime.reload(log_pg.raw);
            run_log_retention(log_pg.raw, runtime, batch_id, "reconcile");
            return run_reconcile_cli(cfg, log_pg.raw, conn_id, *tier);
        }
        if (command == "reconcile-loop") {
            RuntimeConfig runtime;
            runtime.reload(log_pg.raw);
            run_log_retention(log_pg.raw, runtime, batch_id, "reconcile");
            return run_reconcile_loop(cfg, log_pg.raw, tier, once);
        }
        if (command == "daemon") {
            return run_cdc_daemon(cfg, log_pg.raw, once);
        }
        return run_cdc(cfg, log_pg.raw, batch_id, tier);
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << '\n';
        return 1;
    }
}
