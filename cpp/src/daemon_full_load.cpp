#include "daemon_full_load.hpp"

#include "capture_common.hpp"
#include "config.hpp"
#include "mariadb_full_load.hpp"
#include "mongo_full_load.hpp"
#include "mssql_full_load.hpp"
#include "obs_log.hpp"

#include <sys/wait.h>
#include <unistd.h>
#include <algorithm>
#include <cstdlib>
#include <vector>

namespace {

int spawn_wait(const std::vector<std::string>& args) {
    if (args.empty()) {
        return 127;
    }
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    const pid_t pid = fork();
    if (pid < 0) {
        return 127;
    }
    if (pid == 0) {
        execvp(argv[0], argv.data());
        _exit(127);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
        return 127;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return 1;
}

std::string self_binary_path() {
    char buf[4096];
    const ssize_t n = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (n <= 0) {
        return "DataSync";
    }
    buf[n] = '\0';
    return std::string(buf);
}

}  // namespace

int run_conn_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier) {
    const std::string engine = conn_engine(cfg, conn_id);
    const std::optional<std::string> conn_filter = conn_id;

    if (engine == "mssql") {
        const auto stats = run_mssql_full_load(cfg, log_pg, batch_id, service_tier, conn_filter);
        return full_load_process_exit_code(stats);
    }
    if (engine == "mongodb") {
        const auto stats = run_mongo_full_load(cfg, log_pg, batch_id, service_tier, conn_filter);
        return full_load_process_exit_code(stats);
    }
    const auto stats = run_mariadb_full_load(cfg, log_pg, batch_id, service_tier, conn_filter);
    return full_load_process_exit_code(stats);
}

DaemonFullLoadOutcome run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id) {
    DaemonFullLoadOutcome outcome;
    const std::string db_engine = conn_engine(cfg, conn_id);
    outcome.pending_tables = count_full_load_pending(log_pg, conn_id, tier, db_engine);
    if (outcome.pending_tables <= 0) {
        const int pending_any_tier = count_full_load_pending_any_tier(log_pg, conn_id, db_engine);
        if (pending_any_tier > 0) {
            const auto pending_tiers = list_full_load_pending_tiers(log_pg, conn_id, db_engine);
            nlohmann::json tier_list = nlohmann::json::array();
            for (const auto& t : pending_tiers) {
                tier_list.push_back(t);
            }
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_daemon",
                .message = "full-load skipped: tables pending on another service_tier",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"tier", tier},
                    {"db_engine", db_engine},
                    {"pending_any_tier", pending_any_tier},
                    {"pending_tiers", tier_list},
                },
            });
        }
        return outcome;
    }

    outcome.ran = true;

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon full-load phase started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tier", tier},
            {"db_engine", db_engine},
            {"pending_tables", outcome.pending_tables},
            {"phase", "full_load"},
        },
    });

    const std::string binary = self_binary_path();
    std::vector<std::string> args = {
        binary,
        "full-load",
        "--tier",
        tier,
        "--conn-id",
        conn_id,
        "--skip-onboard",
    };
    if (const char* config_path = std::getenv("DATASYNC_CONFIG")) {
        args.push_back("--config");
        args.push_back(config_path);
    }
    outcome.exit_code = spawn_wait(args);

    outcome.pending_after = count_full_load_pending(log_pg, conn_id, tier, db_engine);
    outcome.tables_loaded = std::max(0, outcome.pending_tables - outcome.pending_after);

    if (outcome.tables_loaded > 0) {
        if (!onboard_conn_after_full_load(cfg, log_pg, conn_id, tier, db_engine, batch_id)) {
            if (outcome.exit_code == 0) {
                outcome.exit_code = 1;
            }
        }
    } else if (outcome.exit_code == 0 && outcome.pending_tables > 0) {
        outcome.exit_code = 1;
    }

    log_write(log_pg, {
        .level = outcome.exit_code == 0 ? LogLevel::Info : LogLevel::Warning,
        .component = "cdc_daemon",
        .message = outcome.exit_code == 0 ? "daemon full-load phase completed"
                                         : "daemon full-load phase completed with errors",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tier", tier},
            {"db_engine", db_engine},
            {"pending_tables", outcome.pending_tables},
            {"pending_after", outcome.pending_after},
            {"tables_loaded", outcome.tables_loaded},
            {"exit_code", outcome.exit_code},
            {"phase", "full_load"},
        },
    });

    return outcome;
}
