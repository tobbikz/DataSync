#include "daemon_full_load.hpp"

#include "capture_common.hpp"
#include "config.hpp"
#include "full_load_checkpoint.hpp"
#include "mariadb_full_load.hpp"
#include "mongo_full_load.hpp"
#include "mssql_full_load.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "pipeline_defaults.hpp"
#include "runtime_config.hpp"

#include <dirent.h>

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <signal.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

struct ConnFullLoadSlot {
    std::mutex mu;
    std::atomic<pid_t> child_pid{-1};
};

std::mutex g_conn_lock_registry_mu;
std::map<std::string, std::shared_ptr<ConnFullLoadSlot>> g_conn_slots;

std::shared_ptr<ConnFullLoadSlot> conn_full_load_slot(const std::string& conn_id) {
    std::lock_guard<std::mutex> guard(g_conn_lock_registry_mu);
    auto& slot = g_conn_slots[conn_id];
    if (!slot) {
        slot = std::make_shared<ConnFullLoadSlot>();
    }
    return slot;
}

bool cmdline_contains_full_load_for_conn(const std::string& conn_id) {
    DIR* dir = opendir("/proc");
    if (!dir) {
        return false;
    }

    bool found = false;
    for (dirent* ent = readdir(dir); ent != nullptr; ent = readdir(dir)) {
        if (!ent->d_name || ent->d_name[0] < '0' || ent->d_name[0] > '9') {
            continue;
        }
        const std::string cmdline_path = std::string("/proc/") + ent->d_name + "/cmdline";
        std::ifstream in(cmdline_path, std::ios::binary);
        if (!in) {
            continue;
        }
        std::string cmdline((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        std::vector<std::string> args;
        args.reserve(8);
        for (std::size_t i = 0; i < cmdline.size();) {
            const std::size_t start = i;
            while (i < cmdline.size() && cmdline[i] != '\0') {
                ++i;
            }
            if (start < i) {
                args.emplace_back(cmdline.data() + start, i - start);
            }
            ++i;
        }
        bool seen_full_load = false;
        for (std::size_t i = 0; i < args.size(); ++i) {
            if (args[i] == "full-load") {
                seen_full_load = true;
            }
            if (seen_full_load && args[i] == "--conn-id" && i + 1 < args.size() && args[i + 1] == conn_id) {
                found = true;
                break;
            }
        }
        if (found) {
            break;
        }
    }
    closedir(dir);
    return found;
}

bool full_load_subprocess_alive_for_conn(const std::string& conn_id) {
    const auto slot = conn_full_load_slot(conn_id);
    const pid_t tracked = slot->child_pid.load();
    if (tracked > 0 && kill(tracked, 0) == 0) {
        return true;
    }
    return cmdline_contains_full_load_for_conn(conn_id);
}

struct SpawnWaitResult {
    int exit_code{127};
    bool timed_out{false};
    pid_t child_pid{-1};
};

SpawnWaitResult spawn_wait_with_timeout(
    const std::vector<std::string>& args,
    int timeout_seconds,
    ConnFullLoadSlot* slot) {
    SpawnWaitResult out;
    if (args.empty()) {
        return out;
    }

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    const pid_t pid = fork();
    if (pid < 0) {
        return out;
    }
    if (pid == 0) {
        execvp(argv[0], argv.data());
        _exit(127);
    }

    out.child_pid = pid;
    if (slot) {
        slot->child_pid.store(pid);
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(std::max(1, timeout_seconds));

    for (;;) {
        int status = 0;
        const pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid) {
            if (WIFEXITED(status)) {
                out.exit_code = WEXITSTATUS(status);
            } else {
                out.exit_code = 1;
            }
            break;
        }
        if (got < 0 && errno != EINTR) {
            break;
        }

        if (std::chrono::steady_clock::now() >= deadline) {
            out.timed_out = true;
            kill(pid, SIGTERM);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            if (kill(pid, 0) == 0) {
                kill(pid, SIGKILL);
            }
            int status = 0;
            waitpid(pid, &status, 0);
            out.exit_code = 124;
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (slot) {
        slot->child_pid.store(-1);
    }
    return out;
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

bool full_load_conn_busy(const std::string& conn_id) {
    const auto slot = conn_full_load_slot(conn_id);
    std::unique_lock<std::mutex> lock(slot->mu, std::try_to_lock);
    return !lock.owns_lock();
}

bool full_load_subprocess_running(const std::string& conn_id) {
    return full_load_subprocess_alive_for_conn(conn_id);
}

bool full_load_gate_blocks_cdc(PGconn* pg, const std::string& conn_id) {
    if (full_load_subprocess_running(conn_id)) {
        return true;
    }
    if (pg != nullptr && conn_has_active_copy_checkpoints(pg, conn_id)) {
        return true;
    }
    return false;
}

bool try_recover_stale_full_load_lock(
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id) {
    if (full_load_subprocess_alive_for_conn(conn_id)) {
        return false;
    }

    const auto slot = conn_full_load_slot(conn_id);
    const pid_t tracked = slot->child_pid.load();
    if (tracked > 0) {
        int status = 0;
        waitpid(tracked, &status, WNOHANG);
        slot->child_pid.store(-1);
    }

    std::unique_lock<std::mutex> lock(slot->mu, std::try_to_lock);
    if (!lock.owns_lock()) {
        if (log_pg) {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_daemon",
                .message = "full-load lock still held in-process; no live subprocess (restart daemon if stuck)",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"tracked_child_pid", static_cast<long long>(tracked)}},
            });
        }
        return false;
    }

    slot->child_pid.store(-1);
    if (log_pg) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_daemon",
            .message = "stale full-load conn lock recovered (no live subprocess)",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
    }
    return true;
}

std::optional<DaemonFullLoadOutcome> try_run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id) {
    const auto slot = conn_full_load_slot(conn_id);
    std::unique_lock<std::mutex> lock(slot->mu, std::try_to_lock);
    if (!lock.owns_lock()) {
        return std::nullopt;
    }
    return run_daemon_full_load_isolated(cfg, log_pg, conn_id, batch_id);
}

int run_conn_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id) {
    const std::string engine = conn_engine(cfg, conn_id);
    const std::optional<std::string> conn_filter = conn_id;

    if (engine == "mssql") {
        const auto stats = run_mssql_full_load(cfg, log_pg, batch_id, conn_filter);
        return full_load_process_exit_code(stats);
    }
    if (engine == "mongodb") {
        const auto stats = run_mongo_full_load(cfg, log_pg, batch_id, conn_filter);
        return full_load_process_exit_code(stats);
    }
    const auto stats = run_mariadb_full_load(cfg, log_pg, batch_id, conn_filter);
    return full_load_process_exit_code(stats);
}

DaemonFullLoadOutcome run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id) {
    DaemonFullLoadOutcome outcome;
    const std::string db_engine = conn_engine(cfg, conn_id);
    outcome.pending_tables = count_full_load_pending(log_pg, conn_id, db_engine);
    if (outcome.pending_tables <= 0) {
        return outcome;
    }

    outcome.ran = true;

    if (!full_load_subprocess_running(conn_id)) {
        reset_full_load_in_progress_for_conn(log_pg, conn_id, db_engine);
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_daemon",
        .message = "daemon full-load phase started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"pending_tables", outcome.pending_tables},
            {"phase", "full_load"},
        },
    });

    const std::string binary = self_binary_path();
    std::vector<std::string> args = {
        binary,
        "full-load",
        "--conn-id",
        conn_id,
        "--skip-onboard",
    };
    if (const char* config_path = std::getenv("DATASYNC_CONFIG")) {
        args.push_back("--config");
        args.push_back(config_path);
    }

    const auto slot = conn_full_load_slot(conn_id);
    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int timeout_minutes = runtime.get_int(
        "full_load_daemon_timeout_minutes",
        pipeline_defaults::kFullLoadDaemonSubprocessTimeoutMinutes,
        "global");
    const SpawnWaitResult spawn = spawn_wait_with_timeout(
        args,
        timeout_minutes * 60,
        slot.get());
    outcome.exit_code = spawn.exit_code;

    if (spawn.timed_out) {
        reset_full_load_in_progress_for_conn(log_pg, conn_id, db_engine);
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_daemon",
            .message = "daemon full-load subprocess timed out; child terminated",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"timeout_minutes", timeout_minutes},
                {"child_pid", static_cast<long long>(spawn.child_pid)},
                {"phase", "full_load"},
            },
        });
        if (outcome.exit_code == 0) {
            outcome.exit_code = 124;
        }
    }

    outcome.pending_after = count_full_load_pending(log_pg, conn_id, db_engine);
    outcome.tables_loaded = std::max(0, outcome.pending_tables - outcome.pending_after);

    const int pending_onboard = count_full_load_pending_onboard(log_pg, conn_id, db_engine);
    if (pending_onboard > 0) {
        if (!onboard_conn_after_full_load(cfg, log_pg, conn_id, db_engine, batch_id)) {
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
            {"db_engine", db_engine},
            {"pending_tables", outcome.pending_tables},
            {"pending_after", outcome.pending_after},
            {"pending_onboard", pending_onboard},
            {"tables_loaded", outcome.tables_loaded},
            {"exit_code", outcome.exit_code},
            {"timed_out", spawn.timed_out},
            {"phase", "full_load"},
        },
    });

    return outcome;
}
