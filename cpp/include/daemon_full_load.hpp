#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

struct DaemonFullLoadOutcome {
    bool ran{false};
    int exit_code{0};
    int pending_tables{0};
    int pending_after{0};
    int tables_loaded{0};
};

// Isolated full-load phase for daemon: fork+exec `DataSync full-load --conn-id`.
// Daemon runs this in a background thread while pre-apply/apply run concurrently.
DaemonFullLoadOutcome run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);

/** True when another thread holds the per-conn_id full-load lock (subprocess + onboard). */
bool full_load_conn_busy(const std::string& conn_id);

/** True when a DataSync full-load subprocess for conn_id is alive (see /proc cmdline). */
bool full_load_subprocess_running(const std::string& conn_id);

/** Defer capture/apply/onboard while full-load subprocess or copy checkpoint is active. */
bool full_load_gate_blocks_cdc(PGconn* pg, const std::string& conn_id);

/** Clear in-process lock when no full-load subprocess is alive (orphaned waiter / dead child). */
bool try_recover_stale_full_load_lock(
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);

/** Acquire conn lock; returns nullopt if another full-load/onboard is already running. */
std::optional<DaemonFullLoadOutcome> try_run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id);

int run_conn_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id);
