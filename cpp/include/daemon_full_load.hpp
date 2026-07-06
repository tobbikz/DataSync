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

/** Main + stream branch table counts for capture gate decisions. */
struct CaptureBranchCounts {
    int main_branch{0};
    int stream_branch{0};
};

CaptureBranchCounts count_capture_branch_tables(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

/** Branch-aware: true = defer capture (no main/stream tables eligible). */
bool full_load_gate_blocks_capture(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

/** Strict gate: defer onboard while full-load subprocess or copy checkpoint is active. */
bool full_load_gate_blocks_onboard(PGconn* pg, const std::string& conn_id);

/** @deprecated Alias for full_load_gate_blocks_onboard — legacy call sites. */
bool full_load_gate_blocks_cdc(PGconn* pg, const std::string& conn_id);

/** Resolve db_engine for conn_id from cdc_catalog.connections (empty if unknown). */
std::string conn_db_engine_from_pg(PGconn* pg, const std::string& conn_id);

/**
 * Remove copy-phase checkpoints that block CDC without a live full-load subprocess:
 * orphaned (catalog not full_load_in_progress) or stale (no progress within stale_minutes).
 * Returns number of checkpoint rows deleted.
 */
int clear_stale_copy_checkpoints_blocking_cdc(
    PGconn* pg,
    const std::string& conn_id,
    int stale_minutes,
    const std::string& batch_id);

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
