#pragma once

#include "config.hpp"

#include <atomic>
#include <libpq-fe.h>

#include <string>
#include <vector>

/** When run_id >= 0, append results to an existing cycle run (no per-conn run row). */
struct ReconcileCycleScope {
    long long run_id{-1};
    std::string batch_id;
    std::vector<long long>* table_timings_ms{nullptr};
    std::string reconcile_mode{"full"};
};

/** One-shot reconcile: row counts source vs lake for conn. */
int run_reconcile_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    std::atomic<bool>* shutdown = nullptr,
    const ReconcileCycleScope* cycle = nullptr);

/** Scheduled loop using reconcile_interval_hours / reconcile_enabled from runtime_config. */
int run_reconcile_loop(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once = false,
    std::atomic<bool>* shutdown = nullptr);
