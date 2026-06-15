#pragma once

#include "config.hpp"

#include <atomic>
#include <libpq-fe.h>

#include <string>

/** One-shot reconcile: row counts source vs lake for conn. */
int run_reconcile_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id);

/** Scheduled loop using reconcile_interval_hours / reconcile_enabled from runtime_config. */
int run_reconcile_loop(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once = false,
    std::atomic<bool>* shutdown = nullptr);
