#pragma once

#include "config.hpp"

#include <libpq-fe.h>

#include <optional>
#include <string>

/** One-shot reconcile: row counts source vs lake for conn+tier. */
int run_reconcile_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier);

/** Scheduled loop using reconcile_interval_hours / reconcile_enabled from runtime_config. */
int run_reconcile_loop(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::optional<std::string>& tier,
    bool once = false);
