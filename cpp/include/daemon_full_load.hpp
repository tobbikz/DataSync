#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <optional>
#include <string>

struct DaemonFullLoadOutcome {
    bool ran{false};
    int exit_code{0};
    int pending_tables{0};
    int pending_after{0};
    int tables_loaded{0};
};

// Isolated full-load phase for daemon: fork+exec `DataSync full-load --tier --conn-id`.
// Caller should continue capture/apply in the same cycle (apply skips needs_full_load tables).
DaemonFullLoadOutcome run_daemon_full_load_isolated(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& batch_id);

int run_conn_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier);
