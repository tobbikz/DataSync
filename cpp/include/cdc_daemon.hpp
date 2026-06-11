#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

// Unified CDC daemon: all tiers × all conn_ids from cdc_catalog.connections.
// Returns 0 on success. When once=true, returns non-zero if any cycle had errors.
// Continuous daemon mode keeps exit 0 so systemd restart/stop is not marked failed.
int run_cdc_daemon(
    AppConfig& cfg,
    PGconn* log_pg,
    bool once);
