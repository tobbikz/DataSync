#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

// Unified CDC daemon: all tiers × all conn_ids from cdc_catalog.connections.
int run_cdc_daemon(
    AppConfig& cfg,
    PGconn* log_pg,
    bool once);
