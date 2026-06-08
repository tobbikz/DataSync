#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

// Unified CDC daemon: all tiers × all conn_ids from env (MariaDB + MSSQL + MongoDB).
int run_cdc_daemon(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once);
