#pragma once

#include "config.hpp"

#include <libpq-fe.h>

#include <optional>
#include <string>

/** Preflight every active connection (or just one) without touching the catalog or the lake.
 *  Prints a JSON report on stdout and mirrors it into cdc_catalog.logs.
 *  Returns 1 when any connection is unusable, 0 otherwise (warnings do not fail). */
int run_test_connection_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter);
