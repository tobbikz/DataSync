#pragma once

#include "capture_common.hpp"
#include "config.hpp"

#include <libpq-fe.h>

#include <optional>
#include <string>

/** Run reconcile-lite: COUNT + MAX(PK) + MAX(ts) for catalog CDC tables. */
int run_reconcile_lite(
    const AppConfig& cfg,
    PGconn* log_pg,
    PGconn* lake_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter,
    CatalogHotTier hot_tier,
    int sample_pct);
