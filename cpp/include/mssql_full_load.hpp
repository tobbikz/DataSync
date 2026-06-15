#pragma once

#include "config.hpp"
#include "mariadb_full_load.hpp"

#include <optional>

FullLoadRunStats run_mssql_full_load(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter = std::nullopt);
