#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <optional>
#include <string>

struct CdcRunStats {
    int conns_processed{0};
    int conns_failed{0};
    long long events_applied{0};
    long long upserts{0};
    long long deletes{0};
};

CdcRunStats run_mariadb_cdc(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::optional<std::string>& service_tier);
