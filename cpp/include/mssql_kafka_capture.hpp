#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>

struct MssqlCaptureStats {
    std::string batch_id;
    int events_published{0};
    int errors{0};
    int tables{0};
    long long duration_ms{0};
};

/** Seed cdc_mssql_lsn to max LSN after full-load (parity with MariaDB capture_position T0). */
int seed_mssql_cdc_lsn_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id);

MssqlCaptureStats run_mssql_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id,
    int worker_id = 0,
    int worker_count = 1);
