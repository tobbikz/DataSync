#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>

struct MariaDbCaptureStats {
    std::string batch_id;
    int events_published{0};
    int errors{0};
    int ddl_recorded{0};
    long long duration_ms{0};
    std::string binlog_file;
    long long binlog_position{0};
};

MariaDbCaptureStats run_mariadb_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id,
    int worker_id = 0,
    int worker_count = 1);
