#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <optional>
#include <string>

struct KafkaApplyStats {
    long long events_seen{0};
    long long events_applied{0};
    int errors{0};
    std::string stop_reason;
    long long duration_ms{0};
};

// Apply one JSON batch from stdin (used by Python bridge). Writes result JSON to stdout.
int run_kafka_apply_stdin_batch(const AppConfig& cfg, PGconn* log_pg, const std::string& conn_id);

// Native librdkafka consumer apply slice. Writes stats JSON to stdout.
int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    int worker_id,
    int worker_count);

