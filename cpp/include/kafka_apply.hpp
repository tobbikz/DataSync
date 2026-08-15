#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

/** Long-lived Kafka consumer for daemon apply workers. CLI may pass nullptr. */
struct KafkaApplySession {
    KafkaApplySession() = default;
    ~KafkaApplySession();
    KafkaApplySession(const KafkaApplySession&) = delete;
    KafkaApplySession& operator=(const KafkaApplySession&) = delete;
    void reset();
    void* impl{nullptr};
};

int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    KafkaApplySession* session = nullptr);
