#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>

struct MongoCaptureStats {
    std::string batch_id;
    int events_published{0};
    int errors{0};
    int collections{0};
    long long duration_ms{0};
};

MongoCaptureStats run_mongo_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id,
    int worker_id = 0,
    int worker_count = 1);

#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"

/** Seed cdc_mongo_resume at T0 for one collection (skip if row exists). */
bool seed_mongo_cdc_resume_for_collection_if_absent(
    PGconn* log_pg,
    MongoConn& mongo,
    const std::string& conn_id,
    const std::string& database,
    const std::string& collection);
#endif

/** Seed all catalog collections for conn; skips rows that already exist. */
int seed_mongo_cdc_resume_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id);
