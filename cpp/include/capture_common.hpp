#pragma once

#include "config.hpp"
#include "runtime_config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <string>
#include <vector>

struct st_mysql;
typedef struct st_mysql MYSQL;

struct CaptureCatalogTable {
    long long catalog_id{0};
    std::string conn_id;
    std::string source_database;
    std::string source_schema;
    std::string source_table;
    std::string pk_columns;
    nlohmann::json engine_meta = nlohmann::json::object();
    std::string lake_schema;
    std::string lake_table;
};

struct CaptureRuntimeConfig {
    int max_seconds{300};
    int max_events{50000};
    int idle_poll_seconds{3};
    int heartbeat_seconds{60};
    std::string topic_prefix;
    std::string topic_mode{"bucketed"};
    int topic_buckets{64};
    std::string bootstrap{"localhost:9092"};
    int linger_ms{5};
    int producer_batch{10000};
    int topic_partitions{6};
    bool mssql_replay_on_idle{false};
};

/** MariaDB: touch cdc_meta.heartbeat when capture is idle. */
void bump_mariadb_capture_heartbeat(MYSQL* mysql);

/** MSSQL/MariaDB: refresh capture idle markers on DataSync when source heartbeat is N/A. */
void bump_capture_heartbeat_pg(PGconn* pg, const std::string& conn_id, const std::string& db_engine);

CaptureRuntimeConfig load_mariadb_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc = nullptr);
CaptureRuntimeConfig load_mssql_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc = nullptr);
CaptureRuntimeConfig load_mongo_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc = nullptr);

std::string runtime_topic_prefix(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

// Per-tier suffix avoids Kafka consumer group rebalance when daemon runs tiers in parallel.
std::string kafka_apply_consumer_group(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier);

std::vector<CaptureCatalogTable> fetch_capture_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    int worker_id,
    int worker_count,
    const std::string& db_engine);

std::vector<std::string> split_pk_columns(const std::string& pk_columns);

#ifdef HAVE_RDKAFKA
long long kafka_backlog_messages(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition);

long long reset_apply_offset_to_end(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition);

// Create Kafka topics if missing (idempotent; TOPIC_ALREADY_EXISTS is OK).
void ensure_kafka_topics_exist(
    const std::string& bootstrap,
    const std::vector<std::string>& topics,
    int partition_count,
    int replication_factor = 1);
#endif

void enable_cdc_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& db_engine,
    const std::string& batch_id);

void flag_table_for_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& schema,
    const std::string& table,
    const std::string& db_engine);

/** Set catalog.status while a table is actively loading (full load COPY). */
void mark_catalog_full_load_in_progress(PGconn* pg, long long catalog_id);

/** Mark table skipped (no PK, unsupported) — clears in_progress, disables CDC for this table. */
void mark_catalog_skipped(PGconn* pg, long long catalog_id, const std::string& reason);

/** Set catalog.status while CDC capture or apply is processing a table. */
void mark_catalog_cdc_in_progress(PGconn* pg, long long catalog_id);

/** Mark CDC idle: status success + last_cdc_at. */
void mark_catalog_cdc_success(PGconn* pg, long long catalog_id);

/** Reset stale full_load_in_progress rows (crash/reload) back to pending. */
void clear_stale_full_load_in_progress(PGconn* pg, const std::string& conn_id, const std::string& db_engine);

/** Reset stale cdc_in_progress rows (crash/reload) back to success. */
void clear_stale_cdc_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& db_engine);

int count_full_load_pending(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine);

struct FullLoadKafkaResetStats {
    int tables{0};
    int topics_reset{0};
    long long dedup_deleted{0};
    int errors{0};
};

// Catchup parity after full-load: reset consumer offsets to end + prune dedup for tier tables.
FullLoadKafkaResetStats reset_kafka_apply_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine,
    const std::string& batch_id);

// Returns false if kafka offset reset failed (dedup is NOT cleared on failure).
bool onboard_conn_after_full_load(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine,
    const std::string& batch_id);
