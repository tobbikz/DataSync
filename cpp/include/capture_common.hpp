#pragma once

#include "config.hpp"
#include "runtime_config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <optional>
#include <set>
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

/** Kafka bootstrap: KAFKA_BOOTSTRAP env → runtime_config → localhost:9092 */
struct KafkaBootstrapResolved {
    std::string bootstrap;
    std::string source;  // env | runtime_config | default
};

KafkaBootstrapResolved resolve_kafka_bootstrap(RuntimeConfig& runtime, const std::string& conn_id);

/** Topic prefix is always conn_id (not runtime_config). */
std::string topic_prefix_for_conn(const std::string& conn_id);

std::string runtime_topic_prefix(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

/** Log why capture/apply found zero eligible tables (cdc_catalog.logs). */
void log_cdc_skip_no_tables(
    PGconn* pg,
    const std::string& component,
    const std::string& pipeline,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
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

void ensure_capture_kafka_topics(
    PGconn* pg,
    const std::string& component,
    const std::string& batch_id,
    const std::string& conn_id,
    const CaptureRuntimeConfig& rcfg,
    const std::vector<std::pair<std::string, std::string>>& tables);

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

long long reset_apply_consumer_offset(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition,
    long long offset);

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
    const std::string& db_engine,
    const std::string& batch_id = "");

/** Bookmark Kafka offsets in catalog.engine_meta when capture_during_full_load is set. */
bool seed_stream_capture_bookmark_if_needed(
    PGconn* pg,
    RuntimeConfig& runtime,
    const std::string& conn_id,
    long long catalog_id,
    const std::string& db_engine,
    const std::string& batch_id);

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

/** Reset stale cdc_in_progress rows (crash/reload) back to success. Returns rows updated. */
int clear_stale_cdc_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& db_engine);

/** Release cdc_in_progress flags after a failed capture/apply slice. */
void rollback_cdc_in_progress_ids(PGconn* pg, const std::set<long long>& catalog_ids);

int count_full_load_pending(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& tier,
    const std::string& db_engine);

/** Pending full-load rows for conn across any tier (tier-mismatch diagnostics). */
int count_full_load_pending_any_tier(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

/** Distinct service_tier values with pending full-load for conn. */
std::vector<std::string> list_full_load_pending_tiers(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

struct ApplySkipReasonCounts {
    int active_total{0};
    int needs_full_load{0};
    int cdc_disabled{0};
    int no_pk{0};
    int skipped_status{0};
    int apply_ready{0};
};

/** Why apply found zero tables (per conn + optional tier filter). */
ApplySkipReasonCounts fetch_apply_skip_reasons(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
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
