#pragma once

#include "config.hpp"
#include "pipeline_defaults.hpp"
#include "runtime_config.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <set>
#include <string>
#include <vector>

extern std::atomic<bool> g_shutdown;

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
    bool hot{false};
};

/** Which catalog.hot rows to include in capture/apply table fetch. */
enum class CatalogHotTier { All, ColdOnly, HotOnly };

struct CaptureRuntimeConfig {
    int max_seconds{300};
    int max_events{50000};
    int idle_poll_seconds{3};
    int quiet_exit_lagging_chunks{3};
    int heartbeat_seconds{60};
    std::string topic_prefix;
    std::string topic_mode{"bucketed"};
    int topic_buckets{64};
    std::string bootstrap{"localhost:9092"};
    int linger_ms{5};
    int producer_batch{10000};
    int producer_queue_max_messages{500000};
    int producer_queue_max_kbytes{1048576};
    int topic_partitions{6};
    bool mssql_replay_on_idle{false};
};

/** MariaDB: touch cdc_meta.heartbeat when capture is idle. */
void bump_mariadb_capture_heartbeat(MYSQL* mysql);

/** MSSQL/MariaDB: refresh capture idle markers on DataSync when source heartbeat is N/A. */
void bump_capture_heartbeat_pg(PGconn* pg, const std::string& conn_id, const std::string& db_engine);

/** MariaDB: touch capture_position.updated_at at slice start (monitoring liveness). */
void touch_capture_position_slice(PGconn* pg, const std::string& conn_id);

/** MariaDB: record failed capture slice — status + last_error + updated_at. */
void mark_capture_position_failed(PGconn* pg, const std::string& conn_id, const std::string& error);

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

/** Kafka bootstrap: KAFKA_BOOTSTRAP env → localhost:9092 */
struct KafkaBootstrapResolved {
    std::string bootstrap;
    std::string source;  // env | default
};

KafkaBootstrapResolved resolve_kafka_bootstrap();

enum class CatalogPipeline { Capture, Apply };

std::vector<CaptureCatalogTable> fetch_conn_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    const std::string& db_engine,
    CatalogPipeline pipeline,
    CatalogHotTier hot_tier = CatalogHotTier::All);

/** Topic prefix is always conn_id (not runtime_config). */
inline std::string topic_prefix_for_conn(const std::string& conn_id) {
    if (conn_id.empty()) {
        return "UNKNOWN_CONN";
    }
    return conn_id;
}

/** Log why capture/apply found zero eligible tables (cdc_catalog.logs). */
void log_cdc_skip_no_tables(
    PGconn* pg,
    const std::string& component,
    const std::string& pipeline,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& db_engine);

using pipeline_defaults::kApplyWorkerCount;
using pipeline_defaults::kCaptureWorkerCount;
using pipeline_defaults::kFullLoadParallelTables;

// Per-conn suffix + -w{N} when worker_count > 1 (independent Kafka offsets per worker).
// hot_path=true → separate consumer group (datalake-cdc-apply-hot-…).
std::string kafka_apply_consumer_group(
    const std::string& conn_id,
    int worker_id = 0,
    int worker_count = 1,
    bool hot_path = false);

inline std::vector<CaptureCatalogTable> fetch_capture_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    const std::string& db_engine) {
    return fetch_conn_catalog_tables(pg, conn_id, worker_id, worker_count, db_engine, CatalogPipeline::Capture);
}

std::vector<std::string> split_pk_columns(const std::string& pk_columns);

/** Alias for split_pk_columns (lake apply / reconcile). */
inline std::vector<std::string> split_pk(const std::string& pk_columns) {
    return split_pk_columns(pk_columns);
}

void ensure_capture_kafka_topics(
    PGconn* pg,
    const std::string& component,
    const std::string& batch_id,
    const std::string& conn_id,
    const CaptureRuntimeConfig& rcfg,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::set<std::pair<std::string, std::string>>& hot_tables = {});

#ifdef HAVE_RDKAFKA
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

void ensure_kafka_topics_exist(
    const std::string& bootstrap,
    const std::vector<std::string>& topics,
    int partition_count,
    int replication_factor = 1);

#endif

void enable_cdc_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id,
    bool expect_updates = false);

bool enable_cdc_after_full_load_table(
    PGconn* pg,
    long long catalog_id,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& source_schema,
    const std::string& source_table);

void ensure_apply_position_for_catalog(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id);

/** Idempotent apply_position row: clears stale catalog_id / object-key conflicts before insert. */
bool upsert_apply_position(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& source_schema,
    const std::string& source_table,
    const std::string& kafka_topic,
    std::string* error_out = nullptr);

struct BinlogGapRebootResult {
    bool ran{false};
    bool t0_reset{false};
    int tables_flagged{0};
};

BinlogGapRebootResult reboot_conn_after_mariadb_binlog_gap(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& conn_id,
    const std::string& batch_id);

BinlogGapRebootResult reboot_conn_after_mssql_cdc_gap(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& batch_id);

bool seed_stream_capture_bookmark_if_needed(
    PGconn* pg,
    const std::string& conn_id,
    long long catalog_id,
    const std::string& db_engine,
    const std::string& batch_id);

void mark_catalog_full_load_in_progress(PGconn* pg, long long catalog_id);

/** Data copied; onboard_table_after_full_load sets success + cdc_enabled (kafka reset is best-effort). */
void mark_catalog_full_load_data_ready(PGconn* pg, long long catalog_id);

void mark_catalog_skipped(PGconn* pg, long long catalog_id, const std::string& reason);

void mark_catalog_cdc_in_progress(PGconn* pg, long long catalog_id);

void mark_catalog_cdc_success(PGconn* pg, long long catalog_id);

void mark_catalog_cdc_failed(PGconn* pg, long long catalog_id, const std::string& error);

void mark_catalog_reconcile_failed(
    PGconn* pg,
    long long catalog_id,
    const std::string& error,
    bool needs_full_load);

void mark_catalog_reconcile_healed(PGconn* pg, long long catalog_id);

void clear_stale_full_load_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    int stale_minutes = 30);

/** Reset all in-flight full-load rows for conn (e.g. after daemon subprocess timeout). */
void reset_full_load_in_progress_for_conn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

int clear_stale_cdc_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

void rollback_cdc_in_progress_ids(PGconn* pg, const std::set<long long>& catalog_ids);

int count_full_load_pending(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

/** Tables loaded (needs_full_load=false) awaiting Kafka onboard + cdc enable. */
int count_full_load_pending_onboard(
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

ApplySkipReasonCounts fetch_apply_skip_reasons(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

struct FullLoadKafkaResetStats {
    int tables{0};
    int topics_reset{0};
    long long dedup_deleted{0};
    int errors{0};
};

FullLoadKafkaResetStats reset_kafka_apply_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id);

FullLoadKafkaResetStats reset_kafka_apply_after_full_load_table(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id,
    const std::string& batch_id);

/** Kafka reset + cdc enable immediately after a table COPY succeeds. */
bool onboard_table_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id,
    const std::string& batch_id,
    const std::string& source_schema,
    const std::string& source_table);

bool onboard_conn_after_full_load(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id);
