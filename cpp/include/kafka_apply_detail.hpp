#pragma once

#include "host_metrics.hpp"

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace kafka_apply_detail {

struct ApplyEvent {
    std::string event_id;
    std::string op;
    std::string schema_name;
    std::string table_name;
    std::string topic;
    int partition{0};
    long long offset{0};
    std::string gtid;
    long long catalog_id{0};
    long long ts_ms{0};
    nlohmann::json row;
};

struct TableSliceState {
    bool is_starving{false};
    bool catchup_triggered{false};
    int events_seen_in_slice{0};
    int dedup_skipped{0};
    long long kafka_consumer_lag{0};
};

struct ApplyBatchOptions {
    bool append_only{true};
    bool audit_enabled{true};
    std::string source_system{"MariaDB"};
    std::string service_tier;
    int apply_staleness_seconds{900};
    int apply_inactive_seconds{3600};
    std::unordered_map<std::string, TableSliceState> slice_table_state;
    std::unordered_set<std::string> catchup_tables;
    const HostMetricsSampler* host_sampler{nullptr};
    /** Optional: failed table events re-queued by flush layer (not committed). */
    std::vector<ApplyEvent>* failed_events_out{nullptr};
    /** Optional: per-table apply failures in this batch. */
    int* table_errors_out{nullptr};
};

struct QuietTableRef {
    long long catalog_id{0};
    std::string source_schema;
    std::string source_table;
};

void record_quiet_table_batch_stats(
    PGconn* app_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& service_tier,
    const ApplyBatchOptions& options,
    const std::vector<QuietTableRef>& tables,
    int events_seen_in_slice,
    bool is_starving,
    bool is_inactive);

nlohmann::json apply_events_batch(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    const std::vector<ApplyEvent>& events,
    const ApplyBatchOptions& options = {});

ApplyEvent parse_apply_event(const nlohmann::json& obj);

/** Fill schema_name/table_name from event_id (pos|schema.table|op|pk) when missing. */
bool fill_table_key_from_event_id(ApplyEvent& event, const std::string& db_engine = "mariadb");

/** Resolve schema/table from cdc_catalog.catalog when catalog_id > 0; overwrites with lake names. */
bool resolve_event_lake_key_from_catalog(
    PGconn* pg,
    const std::string& conn_id,
    ApplyEvent& event);

std::unordered_set<std::string> filter_new_event_ids(PGconn* pg, const std::vector<std::string>& event_ids);

nlohmann::json parse_kafka_message_json(const std::string& payload);

bool parse_kafka_payload(
    const char* payload,
    size_t len,
    ApplyEvent& out,
    const std::string& topic,
    int partition,
    long long offset,
    const std::vector<std::string>& pk_cols,
    const std::string& db_engine = "mariadb");

bool parse_kafka_payload(
    const nlohmann::json& data,
    ApplyEvent& out,
    const std::string& topic,
    int partition,
    long long offset,
    const std::vector<std::string>& pk_cols,
    const std::string& db_engine = "mariadb");

}  // namespace kafka_apply_detail
