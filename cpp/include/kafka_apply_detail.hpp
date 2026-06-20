#pragma once

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <map>
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
    int events_seen_in_slice{0};
    int dedup_skipped{0};
    long long kafka_consumer_lag{0};
};

struct ApplyBatchOptions {
    bool audit_enabled{true};
    std::string source_system{"MariaDB"};
    int apply_staleness_seconds{900};
    int apply_inactive_seconds{3600};
    std::unordered_map<std::string, TableSliceState> slice_table_state;
    /** Optional: failed table events re-queued by flush layer (not committed). */
    std::vector<ApplyEvent>* failed_events_out{nullptr};
    /** Optional: per-table apply failures in this batch. */
    int* table_errors_out{nullptr};
    /** Optional: per-table parse_skipped count from main loop. */
    const std::map<std::pair<std::string, std::string>, int>* parse_skipped_by_table{nullptr};
    /** Optional: per-table dropped_unrecoverable (mutated by apply_events_batch). */
    std::map<std::pair<std::string, std::string>, int>* dropped_unrecoverable_by_table{nullptr};
};

nlohmann::json apply_events_batch(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    const std::vector<ApplyEvent>& events,
    const ApplyBatchOptions& options = {});

/** Fill schema_name/table_name from event_id (pos|schema.table|op|pk) when missing. */
bool fill_table_key_from_event_id(ApplyEvent& event, const std::string& db_engine = "mariadb");

/** Resolve schema/table from cdc_catalog.catalog when catalog_id > 0; overwrites with lake names. */
bool resolve_event_lake_key_from_catalog(
    PGconn* pg,
    const std::string& conn_id,
    ApplyEvent& event);

/** Attribute unrecoverable drop to catalog source_schema/source_table when possible. */
void record_dropped_unrecoverable(
    PGconn* pg,
    const ApplyEvent& event,
    std::map<std::pair<std::string, std::string>, int>* by_table);

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

/** Fill missing/null columns from CDC before image (partial UPDATE payloads). */
void enrich_apply_row_from_payload(
    nlohmann::json& row,
    const nlohmann::json& data,
    const std::string& op,
    const std::vector<std::string>& pk_cols);

/** COPY cell formatter for lake apply (shared with unit tests). */
std::string json_cell_csv(
    const nlohmann::json& val,
    const std::string& pg_type = "",
    bool mssql_text = false);

}  // namespace kafka_apply_detail
