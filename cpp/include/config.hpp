#pragma once

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

struct PgConfig {
    std::string host{"localhost"};
    std::uint16_t port{5432};
    std::string database{"DataLake"};
    std::string user;
    std::string password;
    std::string sslmode;
    std::string conn_string() const;
};

struct MariaDbSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{3306};
    std::string db_name;
    std::string user;
    std::string password;
    nlohmann::json extras = nlohmann::json::object();
};

struct MssqlSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{1433};
    std::string db_name;
    std::string user;
    std::string password;
    nlohmann::json extras = nlohmann::json::object();
};

struct MongoSource {
    std::string conn_id;
    std::string host{"localhost"};
    std::uint16_t port{27017};
    std::string db_name;
    std::string user;
    std::string password;
    std::string replica_set;
    bool replica_set_in_extras{false};
    nlohmann::json extras = nlohmann::json::object();
};

/** CDC daemon tuning — config.json `cdc` object; hot-reloaded each daemon round (C07). */
struct CdcConfig {
    int round_idle_seconds{5};
    int slice_max_seconds{180};
    int slice_max_events{10'000'000};
    /** S13: capture worker shards (mod catalog_id). */
    int capture_worker_count{1};
    /** A07: claim event_id in apply_dedup before lake write. */
    bool apply_dedup{true};
    /** A07b: write apply_audit rows. */
    bool apply_audit{false};
    /** S14: MSSQL idle-slot replay. */
    bool mssql_replay_on_idle{false};
    /** M01: publish Kafka heartbeat records on idle capture. */
    bool kafka_heartbeat{true};
    /** S09: `auto` | `cli` | `native` — native RPL with CLI fallback when auto. */
    std::string binlog_mode{"auto"};
    /** E03: emit Kafka null tombstone after delete events. */
    bool kafka_tombstones{true};
    /** T12: MariaDB apply re-SELECT by PK to fill missing/null columns. */
    bool reselect_on_apply{false};
    /** R12: wrap CDC JSON in a CloudEvents envelope (Avro/SR out of scope). */
    bool cloudevents{false};
    /** M04: inject trace_id (= batch_id) into obs_log context. */
    bool tracing{true};
    /** R08: emit rows to cdc_catalog.lineage_events. */
    bool openlineage{false};
    /** E05: attach Query/DDL SQL to source.query when seen in binlog. */
    bool include_source_query{true};
    /** D02: publish schema-change events to `{prefix}.__schema`. */
    bool schema_events{true};
    /**
     * T05: content-based routing rules (JSON array).
     * Each rule: {"when":{"op":"d"}|"after.field":"v", "topic_suffix":".deletes"}
     */
    nlohmann::json content_routing = nlohmann::json::array();
    /**
     * T06: drop events whose op is in this list (e.g. ["d"] to skip deletes at capture).
     */
    nlohmann::json message_filter_exclude_ops = nlohmann::json::array();
};

struct AppConfig {
    /** Control plane: catalog, logs, apply_position, apply_batch_stats. */
    PgConfig datasync;
    /** Lake targets: full-load COPY and CDC apply INSERT/COPY. */
    PgConfig datalake;
    std::vector<MariaDbSource> mariadb_sources;
    std::vector<MssqlSource> mssql_sources;
    std::vector<MongoSource> mongo_sources;
    CdcConfig cdc;
    /** Path used for C07 hot-reload of the `cdc` section. */
    std::string config_path;
};

CdcConfig default_cdc_config();

/** Load DataSync + DataLake PG from config.json (project root). Sources from cdc_catalog.connections. */
AppConfig load_config(const std::string& path);
AppConfig load_config_auto(const char* argv0);
/** @deprecated use load_config_auto */
AppConfig load_config_from_env();

/** Returns "mariadb", "mssql", or "mongodb" for a conn_id in config. */
std::string conn_engine(const AppConfig& cfg, const std::string& conn_id);
