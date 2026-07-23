#pragma once

#include <nlohmann/json.hpp>

#include <optional>
#include <string>
#include <vector>

struct CdcEvent {
    std::string op;
    std::string conn_id;
    std::string db_engine{"mariadb"};
    std::string source_database;
    std::string schema_name;
    std::string table_name;
    nlohmann::json before = nullptr;
    nlohmann::json after = nullptr;
    std::string gtid;
    std::string mssql_seqval;
    std::string binlog_file;
    std::optional<long long> binlog_pos;
    std::optional<long long> ts_ms;
    nlohmann::json resume_token = nullptr;
    std::string collection;
    std::string ingestion_ts;

    nlohmann::json to_kafka_dict() const;
};

/** Recursively sanitize string keys/values for nlohmann::json::dump(). */
nlohmann::json json_sanitize_for_kafka(const nlohmann::json& value);

/** Safe Kafka payload JSON (invalid UTF-8 replaced, never throws on dump). */
std::string cdc_event_kafka_payload(const CdcEvent& event);

std::string utc_iso_timestamp_now();

nlohmann::json parse_sql_literal(const std::string& raw);

nlohmann::json row_dict_from_columns(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& col_values);

/** Returns c/u/d for known ops; empty string for unknown (callers must skip publish). */
std::string op_char_from_mysql(const std::string& mysql_op);
