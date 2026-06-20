#pragma once

#include <nlohmann/json.hpp>

#include <string>
#include <utility>
#include <vector>

int topic_bucket(const std::string& schema, const std::string& table, int num_buckets);

std::string topic_for_catalog(
    const std::string& prefix,
    const std::string& schema,
    const std::string& table,
    const std::string& mode,
    int num_buckets);

std::vector<std::string> topics_for_tables(
    const std::string& prefix,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::string& mode,
    int num_buckets);

std::string kafka_message_key(const std::string& schema, const std::string& table);

std::string kafka_message_key_for_row(
    const std::string& schema,
    const std::string& table,
    const nlohmann::json* row,
    const std::string& pk_columns);
