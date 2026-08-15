#pragma once

#include <nlohmann/json.hpp>

#include <string>
#include <utility>
#include <vector>

int topic_bucket(const std::string& schema, const std::string& table, int num_buckets);

/** Stable non-negative hash partition in [0, num_partitions). */
int partition_index_for_key(const std::string& key, int num_partitions);

/** Positive modulo for catalog_id / worker sharding. */
int positive_mod(long long value, int modulus);

/** Kafka partition for CDC produce: catalog_id % partitions (bucketed topics). */
int kafka_produce_partition(long long catalog_id, int topic_partitions);

/** True when partition p is owned by apply worker worker_id. Requires topic_partitions % worker_count == 0. */
bool kafka_partition_owned_by_worker(int partition, int worker_id, int worker_count);

std::string topic_for_catalog(
    const std::string& prefix,
    const std::string& schema,
    const std::string& table,
    const std::string& mode,
    int num_buckets);

std::string topic_for_catalog_table(
    const std::string& prefix,
    const std::string& schema,
    const std::string& table,
    const std::string& default_mode,
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
