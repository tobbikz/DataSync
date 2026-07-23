#include "kafka_topics.hpp"

#include "cdc_envelope.hpp"
#include "mariadb_schema.hpp"

#include <openssl/sha.h>

#include <iomanip>
#include <set>
#include <sstream>

int topic_bucket(const std::string& schema, const std::string& table, int num_buckets) {
    return partition_index_for_key(schema + "." + table, num_buckets);
}

int partition_index_for_key(const std::string& key, int num_partitions) {
    if (num_partitions <= 0) {
        num_partitions = 1;
    }
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(key.data()), key.size(), digest);
    const unsigned val = (static_cast<unsigned>(digest[0]) << 24) | (static_cast<unsigned>(digest[1]) << 16) |
                         (static_cast<unsigned>(digest[2]) << 8) | static_cast<unsigned>(digest[3]);
    return static_cast<int>(val % static_cast<unsigned>(num_partitions));
}

int positive_mod(long long value, int modulus) {
    if (modulus <= 0) {
        return 0;
    }
    long long r = value % modulus;
    if (r < 0) {
        r += modulus;
    }
    return static_cast<int>(r);
}

int kafka_produce_partition(
    long long catalog_id,
    const std::string& message_key,
    int topic_partitions,
    bool per_table_topic) {
    if (topic_partitions <= 0) {
        return -1;
    }
    if (per_table_topic) {
        return partition_index_for_key(message_key, topic_partitions);
    }
    return positive_mod(catalog_id, topic_partitions);
}

bool kafka_partition_owned_by_worker(int partition, int worker_id, int worker_count) {
    if (worker_count <= 1) {
        return true;
    }
    if (partition < 0) {
        return false;
    }
    return (partition % worker_count) == worker_id;
}

std::string topic_for_catalog(
    const std::string& prefix,
    const std::string& schema,
    const std::string& table,
    const std::string& mode,
    int num_buckets) {
    if (mode == "per_table") {
        return prefix + "." + schema + "." + table;
    }
    const int bucket = topic_bucket(schema, table, num_buckets);
    std::ostringstream oss;
    oss << prefix << ".b" << std::setw(4) << std::setfill('0') << bucket;
    return oss.str();
}

std::string topic_for_catalog_table(
    const std::string& prefix,
    const std::string& schema,
    const std::string& table,
    const std::string& default_mode,
    int num_buckets,
    bool hot) {
    if (hot) {
        return topic_for_catalog(prefix, schema, table, "per_table", num_buckets);
    }
    return topic_for_catalog(prefix, schema, table, default_mode, num_buckets);
}

std::string kafka_message_key(const std::string& schema, const std::string& table) {
    return schema + "." + table;
}

std::string kafka_message_key_for_row(
    const std::string& schema,
    const std::string& table,
    const nlohmann::json* row,
    const std::string& pk_columns) {
    const std::string base = kafka_message_key(schema, table);
    if (!row || !row->is_object()) {
        return base;
    }
    std::vector<std::string> cols;
    std::string part;
    std::istringstream iss(pk_columns);
    while (std::getline(iss, part, ',')) {
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.front()))) {
            part.erase(part.begin());
        }
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.back()))) {
            part.pop_back();
        }
        if (!part.empty()) {
            cols.push_back(part);
        }
    }
    if (cols.empty()) {
        return base;
    }
    const auto it = row->find(cols[0]);
    if (it == row->end() || it->is_null()) {
        return base;
    }
    std::ostringstream oss;
    oss << base << "|" << cols[0] << "=";
    if (it->is_string()) {
        oss << "'" << sanitize_utf8_for_json(it->get<std::string>()) << "'";
    } else if (it->is_number_integer()) {
        oss << it->get<long long>();
    } else if (it->is_number_float()) {
        oss << it->get<double>();
    } else if (it->is_boolean()) {
        oss << (it->get<bool>() ? "True" : "False");
    } else {
        oss << json_dump_for_kafka(json_sanitize_for_kafka(*it));
    }
    return oss.str();
}

std::vector<std::string> topics_for_tables(
    const std::string& prefix,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::string& mode,
    int num_buckets) {
    return topics_for_tables(prefix, tables, mode, num_buckets, {});
}

std::vector<std::string> topics_for_tables(
    const std::string& prefix,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::string& mode,
    int num_buckets,
    const std::set<std::pair<std::string, std::string>>& hot_tables) {
    std::set<std::string> uniq;
    for (const auto& [schema, table] : tables) {
        const bool hot = hot_tables.count({schema, table}) > 0;
        uniq.insert(topic_for_catalog_table(prefix, schema, table, mode, num_buckets, hot));
    }
    return {uniq.begin(), uniq.end()};
}
