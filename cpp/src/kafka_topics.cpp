#include "kafka_topics.hpp"

#include <openssl/sha.h>

#include <iomanip>
#include <set>
#include <sstream>

int topic_bucket(const std::string& schema, const std::string& table, int num_buckets) {
    const std::string key = schema + "." + table;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(key.data()), key.size(), digest);
    const unsigned val = (static_cast<unsigned>(digest[0]) << 24) | (static_cast<unsigned>(digest[1]) << 16) |
                         (static_cast<unsigned>(digest[2]) << 8) | static_cast<unsigned>(digest[3]);
    return static_cast<int>(val % static_cast<unsigned>(num_buckets));
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
        oss << "'" << it->get<std::string>() << "'";
    } else if (it->is_number_integer()) {
        oss << it->get<long long>();
    } else if (it->is_number_float()) {
        oss << it->get<double>();
    } else if (it->is_boolean()) {
        oss << (it->get<bool>() ? "True" : "False");
    } else {
        oss << it->dump();
    }
    return oss.str();
}

std::vector<std::string> topics_for_tables(
    const std::string& prefix,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::string& mode,
    int num_buckets) {
    std::set<std::string> uniq;
    for (const auto& [schema, table] : tables) {
        uniq.insert(topic_for_catalog(prefix, schema, table, mode, num_buckets));
    }
    return {uniq.begin(), uniq.end()};
}
