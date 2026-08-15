#pragma once

#ifdef HAVE_RDKAFKA

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <map>
#include <string>
#include <utility>

namespace kafka_table_lag {

using TableKey = std::pair<std::string, std::string>;

struct TableApplyCursor {
    std::string kafka_topic;
    int kafka_partition{-1};
    long long kafka_offset{-1};
};

/** high_watermark − (consumed_offset + 1); partition-level (shared bucket topics). */
long long compute_kafka_partition_lag(
    rd_kafka_t* rk,
    const std::string& topic,
    int partition,
    long long consumed_offset);

}  // namespace kafka_table_lag

#endif
