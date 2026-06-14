#pragma once

#ifdef HAVE_RDKAFKA

#include <string>

struct KafkaPurgeConsumedResult {
    int partitions_checked{0};
    int partitions_purged{0};
    long long deletable_offsets{0};
};

/** Delete Kafka log segments fully consumed by the apply consumer group (DeleteRecords admin API). */
KafkaPurgeConsumedResult purge_kafka_consumed_logs(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic_prefix,
    int max_lag_messages,
    long long min_deletable_offsets);

#endif
