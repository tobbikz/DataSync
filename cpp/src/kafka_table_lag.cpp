#ifdef HAVE_RDKAFKA

#include "kafka_table_lag.hpp"

namespace kafka_table_lag {

long long compute_kafka_partition_lag(
    rd_kafka_t* rk,
    const std::string& topic,
    int partition,
    long long consumed_offset) {
    if (!rk || consumed_offset < 0 || topic.empty() || partition < 0) {
        return 0;
    }
    int64_t low = 0;
    int64_t high = 0;
    if (rd_kafka_query_watermark_offsets(rk, topic.c_str(), partition, &low, &high, 5000) !=
        RD_KAFKA_RESP_ERR_NO_ERROR) {
        return -1;
    }
    if (high <= low) {
        return 0;
    }
    const long long next_offset = consumed_offset + 1;
    return next_offset < high ? (high - next_offset) : 0;
}

}  // namespace kafka_table_lag

#endif
