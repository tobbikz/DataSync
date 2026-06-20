#pragma once

#ifdef HAVE_RDKAFKA

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <memory>
#include <string>

/** Ephemeral Kafka consumer for metadata queries (reconcile lag probe). */
struct KafkaLagProbe {
    rd_kafka_t* rk{nullptr};

    explicit KafkaLagProbe(const std::string& bootstrap_servers);
    ~KafkaLagProbe();

    KafkaLagProbe(const KafkaLagProbe&) = delete;
    KafkaLagProbe& operator=(const KafkaLagProbe&) = delete;
};

/** high_watermark - (consumed_offset + 1); 0 when caught up; -1 when watermark query fails. */
long long compute_kafka_consumer_lag(
    rd_kafka_t* rk,
    const std::string& topic,
    int partition,
    long long consumed_offset);

#endif
