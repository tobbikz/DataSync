#ifdef HAVE_RDKAFKA

#include "kafka_lag.hpp"

#include <cstring>
#include <stdexcept>

KafkaLagProbe::KafkaLagProbe(const std::string& bootstrap_servers) {
    char errstr[512]{};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap_servers.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Kafka lag probe config: ") + errstr);
    }
    if (rd_kafka_conf_set(conf, "group.id", "datasync-kafka-lag-probe", errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Kafka lag probe group.id: ") + errstr);
    }
    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Kafka lag probe auto.commit: ") + errstr);
    }
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("Kafka lag probe create failed: ") + errstr);
    }
    rd_kafka_poll_set_consumer(rk);
}

KafkaLagProbe::~KafkaLagProbe() {
    if (rk) {
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
    }
}

long long compute_kafka_consumer_lag(
    rd_kafka_t* rk,
    const std::string& topic,
    int partition,
    long long consumed_offset) {
    if (!rk || consumed_offset < 0 || topic.empty()) {
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

#endif
