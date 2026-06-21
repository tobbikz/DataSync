#pragma once

#include <string>

struct KafkaProducerStats {
    int events_published{0};
    int errors{0};
    int pending{0};
    std::string first_error;
};

class KafkaProducer {
public:
    KafkaProducer(
        const std::string& bootstrap,
        int linger_ms = 5,
        int batch_size = 10000,
        int queue_max_messages = 0,
        int queue_max_kbytes = 0);
    ~KafkaProducer();

    KafkaProducer(const KafkaProducer&) = delete;
    KafkaProducer& operator=(const KafkaProducer&) = delete;

    bool available() const;
    void produce(const std::string& topic, const std::string& key, const std::string& value, int partition = -1);
    int flush(int timeout_sec);
    KafkaProducerStats stats() const;

private:
    struct Impl;
    Impl* impl_{nullptr};
};
