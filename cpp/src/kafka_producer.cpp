#include "kafka_producer.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#ifdef HAVE_RDKAFKA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop
#endif

namespace {

struct KafkaProducerImpl {
#ifdef HAVE_RDKAFKA
    rd_kafka_t* producer{nullptr};
#endif
    std::atomic<int> events_published{0};
    std::atomic<int> errors{0};
    std::atomic<int> pending{0};
    std::mutex error_mtx;
    std::string first_error;
    bool available{false};
};

#ifdef HAVE_RDKAFKA

void delivery_report(rd_kafka_t*, const rd_kafka_message_t* rkmessage, void* opaque) {
    auto* impl = static_cast<KafkaProducerImpl*>(opaque);
    impl->pending.fetch_sub(1);
    if (rkmessage->err) {
        impl->errors.fetch_add(1);
        std::lock_guard<std::mutex> lock(impl->error_mtx);
        if (impl->first_error.empty()) {
            impl->first_error = rd_kafka_err2str(rkmessage->err);
        }
    } else {
        impl->events_published.fetch_add(1);
    }
}

#endif

}  // namespace

struct KafkaProducer::Impl {
    KafkaProducerImpl body;
};

KafkaProducer::KafkaProducer(
    const std::string& bootstrap,
    int linger_ms,
    int batch_size,
    int queue_max_messages,
    int queue_max_kbytes)
    : impl_(new Impl()) {
#ifdef HAVE_RDKAFKA
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "compression.type", "zstd", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "linger.ms", std::to_string(linger_ms).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.num.messages", std::to_string(batch_size).c_str(), errstr, sizeof(errstr));
    if (queue_max_messages > 0) {
        rd_kafka_conf_set(
            conf, "queue.buffering.max.messages", std::to_string(queue_max_messages).c_str(), errstr, sizeof(errstr));
    }
    if (queue_max_kbytes > 0) {
        rd_kafka_conf_set(
            conf, "queue.buffering.max.kbytes", std::to_string(queue_max_kbytes).c_str(), errstr, sizeof(errstr));
    }
    rd_kafka_conf_set_dr_msg_cb(conf, delivery_report);
    rd_kafka_conf_set_opaque(conf, &impl_->body);

    impl_->body.producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!impl_->body.producer) {
        rd_kafka_conf_destroy(conf);
        impl_->body.first_error = errstr;
        impl_->body.available = false;
        return;
    }

    const struct rd_kafka_metadata* metadata = nullptr;
    const rd_kafka_resp_err_t md_err =
        rd_kafka_metadata(impl_->body.producer, 0, nullptr, &metadata, 10000);
    if (md_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        impl_->body.first_error = std::string("broker metadata: ") + rd_kafka_err2str(md_err);
        rd_kafka_destroy(impl_->body.producer);
        impl_->body.producer = nullptr;
        impl_->body.available = false;
        return;
    }
    if (metadata) {
        rd_kafka_metadata_destroy(metadata);
    }
    impl_->body.available = true;
#else
    (void)bootstrap;
    (void)linger_ms;
    (void)batch_size;
    (void)queue_max_messages;
    (void)queue_max_kbytes;
    impl_->body.first_error = "librdkafka not available";
    impl_->body.available = false;
#endif
}

KafkaProducer::~KafkaProducer() {
    if (!impl_) {
        return;
    }
#ifdef HAVE_RDKAFKA
    if (impl_->body.producer) {
        const int remaining = rd_kafka_flush(impl_->body.producer, 5000);
        if (remaining > 0) {
            std::cerr << "KafkaProducer destructor: " << remaining << " message(s) remain after flush\n";
        }
        rd_kafka_destroy(impl_->body.producer);
    }
#endif
    delete impl_;
}

bool KafkaProducer::available() const {
    return impl_ && impl_->body.available;
}

void KafkaProducer::produce(const std::string& topic, const std::string& key, const std::string& value, int partition) {
    if (!impl_ || !impl_->body.available) {
        throw std::runtime_error("Kafka producer unavailable: " + (impl_ ? impl_->body.first_error : "null"));
    }
#ifdef HAVE_RDKAFKA
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__FAIL;
    for (int attempt = 0; attempt < 200; ++attempt) {
        impl_->body.pending.fetch_add(1);
        if (partition >= 0) {
            err = rd_kafka_producev(
                impl_->body.producer,
                RD_KAFKA_V_TOPIC(topic.c_str()),
                RD_KAFKA_V_PARTITION(partition),
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
                RD_KAFKA_V_VALUE(const_cast<char*>(value.data()), value.size()),
                RD_KAFKA_V_END);
        } else {
            err = rd_kafka_producev(
                impl_->body.producer,
                RD_KAFKA_V_TOPIC(topic.c_str()),
                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
                RD_KAFKA_V_VALUE(const_cast<char*>(value.data()), value.size()),
                RD_KAFKA_V_END);
        }
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_poll(impl_->body.producer, 0);
            return;
        }
        impl_->body.pending.fetch_sub(1);
        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            impl_->body.errors.fetch_add(1);
            throw std::runtime_error(std::string("kafka produce failed: ") + rd_kafka_err2str(err));
        }
        rd_kafka_poll(impl_->body.producer, 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(5 + attempt / 4));
    }
    impl_->body.errors.fetch_add(1);
    impl_->body.first_error = std::string("kafka produce failed after 200 retries (QUEUE_FULL): ") + rd_kafka_err2str(err);
    throw std::runtime_error(impl_->body.first_error);
#endif
}

int KafkaProducer::flush(int timeout_sec) {
    if (!impl_ || !impl_->body.available) {
        return 0;
    }
#ifdef HAVE_RDKAFKA
    return rd_kafka_flush(impl_->body.producer, timeout_sec * 1000);
#else
    (void)timeout_sec;
    return 0;
#endif
}

KafkaProducerStats KafkaProducer::stats() const {
    KafkaProducerStats out;
    if (!impl_) {
        return out;
    }
    out.events_published = impl_->body.events_published.load();
    out.errors = impl_->body.errors.load();
    out.pending = impl_->body.pending.load();
    std::lock_guard<std::mutex> lock(impl_->body.error_mtx);
    out.first_error = impl_->body.first_error;
    return out;
}
