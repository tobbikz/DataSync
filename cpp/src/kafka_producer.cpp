#include "kafka_producer.hpp"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>

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

KafkaProducer::KafkaProducer(const std::string& bootstrap, int linger_ms, int batch_size)
    : impl_(new Impl()) {
#ifdef HAVE_RDKAFKA
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "acks", "all", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "linger.ms", std::to_string(linger_ms).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "batch.num.messages", std::to_string(batch_size).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set_dr_msg_cb(conf, delivery_report);
    rd_kafka_conf_set_opaque(conf, &impl_->body);

    impl_->body.producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!impl_->body.producer) {
        impl_->body.first_error = errstr;
        impl_->body.available = false;
        return;
    }
    impl_->body.available = true;
#else
    (void)bootstrap;
    (void)linger_ms;
    (void)batch_size;
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
        rd_kafka_flush(impl_->body.producer, 0);
        rd_kafka_destroy(impl_->body.producer);
    }
#endif
    delete impl_;
}

bool KafkaProducer::available() const {
    return impl_ && impl_->body.available;
}

void KafkaProducer::produce(const std::string& topic, const std::string& key, const std::string& value) {
    if (!impl_ || !impl_->body.available) {
        throw std::runtime_error("Kafka producer unavailable: " + (impl_ ? impl_->body.first_error : "null"));
    }
#ifdef HAVE_RDKAFKA
    impl_->body.pending.fetch_add(1);
    rd_kafka_resp_err_t err = rd_kafka_producev(
        impl_->body.producer,
        RD_KAFKA_V_TOPIC(topic.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_KEY(const_cast<char*>(key.data()), key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(value.data()), value.size()),
        RD_KAFKA_V_END);
    if (err) {
        impl_->body.pending.fetch_sub(1);
        impl_->body.errors.fetch_add(1);
        throw std::runtime_error(std::string("kafka produce failed: ") + rd_kafka_err2str(err));
    }
    rd_kafka_poll(impl_->body.producer, 0);
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
