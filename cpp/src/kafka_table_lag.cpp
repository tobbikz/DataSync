#ifdef HAVE_RDKAFKA

#include "kafka_table_lag.hpp"

#include "kafka_apply_detail.hpp"
#include "pipeline_defaults.hpp"

#include <chrono>
#include <stdexcept>

namespace kafka_table_lag {

void TableLagTracker::set_baseline(const TableKey& key, long long offset, TableApplyCursor cursor) {
    baseline_[key] = offset;
    cursors_[key] = std::move(cursor);
    if (!unresolved_.count(key)) {
        unresolved_[key] = 0;
    }
}

const TableApplyCursor* TableLagTracker::cursor(const TableKey& key) const {
    const auto it = cursors_.find(key);
    return it != cursors_.end() ? &it->second : nullptr;
}

void TableLagTracker::on_message_seen(const TableKey& key, long long offset) {
    const auto it = baseline_.find(key);
    if (it == baseline_.end() || offset <= it->second) {
        return;
    }
    unresolved_[key] += 1;
}

void TableLagTracker::on_message_resolved(const TableKey& key, long long offset) {
    const auto it = baseline_.find(key);
    if (it == baseline_.end() || offset <= it->second) {
        return;
    }
    auto uit = unresolved_.find(key);
    if (uit == unresolved_.end() || uit->second <= 0) {
        return;
    }
    uit->second -= 1;
}

long long TableLagTracker::unresolved(const TableKey& key) const {
    const auto it = unresolved_.find(key);
    return it != unresolved_.end() ? std::max<long long>(0, it->second) : 0;
}

long long compute_kafka_partition_lag(
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

TableLagScanResult compute_exact_table_kafka_lag(
    const std::string& bootstrap,
    const std::string& topic,
    int partition,
    long long consumed_offset,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::string& db_engine,
    int timeout_ms,
    long long max_messages) {
    TableLagScanResult out;
    if (topic.empty() || partition < 0 || consumed_offset < 0) {
        out.scan_complete = true;
        return out;
    }

    char errstr[512]{0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", "datasync-table-lag-scan", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.partition.eof", "true", errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        throw std::runtime_error(std::string("table lag scan consumer failed: ") + errstr);
    }
    rd_kafka_poll_set_consumer(rk);

    int64_t low = 0;
    int64_t high = 0;
    if (rd_kafka_query_watermark_offsets(rk, topic.c_str(), partition, &low, &high, 15000) !=
        RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(rk);
        out.partition_lag = -1;
        return out;
    }

    const long long next_offset = consumed_offset + 1;
    out.partition_lag = next_offset < high ? (high - next_offset) : 0;
    if (next_offset >= high) {
        out.table_lag = 0;
        out.scan_complete = true;
        rd_kafka_destroy(rk);
        return out;
    }

    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_t* part =
        rd_kafka_topic_partition_list_add(parts, topic.c_str(), partition);
    part->offset = next_offset;
    if (rd_kafka_assign(rk, parts) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(rk);
        throw std::runtime_error("table lag scan assign failed");
    }
    rd_kafka_topic_partition_list_destroy(parts);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    long long scanned = 0;
    long long table_count = 0;
    int eof_quiet = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, 250);
        if (!msg) {
            continue;
        }
        if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            rd_kafka_message_destroy(msg);
            eof_quiet += 1;
            if (eof_quiet >= 2) {
                out.scan_complete = true;
                break;
            }
            continue;
        }
        if (msg->err) {
            rd_kafka_message_destroy(msg);
            continue;
        }
        eof_quiet = 0;
        scanned += 1;
        if (max_messages > 0 && scanned > max_messages) {
            break;
        }

        const std::string payload(static_cast<const char*>(msg->payload), msg->len);
        const auto probe = kafka_apply_detail::parse_kafka_message_json(payload);
        if (!probe.is_discarded() && probe.is_object() &&
            kafka_apply_detail::kafka_payload_matches_table(
                probe, lake_schema, lake_table, db_engine)) {
            table_count += 1;
        }
        if (msg->offset >= high - 1) {
            out.scan_complete = true;
            rd_kafka_message_destroy(msg);
            break;
        }
        rd_kafka_message_destroy(msg);
    }

    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);

    out.table_lag = table_count;
    return out;
}

}  // namespace kafka_table_lag

#endif
