#ifdef HAVE_RDKAFKA

#include "kafka_log_purge.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <cstring>
#include <stdexcept>
#include <string>

namespace {

bool topic_matches_prefix(const char* topic, const std::string& prefix) {
    if (!topic || prefix.empty()) {
        return false;
    }
    if (std::string(topic).rfind(prefix, 0) != 0) {
        return false;
    }
    return std::strlen(topic) == prefix.size() || topic[prefix.size()] == '.';
}

rd_kafka_t* make_admin_client(const std::string& bootstrap, char* errstr, std::size_t errstr_size) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, errstr_size) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        return nullptr;
    }
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, errstr_size);
    if (!rk) {
        rd_kafka_conf_destroy(conf);
    }
    return rk;
}

const rd_kafka_topic_partition_list_t* fetch_committed_partitions(
    rd_kafka_t* rk,
    const std::string& consumer_group) {
    rd_kafka_ListConsumerGroupOffsets_t* req =
        rd_kafka_ListConsumerGroupOffsets_new(consumer_group.c_str(), nullptr);
    if (!req) {
        throw std::runtime_error("ListConsumerGroupOffsets_new failed");
    }

    rd_kafka_AdminOptions_t* options =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
    rd_kafka_AdminOptions_set_request_timeout(options, 30000, nullptr, 0);

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_ListConsumerGroupOffsets(rk, &req, 1, options, queue);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_ListConsumerGroupOffsets_destroy(req);

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 60000);
    rd_kafka_queue_destroy(queue);
    if (!event) {
        throw std::runtime_error("ListConsumerGroupOffsets timed out");
    }
    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error("ListConsumerGroupOffsets failed: " + msg);
    }

    const rd_kafka_ListConsumerGroupOffsets_result_t* result =
        rd_kafka_event_ListConsumerGroupOffsets_result(event);
    if (!result) {
        rd_kafka_event_destroy(event);
        return nullptr;
    }

    size_t group_cnt = 0;
    const rd_kafka_group_result_t* const* groups =
        rd_kafka_ListConsumerGroupOffsets_result_groups(result, &group_cnt);
    const rd_kafka_topic_partition_list_t* parts = nullptr;
    if (group_cnt > 0 && groups[0]) {
        parts = rd_kafka_group_result_partitions(groups[0]);
    }

    if (!parts) {
        rd_kafka_event_destroy(event);
        return nullptr;
    }

    rd_kafka_topic_partition_list_t* copy =
        rd_kafka_topic_partition_list_copy(const_cast<rd_kafka_topic_partition_list_t*>(parts));
    rd_kafka_event_destroy(event);
    return copy;
}

void delete_records_before(
    rd_kafka_t* rk,
    const rd_kafka_topic_partition_list_t* before_offsets) {
    if (!before_offsets || before_offsets->cnt == 0) {
        return;
    }

    rd_kafka_DeleteRecords_t* del = rd_kafka_DeleteRecords_new(before_offsets);
    if (!del) {
        throw std::runtime_error("DeleteRecords_new failed");
    }

    rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETERECORDS);
    if (!options) {
        rd_kafka_DeleteRecords_destroy(del);
        throw std::runtime_error("AdminOptions_new failed");
    }
    rd_kafka_AdminOptions_set_request_timeout(options, 120000, nullptr, 0);
    rd_kafka_AdminOptions_set_operation_timeout(options, 120000, nullptr, 0);

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_DeleteRecords(rk, &del, 1, options, queue);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_DeleteRecords_destroy(del);

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 180000);
    rd_kafka_queue_destroy(queue);
    if (!event) {
        throw std::runtime_error("DeleteRecords timed out");
    }
    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error("DeleteRecords failed: " + msg);
    }

    const rd_kafka_DeleteRecords_result_t* result = rd_kafka_event_DeleteRecords_result(event);
    if (result) {
        const rd_kafka_topic_partition_list_t* res_parts =
            rd_kafka_DeleteRecords_result_offsets(result);
        for (int i = 0; res_parts && i < res_parts->cnt; ++i) {
            const rd_kafka_topic_partition_t* p = &res_parts->elems[i];
            if (p->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_event_destroy(event);
                throw std::runtime_error(
                    std::string("DeleteRecords partition failed: ") + rd_kafka_err2str(p->err) + " " +
                    p->topic + "-" + std::to_string(p->partition));
            }
        }
    }
    rd_kafka_event_destroy(event);
}

}  // namespace

KafkaPurgeConsumedResult purge_kafka_consumed_logs(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic_prefix,
    int max_lag_messages,
    long long min_deletable_offsets) {
    KafkaPurgeConsumedResult out;
    if (bootstrap.empty() || consumer_group.empty() || topic_prefix.empty()) {
        return out;
    }

    char errstr[512]{0};
    rd_kafka_t* rk = make_admin_client(bootstrap, errstr, sizeof(errstr));
    if (!rk) {
        throw std::runtime_error(std::string("kafka purge admin client: ") + errstr);
    }

    const rd_kafka_topic_partition_list_t* committed = fetch_committed_partitions(rk, consumer_group);
    if (!committed || committed->cnt == 0) {
        if (committed) {
            rd_kafka_topic_partition_list_destroy(
                const_cast<rd_kafka_topic_partition_list_t*>(committed));
        }
        rd_kafka_destroy(rk);
        return out;
    }

    rd_kafka_topic_partition_list_t* to_delete = rd_kafka_topic_partition_list_new(committed->cnt);
    if (!to_delete) {
        rd_kafka_topic_partition_list_destroy(const_cast<rd_kafka_topic_partition_list_t*>(committed));
        rd_kafka_destroy(rk);
        throw std::runtime_error("topic_partition_list_new failed");
    }

    for (int i = 0; i < committed->cnt; ++i) {
        const rd_kafka_topic_partition_t* part = &committed->elems[i];
        if (!topic_matches_prefix(part->topic, topic_prefix)) {
            continue;
        }
        if (part->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            continue;
        }
        if (part->offset <= 0) {
            continue;
        }

        out.partitions_checked += 1;

        int64_t low = 0;
        int64_t high = 0;
        if (rd_kafka_query_watermark_offsets(rk, part->topic, part->partition, &low, &high, 10000) !=
            RD_KAFKA_RESP_ERR_NO_ERROR) {
            continue;
        }

        const long long committed_offset = part->offset;
        const long long lag = high > committed_offset ? (high - committed_offset) : 0;
        if (lag > max_lag_messages) {
            continue;
        }

        const long long deletable = committed_offset - low;
        if (deletable < min_deletable_offsets) {
            continue;
        }

        rd_kafka_topic_partition_list_add(to_delete, part->topic, part->partition);
        rd_kafka_topic_partition_t* del_part = &to_delete->elems[to_delete->cnt - 1];
        del_part->offset = committed_offset;
        out.deletable_offsets += deletable;
    }

    rd_kafka_topic_partition_list_destroy(
        const_cast<rd_kafka_topic_partition_list_t*>(committed));

    if (to_delete->cnt > 0) {
        delete_records_before(rk, to_delete);
        out.partitions_purged = to_delete->cnt;
    }

    rd_kafka_topic_partition_list_destroy(to_delete);
    rd_kafka_destroy(rk);
    return out;
}

#endif
