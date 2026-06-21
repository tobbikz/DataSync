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

/** Per-table unresolved message count ahead of slice-start apply offset. */
class TableLagTracker {
public:
    void set_baseline(const TableKey& key, long long offset, TableApplyCursor cursor);
    const TableApplyCursor* cursor(const TableKey& key) const;

    void on_message_seen(const TableKey& key, long long offset);
    void on_message_resolved(const TableKey& key, long long offset);

    long long unresolved(const TableKey& key) const;

private:
    std::map<TableKey, long long> baseline_;
    std::map<TableKey, long long> unresolved_;
    std::map<TableKey, TableApplyCursor> cursors_;
};

struct TableLagScanResult {
    long long table_lag{0};
    long long partition_lag{0};
    bool scan_complete{false};
};

/** Exact table backlog: count payload matches between consumed_offset+1 and high watermark. */
TableLagScanResult compute_exact_table_kafka_lag(
    const std::string& bootstrap,
    const std::string& topic,
    int partition,
    long long consumed_offset,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::string& db_engine,
    int timeout_ms,
    long long max_messages);

long long compute_kafka_partition_lag(
    rd_kafka_t* rk,
    const std::string& topic,
    int partition,
    long long consumed_offset);

}  // namespace kafka_table_lag

#endif
