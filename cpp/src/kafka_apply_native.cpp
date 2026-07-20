#ifdef HAVE_RDKAFKA

#include "kafka_apply.hpp"
#include "kafka_apply_detail.hpp"
#include "kafka_lag.hpp"
#include "kafka_table_lag.hpp"
#include "kafka_topics.hpp"
#include "capture_common.hpp"
#include "lake_apply_index.hpp"

#include "config.hpp"
#include "mariadb_schema.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using json = nlohmann::json;
using TableKey = std::pair<std::string, std::string>;
using kafka_apply_detail::ApplyEvent;
using kafka_apply_detail::fetch_apply_cursors_for_tables;
using kafka_apply_detail::kafka_payload_matches_table;
using kafka_apply_detail::parse_kafka_message_json;
using kafka_apply_detail::record_slice_table_lag_stats;
using kafka_apply_detail::SliceFlushStats;
using kafka_apply_detail::SliceLagTableState;
using kafka_table_lag::compute_exact_table_kafka_lag;
using kafka_table_lag::compute_kafka_partition_lag;
using kafka_table_lag::TableApplyCursor;
using kafka_table_lag::TableLagTracker;
using kafka_apply_detail::ApplyBatchOptions;
using kafka_apply_detail::apply_events_batch;
using kafka_apply_detail::configure_lake_apply_session;
using kafka_apply_detail::fill_table_key_from_event_id;
using kafka_apply_detail::resolve_event_lake_key_from_catalog;
using kafka_apply_detail::filter_new_event_ids;
using kafka_apply_detail::parse_kafka_payload;
using kafka_apply_detail::record_dropped_unrecoverable;

struct KafkaApplyContext {
    PGconn* log_pg{nullptr};
    std::string batch_id;
    std::string conn_id;
    int worker_id{0};
    int worker_count{1};
};

int filter_partitions_for_worker(rd_kafka_topic_partition_list_t* partitions, int worker_id, int worker_count) {
    if (!partitions || worker_count <= 1) {
        return partitions ? partitions->cnt : 0;
    }
    int kept = 0;
    for (int i = 0; i < partitions->cnt; ++i) {
        const int partition = partitions->elems[i].partition;
        if (!kafka_partition_owned_by_worker(partition, worker_id, worker_count)) {
            continue;
        }
        if (kept != i) {
            partitions->elems[kept] = partitions->elems[i];
        }
        kept += 1;
    }
    partitions->cnt = kept;
    return kept;
}

void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err,
                  rd_kafka_topic_partition_list_t* partitions, void* opaque) {
    auto* ctx = static_cast<KafkaApplyContext*>(opaque);
    if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        if (partitions) {
            rd_kafka_commit(rk, partitions, 0);
        }
        rd_kafka_assign(rk, nullptr);
        if (ctx) {
            log_write(ctx->log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_apply",
                .message = "kafka partitions revoked, committed offsets",
                .batch_id = ctx->batch_id,
                .conn_id = ctx->conn_id,
                .context = {{"partition_count", partitions ? static_cast<int>(partitions->cnt) : 0}},
            });
        }
    } else if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        const int assigned_before = partitions ? partitions->cnt : 0;
        const int assigned_after =
            filter_partitions_for_worker(partitions, ctx ? ctx->worker_id : 0, ctx ? ctx->worker_count : 1);
        rd_kafka_assign(rk, partitions);
        if (ctx) {
            log_write(ctx->log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_apply",
                .message = "kafka partitions assigned",
                .batch_id = ctx->batch_id,
                .conn_id = ctx->conn_id,
                .context = {
                    {"partition_count", assigned_after},
                    {"partition_count_offered", assigned_before},
                    {"worker_id", ctx->worker_id},
                    {"worker_count", ctx->worker_count},
                },
            });
        }
    }
}

struct CatalogMeta {
    long long catalog_id{0};
    std::string pk_columns;
    std::string source_database;  // MSSQL only
    std::string source_schema;
    std::string source_table;
    bool hot{false};
};

std::vector<CatalogMeta> fetch_apply_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& db_engine,
    CatalogHotTier hot_tier) {
    const auto rows = fetch_conn_catalog_tables(
        pg, conn_id, worker_id, worker_count, db_engine, CatalogPipeline::Apply, hot_tier);
    std::vector<CatalogMeta> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        CatalogMeta meta;
        meta.catalog_id = row.catalog_id;
        meta.pk_columns = row.pk_columns;
        meta.source_database = row.source_database;
        meta.source_table = row.source_table;
        meta.hot = row.hot;
        if (db_engine == "mongodb") {
            meta.source_schema = mongo_catalog_source_schema(row.source_database, row.source_schema);
        } else {
            meta.source_schema = row.source_schema;
        }
        const TableKey key = {row.lake_schema, row.lake_table};
        meta_by_key[key] = meta;
        out.push_back(meta);
    }
    return out;
}

void account_lag_message(
    const std::string& payload,
    const std::string& topic,
    int partition,
    long long offset,
    const std::string& db_engine,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    TableLagTracker& tracker,
    std::set<std::tuple<std::string, int, long long>>& lag_seen_offsets) {
    const auto tp_key = std::make_tuple(topic, partition, offset);
    if (!lag_seen_offsets.insert(tp_key).second) {
        return;
    }
    const json probe = parse_kafka_message_json(payload);
    if (probe.is_discarded() || !probe.is_object()) {
        return;
    }
    for (const auto& [lake_key, meta] : meta_by_key) {
        (void)meta;
        if (!kafka_payload_matches_table(probe, lake_key.first, lake_key.second, db_engine)) {
            continue;
        }
        tracker.on_message_seen(lake_key, offset);
        break;
    }
}

void drain_kafka_lag_tail(
    rd_kafka_t* rk,
    const std::string& db_engine,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    TableLagTracker& tracker,
    std::set<std::tuple<std::string, int, long long>>& lag_seen_offsets,
    int quiet_polls) {
    int empty_polls = 0;
    while (empty_polls < quiet_polls) {
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, 500);
        if (!msg) {
            empty_polls += 1;
            continue;
        }
        if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            rd_kafka_message_destroy(msg);
            empty_polls += 1;
            continue;
        }
        if (msg->err) {
            rd_kafka_message_destroy(msg);
            continue;
        }
        empty_polls = 0;
        const std::string topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
        const std::string payload(static_cast<const char*>(msg->payload), msg->len);
        account_lag_message(
            payload,
            topic,
            msg->partition,
            msg->offset,
            db_engine,
            meta_by_key,
            tracker,
            lag_seen_offsets);
        rd_kafka_message_destroy(msg);
    }
}

void finalize_slice_table_lag(
    PGconn* app_pg,
    rd_kafka_t* rk,
    const std::string& bootstrap,
    const std::string& conn_id,
    const std::string& batch_id,
    const std::string& db_engine,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::map<TableKey, int>& seen_by_table,
    const std::map<TableKey, int>& applied_by_table,
    const std::vector<ApplyEvent>& pending_batch,
    TableLagTracker& tracker,
    const std::map<TableKey, SliceFlushStats>& slice_flush_stats,
    const std::map<TableKey, int>& parse_skipped_by_table,
    const std::map<TableKey, int>& dropped_unrecoverable_by_table,
    int apply_staleness_seconds,
    int apply_inactive_seconds,
    int scan_timeout_ms) {
    (void)pending_batch;
    for (const auto& [lake_key, meta] : meta_by_key) {
        SliceLagTableState state;
        state.events_seen_in_slice = seen_by_table.count(lake_key) ? seen_by_table.at(lake_key) : 0;
        state.events_applied_in_slice = applied_by_table.count(lake_key) ? applied_by_table.at(lake_key) : 0;
        state.inactive = state.events_seen_in_slice <= 0;
        if (const TableApplyCursor* cursor = tracker.cursor(lake_key)) {
            state.kafka_topic = cursor->kafka_topic;
            state.kafka_partition = cursor->kafka_partition;
            state.kafka_offset = cursor->kafka_offset;
        }

        const long long tracked = tracker.unresolved(lake_key);
        const bool run_exact_scan = state.events_seen_in_slice > 0 || tracked > 0;
        if (!state.kafka_topic.empty() && state.kafka_partition >= 0 && state.kafka_offset >= 0) {
            state.partition_lag = compute_kafka_partition_lag(
                rk, state.kafka_topic, state.kafka_partition, state.kafka_offset);
            if (run_exact_scan) {
                const auto scan = compute_exact_table_kafka_lag(
                    bootstrap,
                    state.kafka_topic,
                    state.kafka_partition,
                    state.kafka_offset,
                    lake_key.first,
                    lake_key.second,
                    db_engine,
                    scan_timeout_ms,
                    pipeline_defaults::kTableLagScanMaxMessagesDefault);
                state.table_lag = scan.table_lag;
                state.lag_scan_complete = scan.scan_complete;
                if (scan.partition_lag >= 0) {
                    state.partition_lag = scan.partition_lag;
                }
            } else {
                state.table_lag = state.partition_lag;
                state.lag_scan_complete = true;
            }
        } else {
            state.table_lag = tracked;
        }

        SliceFlushStats merged;
        if (const auto fit = slice_flush_stats.find(lake_key); fit != slice_flush_stats.end()) {
            merged = fit->second;
        }
        if (const auto ps = parse_skipped_by_table.find(lake_key); ps != parse_skipped_by_table.end()) {
            merged.parse_skipped = ps->second;
        }
        if (const auto dr = dropped_unrecoverable_by_table.find(lake_key);
            dr != dropped_unrecoverable_by_table.end()) {
            merged.dropped_unrecoverable = dr->second;
        }
        merged.events_seen_in_slice = state.events_seen_in_slice;

        record_slice_table_lag_stats(
            app_pg,
            conn_id,
            batch_id,
            meta.catalog_id,
            meta.source_schema,
            meta.source_table,
            state,
            &merged,
            apply_staleness_seconds,
            apply_inactive_seconds);
    }
}

void ensure_apply_positions(
    PGconn* pg,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& conn_id,
    const std::string& topic_prefix,
    const std::string& topic_mode,
    int topic_buckets) {
    for (const auto& [lake_key, meta] : meta_by_key) {
        const std::string topic = topic_for_catalog_table(
            topic_prefix, lake_key.first, lake_key.second, topic_mode, topic_buckets, meta.hot);
        const std::string cid = std::to_string(meta.catalog_id);
        auto mark_ap_failed = [&](const std::string& err_msg) {
            const std::string trunc = err_msg.substr(0, 950);
            const char* fail_vals[] = {cid.c_str(), trunc.c_str()};
            PGresult* fail = PQexecParams(
                pg,
                R"(
                UPDATE cdc_catalog.catalog
                SET status = 'failed',
                    last_error = $2,
                    last_error_at = now(),
                    updated_at = now()
                WHERE catalog_id = $1::bigint
                )",
                2, nullptr, fail_vals, nullptr, nullptr, 0);
            if (fail) PQclear(fail);
        };
        std::string ap_err;
        if (!upsert_apply_position(
                pg,
                meta.catalog_id,
                conn_id,
                lake_key.first,
                lake_key.second,
                topic,
                &ap_err)) {
            mark_ap_failed("apply_position upsert failed: " + ap_err);
        }
    }
}

TableKey lake_key_for_source(
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& source_schema,
    const std::string& source_table) {
    const TableKey direct{source_schema, source_table};
    if (meta_by_key.count(direct)) {
        return direct;
    }
    for (const auto& [lake_key, meta] : meta_by_key) {
        if (meta.source_schema == source_schema && meta.source_table == source_table) {
            return lake_key;
        }
    }
    return direct;
}

std::set<TableKey> fetch_quarantined(
    PGconn* pg,
    const std::string& conn_id,
    const std::map<TableKey, CatalogMeta>& meta_by_key) {
    std::set<TableKey> out;
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(SELECT source_schema, source_table FROM cdc_catalog.apply_position
           WHERE conn_id = $1 AND status = 'quarantined')",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res); ++i) {
            const std::string src_schema = PQgetvalue(res, i, 0);
            const std::string src_table = PQgetvalue(res, i, 1);
            out.insert(lake_key_for_source(meta_by_key, src_schema, src_table));
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

void commit_kafka_offsets(rd_kafka_t* rk, const std::map<std::pair<std::string, int>, long long>& offsets) {
    if (offsets.empty()) {
        return;
    }
    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(static_cast<int>(offsets.size()));
    for (const auto& [tp, offset] : offsets) {
        rd_kafka_topic_partition_t* part =
            rd_kafka_topic_partition_list_add(parts, tp.first.c_str(), tp.second);
        part->offset = offset + 1;
    }
    const rd_kafka_resp_err_t err = rd_kafka_commit(rk, parts, 0);
    rd_kafka_topic_partition_list_destroy(parts);
    if (err) {
        throw std::runtime_error(std::string("Kafka commit failed: ") + rd_kafka_err2str(err));
    }
}

std::map<std::pair<std::string, int>, long long> flush_pending_batch(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    std::vector<ApplyEvent>& pending,
    bool dedup_enabled,
    bool audit_enabled,
    long long& events_applied,
    int& errors,
    int& dropped_unrecoverable,
    std::map<TableKey, int>& applied_by_table,
    const std::map<TableKey, int>& seen_by_table,
    const std::map<TableKey, int>& parse_skipped_by_table,
    std::map<TableKey, int>* dropped_unrecoverable_by_table,
    std::map<TableKey, SliceFlushStats>* slice_flush_stats,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& source_system = "MariaDB",
    const std::string& db_engine = "mariadb",
    int apply_staleness_seconds = 900,
    int apply_inactive_seconds = 3600,
    rd_kafka_t* rk = nullptr,
    TableLagTracker* lag_tracker = nullptr) {
    (void)rk;
    std::map<std::pair<std::string, int>, long long> batch_offsets;
    if (pending.empty()) {
        return batch_offsets;
    }

    std::vector<ApplyEvent> eligible = std::move(pending);
    pending.clear();

    std::vector<std::string> event_ids;
    event_ids.reserve(eligible.size());
    for (const auto& e : eligible) {
        event_ids.push_back(e.event_id);
    }

    std::unordered_set<std::string> new_ids;
    if (dedup_enabled) {
        new_ids = filter_new_event_ids(app_pg, event_ids);
    } else {
        new_ids.insert(event_ids.begin(), event_ids.end());
    }

    std::vector<ApplyEvent> to_apply;
    to_apply.reserve(eligible.size());
    std::map<std::string, int> dedup_skipped_by_state;
    for (auto& e : eligible) {
        const TableKey key{e.schema_name, e.table_name};
        const auto meta_it = meta_by_key.find(key);
        const std::string state_key =
            meta_it != meta_by_key.end()
                ? meta_it->second.source_schema + "|" + meta_it->second.source_table
                : e.schema_name + "|" + e.table_name;
        if (!new_ids.count(e.event_id)) {
            applied_by_table[key] = applied_by_table[key] + 1;
            batch_offsets[{e.topic, e.partition}] = e.offset;
            dedup_skipped_by_state[state_key] += 1;
            if (lag_tracker) {
                lag_tracker->on_message_resolved(key, e.offset);
            }
            continue;
        }
        to_apply.push_back(std::move(e));
    }

    const int dedup_skipped = static_cast<int>(eligible.size() - to_apply.size());
    if (to_apply.empty()) {
        log_write(app_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply",
            .message = "apply batch dedup only",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"batch_events", static_cast<int>(eligible.size())},
                {"dedup_skipped", dedup_skipped},
                {"to_apply", 0},
            },
        });
        return batch_offsets;
    }

    log_write(app_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = "apply batch flushing to lake",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"batch_events", static_cast<int>(eligible.size())},
            {"dedup_skipped", dedup_skipped},
            {"to_apply", static_cast<int>(to_apply.size())},
        },
    });

    int batch_table_errors = 0;
    std::vector<ApplyEvent> failed_events;
    long long applied_n = 0;
    try {
        ApplyBatchOptions options;
        options.audit_enabled = audit_enabled;
        options.source_system = source_system;
        options.apply_staleness_seconds = apply_staleness_seconds;
        options.apply_inactive_seconds = apply_inactive_seconds;
        failed_events.clear();
        options.table_errors_out = &batch_table_errors;
        options.failed_events_out = &failed_events;
        for (const auto& [state_key, skipped] : dedup_skipped_by_state) {
            options.slice_table_state[state_key].dedup_skipped += skipped;
        }
        options.parse_skipped_by_table = &parse_skipped_by_table;
        options.dropped_unrecoverable_by_table = dropped_unrecoverable_by_table;
        options.slice_flush_stats = slice_flush_stats;
        for (const auto& [lake_key, catalog_meta] : meta_by_key) {
            options.table_hot[lake_key] = catalog_meta.hot;
        }
        std::map<std::string, std::tuple<std::string, int, long long>> last_kafka_by_state;
        for (const auto& e : to_apply) {
            const TableKey lake_key{e.schema_name, e.table_name};
            const auto meta_it = meta_by_key.find(lake_key);
            const std::string state_key =
                meta_it != meta_by_key.end()
                    ? meta_it->second.source_schema + "|" + meta_it->second.source_table
                    : e.schema_name + "|" + e.table_name;
            auto& state = options.slice_table_state[state_key];
            const int seen_n = seen_by_table.count(lake_key) ? seen_by_table.at(lake_key) : 0;
            state.events_seen_in_slice = std::max(state.events_seen_in_slice, seen_n);
            last_kafka_by_state[state_key] = {e.topic, e.partition, e.offset};
        }
        for (auto& [state_key, state] : options.slice_table_state) {
            state.kafka_consumer_lag = 0;
        }
        json result = apply_events_batch(app_pg, lake_pg, conn_id, batch_id, to_apply, options);
        applied_n = result.value("applied", 0LL);
        events_applied += applied_n;
        errors += batch_table_errors;
        dropped_unrecoverable += result.value("dropped_unrecoverable", 0);

        std::set<std::pair<std::string, int>> dirty_partitions;
        for (const auto& e : failed_events) {
            dirty_partitions.insert({e.topic, e.partition});
        }
        for (const auto& e : to_apply) {
            if (dirty_partitions.count({e.topic, e.partition}) > 0) {
                continue;
            }
            const TableKey key{e.schema_name, e.table_name};
            applied_by_table[key] = applied_by_table[key] + 1;
            if (lag_tracker) {
                lag_tracker->on_message_resolved(key, e.offset);
            }
            auto it = batch_offsets.find({e.topic, e.partition});
            if (it == batch_offsets.end() || e.offset > it->second) {
                batch_offsets[{e.topic, e.partition}] = e.offset;
            }
        }
        if (!failed_events.empty()) {
            int dropped_unrecoverable = 0;
            for (auto& e : failed_events) {
                if (e.catalog_id > 0) {
                    resolve_event_lake_key_from_catalog(app_pg, conn_id, e);
                }
                if (e.schema_name.empty() || e.table_name.empty()) {
                    fill_table_key_from_event_id(e, db_engine);
                }
                if (e.schema_name.empty() || e.table_name.empty()) {
                    dropped_unrecoverable += 1;
                    record_dropped_unrecoverable(app_pg, e, dropped_unrecoverable_by_table);
                    continue;
                }
                pending.push_back(std::move(e));
            }
            if (dropped_unrecoverable > 0) {
                log_write(app_pg, {
                    .level = LogLevel::Warning,
                    .component = "cdc_kafka_apply",
                    .message = "apply dropped unrecoverable failed events",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {{"events", dropped_unrecoverable}},
                });
            }
        }

        if (batch_table_errors > 0) {
            log_write(app_pg, {
                .level = applied_n > 0 ? LogLevel::Warning : LogLevel::Error,
                .component = "cdc_kafka_apply",
                .message = applied_n > 0 ? "apply batch partial flush"
                                         : "apply batch flush failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"table_errors", batch_table_errors},
                    {"rows_applied", applied_n},
                    {"failed_events", static_cast<int>(failed_events.size())},
                    {"dirty_partitions", static_cast<int>(dirty_partitions.size())},
                    {"kafka_offsets", static_cast<int>(batch_offsets.size())},
                },
            });
        }

        if (applied_n == 0 && batch_table_errors > 0) {
            throw std::runtime_error("apply batch flush failed: no rows applied");
        }

        log_write(app_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply",
            .message = "lake batch committed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"rows_applied", applied_n},
                {"to_apply", static_cast<int>(to_apply.size())},
                {"table_errors", batch_table_errors},
                {"kafka_offsets", static_cast<int>(batch_offsets.size())},
            },
        });
    } catch (const std::exception& ex) {
        if (batch_table_errors == 0) {
            errors += 1;
        }
        if (pending.empty()) {
            // eligible may contain moved-from shells after to_apply was built; re-queue real events only.
            for (auto& e : to_apply) {
                if (e.catalog_id > 0) {
                    resolve_event_lake_key_from_catalog(app_pg, conn_id, e);
                }
                if (e.schema_name.empty() || e.table_name.empty()) {
                    fill_table_key_from_event_id(e, db_engine);
                }
                if (!e.schema_name.empty() && !e.table_name.empty()) {
                    pending.push_back(std::move(e));
                }
            }
            for (auto& e : failed_events) {
                if (e.catalog_id > 0) {
                    resolve_event_lake_key_from_catalog(app_pg, conn_id, e);
                }
                if (e.schema_name.empty() || e.table_name.empty()) {
                    fill_table_key_from_event_id(e, db_engine);
                }
                if (!e.schema_name.empty() && !e.table_name.empty()) {
                    pending.push_back(std::move(e));
                }
            }
        }
        log_write(app_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = "apply batch flush exception",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"error", ex.what()},
                {"table_errors", batch_table_errors},
                {"rows_applied", applied_n},
                {"to_apply", static_cast<int>(to_apply.size())},
            },
        });
        throw std::runtime_error(ex.what());
    }

    return batch_offsets;
}

}  // namespace

int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    CatalogHotTier hot_tier) {
    const bool hot_path = hot_tier == CatalogHotTier::HotOnly;
    const auto start = std::chrono::steady_clock::now();
    const std::string batch_id = make_batch_id();
    json stats = {
        {"batch_id", batch_id},
        {"events_seen", 0},
        {"events_applied", 0},
        {"errors", 0},
    };

    RuntimeConfig runtime;
    runtime.reload(log_pg);

    const KafkaBootstrapResolved kafka_boot = resolve_kafka_bootstrap();
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = "kafka-apply started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"worker_id", worker_id},
            {"worker_count", worker_count},
            {"hot_path", hot_path},
            {"db_engine", conn_engine(cfg, conn_id)},
            {"kafka_bootstrap", kafka_boot.bootstrap},
            {"kafka_bootstrap_source", kafka_boot.source},
            {"topic_prefix", topic_prefix_for_conn(conn_id)},
            {"apply_batch_size",
             hot_path ? pipeline_defaults::kHotApplyBatchSizeDefault
                      : runtime.get_int(
                            "apply_batch_size",
                            pipeline_defaults::kApplyBatchSizeDefault,
                            "cdc_kafka_apply",
                            conn_id)},
        },
    });

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    runtime.reload(app_pg.raw);
    configure_lake_apply_session(lake_pg.raw, runtime, hot_path, conn_id);

    if (worker_id == 0) {
        const auto idx_stats = backfill_mirror_apply_pk_indexes(app_pg.raw, lake_pg.raw, conn_id);
        if (idx_stats.indexes_created > 0 || idx_stats.errors > 0) {
            log_write(log_pg, {
                .level = idx_stats.errors > 0 ? LogLevel::Warning : LogLevel::Info,
                .component = "cdc_kafka_apply",
                .message = "mirror apply PK index backfill",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"tables_seen", idx_stats.tables_seen},
                    {"indexes_created", idx_stats.indexes_created},
                    {"tables_skipped", idx_stats.tables_skipped},
                    {"errors", idx_stats.errors},
                },
            });
        }
    }

    const std::string db_engine = conn_engine(cfg, conn_id);
    clear_stale_cdc_in_progress(app_pg.raw, conn_id, db_engine);

    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    const std::string bootstrap = kafka.bootstrap;
    if (worker_count <= 0) {
        worker_count = runtime.get_int(
            "apply_worker_count",
            pipeline_defaults::kApplyWorkerCount,
            "cdc_kafka_apply",
            conn_id);
    }
    const std::string consumer_group =
        kafka_apply_consumer_group(conn_id, worker_id, worker_count, hot_path);
    const std::string topic_prefix = topic_prefix_for_conn(conn_id);
    const std::string topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    const int topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    const int slice_seconds_fallback =
        cfg.cdc.slice_max_seconds > 0 ? cfg.cdc.slice_max_seconds : 300;
    const int slice_events_fallback = cfg.cdc.slice_max_events > 0
        ? static_cast<int>(cfg.cdc.slice_max_events)
        : pipeline_defaults::kApplyMaxEventsDefault;
    const int max_seconds = slice_seconds_fallback;
    const int max_events = slice_events_fallback;
    const int poll_timeout_ms = pipeline_defaults::kApplyPollTimeoutMs;
    const int fetch_max_bytes = pipeline_defaults::kApplyFetchMaxBytes;
    const int max_partition_fetch_bytes = pipeline_defaults::kApplyMaxPartitionFetchBytes;
    const int empty_poll_quiet = pipeline_defaults::kApplyEmptyPollQuietThreshold;
    const int batch_size = hot_path
        ? pipeline_defaults::kHotApplyBatchSizeDefault
        : runtime.get_int(
              "apply_batch_size", pipeline_defaults::kApplyBatchSizeDefault, "cdc_kafka_apply", conn_id);
    const int staleness = pipeline_defaults::kApplyMaxTableStalenessSeconds;
    const int inactive_seconds = pipeline_defaults::kApplyInactiveSeconds;
    const bool dedup_enabled = pipeline_defaults::kApplyDedupEnabled;
    const bool audit_enabled = pipeline_defaults::kApplyAuditEnabled;
    const int queued_min_messages = pipeline_defaults::kApplyQueuedMinMessages;
    const int fetch_wait_max_ms = pipeline_defaults::kApplyFetchWaitMaxMs;
    const int topic_partitions = runtime.get_int(
        "kafka_topic_partitions",
        pipeline_defaults::kKafkaTopicPartitions,
        "cdc_kafka_apply",
        conn_id);
    if (worker_count > 1 && topic_partitions % worker_count != 0) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_kafka_apply",
            .message = "kafka topic_partitions not divisible by worker_count; shard routing may skew",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"topic_partitions", topic_partitions},
                {"worker_count", worker_count},
            },
        });
    }

    std::map<TableKey, CatalogMeta> meta_by_key;
    fetch_apply_catalog_tables(
        app_pg.raw, conn_id, worker_id, worker_count, meta_by_key, db_engine, hot_tier);
    if (meta_by_key.empty()) {
        const ApplySkipReasonCounts reasons =
            fetch_apply_skip_reasons(app_pg.raw, conn_id, db_engine);
        log_cdc_skip_no_tables(
            log_pg, "cdc_kafka_apply_cpp", "apply", batch_id, conn_id, db_engine);
        stats["stop_reason"] = "no_tables";
        stats["active_total"] = reasons.active_total;
        stats["needs_full_load"] = reasons.needs_full_load;
        stats["apply_ready"] = reasons.apply_ready;
        stats["duration_ms"] = 0;
        std::cout << stats.dump() << std::endl;
        return 0;
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = "apply tables selected",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"table_count", static_cast<int>(meta_by_key.size())},
            {"worker_id", worker_id},
            {"worker_count", worker_count},
            {"kafka_bootstrap", bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"consumer_group", consumer_group},
            {"topic_prefix", topic_prefix},
            {"topic_mode", topic_mode},
            {"topic_buckets", topic_buckets},
            {"hot_path", hot_path},
        },
    });

    std::set<TableKey> wanted;
    std::vector<std::pair<std::string, std::string>> table_pairs;
    std::map<TableKey, std::vector<std::string>> pk_by_table;
    for (const auto& [key, meta] : meta_by_key) {
        wanted.insert(key);
        table_pairs.push_back(key);
        pk_by_table[key] = split_pk_columns(meta.pk_columns);
    }

    ensure_apply_positions(app_pg.raw, meta_by_key, conn_id, topic_prefix, topic_mode, topic_buckets);

    const int health_refreshed = refresh_apply_position_health(app_pg.raw, conn_id, staleness);
    if (health_refreshed > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply",
            .message = "apply_position health refreshed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .context = {{"tables_updated", health_refreshed}, {"staleness_seconds", staleness}},
        });
    }

    std::set<TableKey> quarantined = fetch_quarantined(app_pg.raw, conn_id, meta_by_key);

    std::set<TableKey> hot_table_keys;
    for (const auto& [key, meta] : meta_by_key) {
        if (meta.hot) {
            hot_table_keys.insert(key);
        }
    }

    const auto topics =
        topics_for_tables(topic_prefix, table_pairs, topic_mode, topic_buckets, hot_table_keys);

    try {
        ensure_kafka_topics_exist(bootstrap, topics, topic_partitions, 1);
    } catch (const std::exception& ex) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = "kafka ensure topics failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}, {"topic_count", static_cast<int>(topics.size())}},
        });
        stats["stop_reason"] = "kafka_ensure_topics_failed";
        stats["errors"] = 1;
        stats["duration_ms"] = 0;
        std::cout << stats.dump() << std::endl;
        return 1;
    }

    char errstr[512]{0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", consumer_group.c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "session.timeout.ms", "30000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "socket.timeout.ms", "120000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "max.poll.interval.ms", "3600000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "fetch.max.bytes", std::to_string(fetch_max_bytes).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(
        conf,
        "max.partition.fetch.bytes",
        std::to_string(max_partition_fetch_bytes).c_str(),
        errstr,
        sizeof(errstr));
    rd_kafka_conf_set(conf, "queued.min.messages", std::to_string(queued_min_messages).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "fetch.wait.max.ms", std::to_string(fetch_wait_max_ms).c_str(), errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "allow.auto.create.topics", "true", errstr, sizeof(errstr));

    auto* kafka_ctx = new KafkaApplyContext{log_pg, batch_id, conn_id, worker_id, worker_count};
    rd_kafka_conf_set_opaque(conf, kafka_ctx);
    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = "kafka consumer create failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", errstr}},
        });
        stats["stop_reason"] = "kafka_init_failed";
        stats["errors"] = 1;
        stats["duration_ms"] = 0;
        rd_kafka_conf_destroy(conf);
        std::cout << stats.dump() << std::endl;
        return 1;
    }
    rd_kafka_poll_set_consumer(rk);

    rd_kafka_topic_partition_list_t* sub = rd_kafka_topic_partition_list_new(static_cast<int>(topics.size()));
    for (const auto& topic : topics) {
        rd_kafka_topic_partition_list_add(sub, topic.c_str(), RD_KAFKA_PARTITION_UA);
    }
    rd_kafka_resp_err_t sub_err = rd_kafka_subscribe(rk, sub);
    rd_kafka_topic_partition_list_destroy(sub);
    if (sub_err) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = "kafka subscribe failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", rd_kafka_err2str(sub_err)}},
        });
        delete static_cast<KafkaApplyContext*>(rd_kafka_opaque(rk));
        rd_kafka_destroy(rk);
        stats["stop_reason"] = "kafka_subscribe_failed";
        stats["errors"] = 1;
        std::cout << stats.dump() << std::endl;
        return 1;
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply",
        .message = "kafka consumer subscribed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"consumer_group", consumer_group},
            {"topic_count", static_cast<int>(topics.size())},
            {"kafka_bootstrap", bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"poll_timeout_ms", poll_timeout_ms},
        },
    });

    TableLagTracker lag_tracker;
    std::set<std::tuple<std::string, int, long long>> lag_seen_offsets;
    std::map<TableKey, long long> catalog_id_by_lake_key;
    for (const auto& [key, meta] : meta_by_key) {
        catalog_id_by_lake_key[key] = meta.catalog_id;
    }
    const auto cursor_states = fetch_apply_cursors_for_tables(app_pg.raw, catalog_id_by_lake_key);
    for (const auto& [lake_key, state] : cursor_states) {
        TableApplyCursor cursor{state.kafka_topic, state.kafka_partition, state.kafka_offset};
        lag_tracker.set_baseline(lake_key, state.kafka_offset, cursor);
    }
    const int table_lag_scan_timeout_ms = runtime.get_int(
        "table_lag_scan_timeout_ms",
        pipeline_defaults::kTableLagScanTimeoutMsDefault,
        "cdc_kafka_apply",
        conn_id);

    std::map<TableKey, int> seen_by_table;
    std::map<TableKey, int> applied_by_table;
    std::map<TableKey, int> parse_skipped_by_table;
    std::map<TableKey, int> dropped_unrecoverable_by_table;
    std::map<TableKey, SliceFlushStats> slice_flush_stats;
    std::map<std::pair<std::string, int>, long long> last_offsets;

    long long events_seen = 0;
    long long events_applied = 0;
    long long parse_skipped = 0;
    int dropped_unrecoverable_total = 0;
    int errors = 0;
    std::string stop_reason = "unknown";
    bool loop_exited_early = false;
    int empty_polls = 0;
    bool first_kafka_message_logged = false;
    long long kafka_poll_progress_last = 0;
    std::map<std::pair<std::string, int>, long long> skipped_shard_offsets;
    std::vector<ApplyEvent> pending_batch;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(max_seconds);

    const std::string source_system =
        (db_engine == "mssql") ? "MSSQL" : (db_engine == "mongodb") ? "MongoDB" : "MariaDB";

    auto flush_batch = [&]() {
        if (pending_batch.empty()) {
            return;
        }
        for (int retry = 0; retry < 3; ++retry) {
            try {
                int dropped = 0;
                auto batch_offsets = flush_pending_batch(
                    app_pg.raw,
                    lake_pg.raw,
                    conn_id,
                    batch_id,
                    pending_batch,
                    dedup_enabled,
                    audit_enabled,
                    events_applied,
                    errors,
                    dropped,
                    applied_by_table,
                    seen_by_table,
                    parse_skipped_by_table,
                    &dropped_unrecoverable_by_table,
                    &slice_flush_stats,
                    meta_by_key,
                    source_system,
                    db_engine,
                    staleness,
                    inactive_seconds,
                    rk,
                    &lag_tracker);
                dropped_unrecoverable_total += dropped;
                commit_kafka_offsets(rk, batch_offsets);
                for (const auto& [tp, offset] : batch_offsets) {
                    last_offsets[tp] = std::max(last_offsets[tp], offset);
                }
                return;
            } catch (const std::exception& ex) {
                if (retry >= 2) {
                    try {
                        const int cleared =
                            clear_stale_cdc_in_progress(app_pg.raw, conn_id, db_engine);
                        log_write(log_pg, {
                            .level = LogLevel::Error,
                            .component = "cdc_kafka_apply",
                            .message = "apply batch flush failed after retries",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .source_schema = std::nullopt,
                            .source_table = std::nullopt,
                            .context = {
                                {"error", ex.what()},
                                {"errors", errors},
                                {"retries", retry},
                                {"pending_batch_size", static_cast<int>(pending_batch.size())},
                                {"cdc_in_progress_cleared", cleared},
                            },
                        });
                    } catch (const std::exception& clear_ex) {
                        log_write(log_pg, {
                            .level = LogLevel::Error,
                            .component = "cdc_kafka_apply",
                            .message = "apply batch flush failed after retries",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .source_schema = std::nullopt,
                            .source_table = std::nullopt,
                            .context = {
                                {"error", ex.what()},
                                {"errors", errors},
                                {"retries", retry},
                                {"clear_error", clear_ex.what()},
                            },
                        });
                    }
                    throw;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100 * (1 << retry)));
            }
        }
    };

    try {
        while (std::chrono::steady_clock::now() < deadline && events_seen < max_events) {
            rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, poll_timeout_ms);
            if (!msg) {
                empty_polls += 1;
                if (empty_polls >= empty_poll_quiet) {
                    flush_batch();
                    if (events_seen == 0 && pending_batch.empty()) {
                        stop_reason = "idle_no_messages";
                        loop_exited_early = true;
                        log_write(log_pg, {
                            .level = LogLevel::Info,
                            .component = "cdc_kafka_apply",
                            .message = "kafka poll idle no messages",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .source_schema = std::nullopt,
                            .source_table = std::nullopt,
                            .context = {
                                {"empty_polls", empty_polls},
                                {"topic_count", static_cast<int>(topics.size())},
                                {"consumer_group", consumer_group},
                            },
                        });
                        break;
                    }
                }
                continue;
            }

            if (msg->err) {
                if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    rd_kafka_message_destroy(msg);
                    empty_polls += 1;
                    if (empty_polls >= empty_poll_quiet) {
                        flush_batch();
                        if (events_seen == 0 && pending_batch.empty()) {
                            stop_reason = "idle_no_messages";
                            loop_exited_early = true;
                            break;
                        }
                    }
                    continue;
                }
                if (msg->err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART ||
                    msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC ||
                    msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
                    rd_kafka_message_destroy(msg);
                    empty_polls += 1;
                    continue;
                }
                std::string kerr = rd_kafka_message_errstr(msg);
                if (kerr.find("Subscribed topic not available") != std::string::npos) {
                    rd_kafka_message_destroy(msg);
                    empty_polls += 1;
                    continue;
                }
                rd_kafka_message_destroy(msg);
                throw std::runtime_error(kerr);
            }

            const std::string topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
            const int partition = msg->partition;
            const long long offset = msg->offset;
            events_seen += 1;
            empty_polls = 0;
            last_offsets[{topic, partition}] = offset;

            const std::string payload(static_cast<const char*>(msg->payload), msg->len);
            rd_kafka_message_destroy(msg);

            account_lag_message(
                payload,
                topic,
                partition,
                offset,
                db_engine,
                meta_by_key,
                lag_tracker,
                lag_seen_offsets);

            if (!first_kafka_message_logged) {
                first_kafka_message_logged = true;
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_apply",
                    .message = "kafka first message received",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"topic", topic},
                        {"partition", partition},
                        {"offset", offset},
                        {"payload_bytes", static_cast<int>(payload.size())},
                    },
                });
            }
            if (events_seen - kafka_poll_progress_last >= 100) {
                kafka_poll_progress_last = events_seen;
                const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start).count();
                const double throughput = elapsed_ms > 0
                    ? static_cast<double>(events_seen) / (elapsed_ms / 1000.0) : 0.0;
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_apply",
                    .message = "kafka poll progress",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"events_seen", events_seen},
                        {"events_applied", events_applied},
                        {"parse_skipped", parse_skipped},
                        {"errors", errors},
                        {"throughput_eps", throughput},
                        {"pending_batch", static_cast<int>(pending_batch.size())},
                        {"empty_polls", empty_polls},
                        {"elapsed_ms", elapsed_ms},
                    },
                });
            }

            const json probe = kafka_apply_detail::parse_kafka_message_json(payload);
            if (probe.is_discarded() || !probe.is_object()) {
                parse_skipped += 1;
                if (parse_skipped <= 10) {
                    log_write(log_pg, {
                        .level = LogLevel::Warning,
                        .component = "cdc_kafka_apply",
                        .message = "kafka message parse skipped",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"topic", topic},
                            {"partition", partition},
                            {"offset", offset},
                            {"payload_bytes", static_cast<int>(payload.size())},
                            {"payload_preview", payload.substr(0, 200)},
                            {"parse_skipped_count", parse_skipped},
                        },
                    });
                }
                continue;
            }
            std::string schema_name;
            std::string table_name;
            const json src = probe.contains("source") && probe["source"].is_object() ? probe["source"] : json::object();
            const std::string actual_engine = probe.value("db_engine", src.value("db_engine", db_engine));
            if (actual_engine == "mssql") {
                const std::string src_db = probe.value("source_database", src.value("database", ""));
                const std::string src_sch = probe.value("source_schema", src.value("db", ""));
                const std::string src_tbl = probe.value("source_table", src.value("table", ""));
                schema_name = mssql_pg_schema_name(src_db, src_sch);
                table_name = mssql_pg_table_name(src_tbl);
            } else if (actual_engine == "mongodb") {
                const std::string src_db = probe.value("source_database", src.value("database", ""));
                const std::string src_coll = probe.value("source_table", src.value("collection", ""));
                schema_name = mongo_pg_schema_name(src_db);
                table_name = mongo_pg_table_name(src_coll);
            } else {
                schema_name = probe.value("source_schema", probe.value("schema", ""));
                table_name = probe.value("source_table", probe.value("table", ""));
                if (schema_name.empty()) {
                    schema_name = src.value("db", "");
                }
                if (table_name.empty()) {
                    table_name = src.value("table", "");
                }
            }
            const TableKey key{schema_name, table_name};
            if (!wanted.count(key) || quarantined.count(key)) {
                auto tp = std::make_pair(topic, partition);
                auto sit = skipped_shard_offsets.find(tp);
                if (sit == skipped_shard_offsets.end() || offset > sit->second) {
                    skipped_shard_offsets[tp] = offset;
                }
                continue;
            }
            ApplyEvent event;
            if (!parse_kafka_payload(
                    probe,
                    event,
                    topic,
                    partition,
                    offset,
                    pk_by_table[key],
                    db_engine)) {
                parse_skipped += 1;
                parse_skipped_by_table[key] += 1;
                if (parse_skipped <= 10) {
                    log_write(log_pg, {
                        .level = LogLevel::Warning,
                        .component = "cdc_kafka_apply",
                        .message = "kafka payload parse skipped",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = schema_name,
                        .source_table = table_name,
                        .context = {
                            {"topic", topic},
                            {"partition", partition},
                            {"offset", offset},
                            {"parse_skipped_count", parse_skipped},
                        },
                    });
                }
                continue;
            }
            event.catalog_id = meta_by_key[key].catalog_id;

            if (events_applied == 0 && pending_batch.empty() && seen_by_table[key] == 0) {
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_apply",
                    .message = "kafka message parsed for table",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = schema_name,
                    .source_table = table_name,
                    .context = {
                        {"op", event.op},
                        {"topic", topic},
                        {"partition", partition},
                        {"offset", offset},
                        {"event_id", event.event_id.substr(0, 64)},
                    },
                });
            }

            seen_by_table[key] = seen_by_table[key] + 1;

            pending_batch.push_back(std::move(event));
            if (static_cast<int>(pending_batch.size()) >= batch_size) {
                flush_batch();
            }
        }

        flush_batch();

        if (!skipped_shard_offsets.empty()) {
            commit_kafka_offsets(rk, skipped_shard_offsets);
            for (const auto& [tp, offset] : skipped_shard_offsets) {
                last_offsets[tp] = std::max(last_offsets[tp], offset);
            }
        }

        if (!loop_exited_early) {
            if (events_seen >= max_events) {
                stop_reason = "max_events";
            } else if (std::chrono::steady_clock::now() >= deadline) {
                stop_reason = "max_seconds";
            } else if (stop_reason == "unknown") {
                stop_reason = "idle_complete";
            }
        }

        drain_kafka_lag_tail(
            rk,
            db_engine,
            meta_by_key,
            lag_tracker,
            lag_seen_offsets,
            empty_poll_quiet);
        refresh_apply_position_health(app_pg.raw, conn_id, staleness);
        finalize_slice_table_lag(
            app_pg.raw,
            rk,
            bootstrap,
            conn_id,
            batch_id,
            db_engine,
            meta_by_key,
            seen_by_table,
            applied_by_table,
            pending_batch,
            lag_tracker,
            slice_flush_stats,
            parse_skipped_by_table,
            dropped_unrecoverable_by_table,
            staleness,
            inactive_seconds,
            table_lag_scan_timeout_ms);

        const long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - start)
                                          .count();

        stats["events_seen"] = events_seen;
        stats["events_applied"] = events_applied;
        stats["parse_skipped"] = parse_skipped;
        stats["errors"] = errors;
        stats["stop_reason"] = stop_reason;
        stats["duration_ms"] = duration_ms;

        log_write(log_pg, {
            .level = (errors > 0 || parse_skipped > 0) ? LogLevel::Warning : LogLevel::Info,
            .component = "cdc_kafka_apply",
            .message = errors > 0 ? "kafka-apply completed with errors"
                : (parse_skipped > 0 ? "kafka-apply completed with parse skips" : "kafka-apply completed"),
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"worker_id", worker_id},
                {"worker_count", worker_count},
                {"stop_reason", stop_reason},
                {"events_seen", events_seen},
                {"events_applied", events_applied},
                {"parse_skipped", parse_skipped},
                {"duration_ms", duration_ms},
                {"errors", errors},
            },
        });

    } catch (const std::exception& ex) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = "kafka-apply failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", ex.what()}},
        });
        stats["errors"] = 1;
        stats["stop_reason"] = "fatal";
        stats["duration_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
        std::cout << stats.dump() << std::endl;
        try {
            clear_stale_cdc_in_progress(app_pg.raw, conn_id, db_engine);
        } catch (...) {
        }
        rd_kafka_consumer_close(rk);
        delete static_cast<KafkaApplyContext*>(rd_kafka_opaque(rk));
        rd_kafka_destroy(rk);
        return 1;
    }

    try {
        const int cleared = clear_stale_cdc_in_progress(app_pg.raw, conn_id, db_engine);
        if (cleared > 0) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_apply",
                .message = "stale cdc_in_progress cleared after apply slice",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"cleared", cleared}},
            });
        }
    } catch (...) {
    }

    rd_kafka_consumer_close(rk);
    delete static_cast<KafkaApplyContext*>(rd_kafka_opaque(rk));
    rd_kafka_destroy(rk);
    std::cout << stats.dump() << std::endl;
    return errors > 0 ? 1 : 0;
}

#endif  // HAVE_RDKAFKA
