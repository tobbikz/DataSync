#ifdef HAVE_RDKAFKA

#include "kafka_apply.hpp"
#include "kafka_apply_detail.hpp"
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
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

using json = nlohmann::json;
using TableKey = std::pair<std::string, std::string>;
using kafka_apply_detail::ApplyEvent;
using kafka_apply_detail::fetch_apply_cursors_for_tables;
using kafka_apply_detail::record_slice_table_lag_stats;
using kafka_apply_detail::SliceFlushStats;
using kafka_apply_detail::SliceLagTableState;
using kafka_table_lag::compute_kafka_partition_lag;
using kafka_apply_detail::ApplyBatchOptions;
using kafka_apply_detail::apply_events_batch;
using kafka_apply_detail::configure_apply_session_resources;
using kafka_apply_detail::configure_lake_apply_session;
using kafka_apply_detail::fill_table_key_from_event_id;
using kafka_apply_detail::resolve_event_lake_key_from_catalog;
using kafka_apply_detail::parse_kafka_payload;
using kafka_apply_detail::record_dropped_unrecoverable;

struct KafkaApplyContext {
    PGconn* log_pg{nullptr};
    std::string batch_id;
    std::string conn_id;
    int worker_id{0};
    int worker_count{1};
};

void rebalance_cb(rd_kafka_t* rk, rd_kafka_resp_err_t err,
                  rd_kafka_topic_partition_list_t* partitions, void* opaque);

struct KafkaApplySessionImpl {
    std::string bootstrap;
    std::string consumer_group;
    int worker_id{-1};
    int worker_count{-1};
    std::vector<std::string> topics;
    rd_kafka_t* rk{nullptr};
    KafkaApplyContext* ctx{nullptr};
};

void destroy_apply_kafka(KafkaApplySessionImpl* impl) {
    if (!impl || !impl->rk) {
        return;
    }
    rd_kafka_consumer_close(impl->rk);
    delete impl->ctx;
    impl->ctx = nullptr;
    rd_kafka_destroy(impl->rk);
    impl->rk = nullptr;
}

struct AcquireConsumerResult {
    rd_kafka_t* rk{nullptr};
    std::string error;
    std::string stop_reason;
};

AcquireConsumerResult acquire_or_reuse_consumer(
    KafkaApplySession* session,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::vector<std::string>& topics,
    int topic_partitions,
    int fetch_max_bytes,
    int max_partition_fetch_bytes,
    int queued_min_messages,
    int fetch_wait_max_ms) {
    AcquireConsumerResult out;
    if (!session) {
        out.error = "kafka apply session is null";
        out.stop_reason = "kafka_init_failed";
        return out;
    }

    std::vector<std::string> sorted = topics;
    std::sort(sorted.begin(), sorted.end());

    auto subscribe_topics = [&](rd_kafka_t* rk) -> rd_kafka_resp_err_t {
        rd_kafka_topic_partition_list_t* sub =
            rd_kafka_topic_partition_list_new(static_cast<int>(topics.size()));
        for (const auto& topic : topics) {
            rd_kafka_topic_partition_list_add(sub, topic.c_str(), RD_KAFKA_PARTITION_UA);
        }
        const rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, sub);
        rd_kafka_topic_partition_list_destroy(sub);
        return err;
    };

    auto* impl = static_cast<KafkaApplySessionImpl*>(session->impl);
    if (impl && impl->rk && impl->bootstrap == bootstrap && impl->consumer_group == consumer_group &&
        impl->worker_id == worker_id && impl->worker_count == worker_count) {
        impl->ctx->log_pg = log_pg;
        impl->ctx->batch_id = batch_id;
        impl->ctx->conn_id = conn_id;
        if (impl->topics != sorted) {
            try {
                ensure_kafka_topics_exist(bootstrap, topics, topic_partitions, 1);
            } catch (const std::exception& ex) {
                out.error = ex.what();
                out.stop_reason = "kafka_ensure_topics_failed";
                return out;
            }
            const rd_kafka_resp_err_t sub_err = subscribe_topics(impl->rk);
            if (sub_err) {
                out.error = rd_kafka_err2str(sub_err);
                out.stop_reason = "kafka_subscribe_failed";
                session->reset();
                return out;
            }
            impl->topics = sorted;
        }
        out.rk = impl->rk;
        return out;
    }

    session->reset();
    try {
        ensure_kafka_topics_exist(bootstrap, topics, topic_partitions, 1);
    } catch (const std::exception& ex) {
        out.error = ex.what();
        out.stop_reason = "kafka_ensure_topics_failed";
        return out;
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

    auto* ctx = new KafkaApplyContext{log_pg, batch_id, conn_id, worker_id, worker_count};
    rd_kafka_conf_set_opaque(conf, ctx);
    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        delete ctx;
        rd_kafka_conf_destroy(conf);
        out.error = errstr;
        out.stop_reason = "kafka_init_failed";
        return out;
    }
    rd_kafka_poll_set_consumer(rk);
    const rd_kafka_resp_err_t sub_err = subscribe_topics(rk);
    if (sub_err) {
        rd_kafka_consumer_close(rk);
        delete ctx;
        rd_kafka_destroy(rk);
        out.error = rd_kafka_err2str(sub_err);
        out.stop_reason = "kafka_subscribe_failed";
        return out;
    }

    impl = new KafkaApplySessionImpl{
        bootstrap, consumer_group, worker_id, worker_count, sorted, rk, ctx};
    session->impl = impl;
    out.rk = rk;
    return out;
}

void kafka_apply_session_reset(void*& impl) {
    auto* p = static_cast<KafkaApplySessionImpl*>(impl);
    if (!p) {
        return;
    }
    destroy_apply_kafka(p);
    delete p;
    impl = nullptr;
}

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
};

std::vector<CatalogMeta> fetch_apply_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& db_engine) {
    const auto rows = fetch_conn_catalog_tables(
        pg, conn_id, worker_id, worker_count, db_engine, CatalogPipeline::Apply);
    std::vector<CatalogMeta> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        CatalogMeta meta;
        meta.catalog_id = row.catalog_id;
        meta.pk_columns = row.pk_columns;
        meta.source_database = row.source_database;
        meta.source_table = row.source_table;
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

void finalize_slice_table_lag(
    PGconn* app_pg,
    rd_kafka_t* rk,
    const std::string& conn_id,
    const std::string& batch_id,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::map<TableKey, int>& seen_by_table,
    const std::map<TableKey, int>& applied_by_table,
    const std::map<TableKey, SliceFlushStats>& slice_flush_stats,
    const std::map<TableKey, int>& parse_skipped_by_table,
    const std::map<TableKey, int>& dropped_unrecoverable_by_table,
    const std::map<TableKey, SliceLagTableState>& cursor_states,
    const std::map<std::pair<std::string, int>, long long>& last_offsets,
    int apply_staleness_seconds,
    int apply_inactive_seconds) {
    std::map<std::pair<std::string, int>, long long> part_lag_cache;
    for (const auto& [lake_key, meta] : meta_by_key) {
        SliceLagTableState state;
        state.events_seen_in_slice = seen_by_table.count(lake_key) ? seen_by_table.at(lake_key) : 0;
        state.events_applied_in_slice = applied_by_table.count(lake_key) ? applied_by_table.at(lake_key) : 0;
        state.inactive = state.events_seen_in_slice <= 0;
        SliceFlushStats merged;
        if (const auto fit = slice_flush_stats.find(lake_key); fit != slice_flush_stats.end()) {
            merged = fit->second;
        }
        if (state.events_seen_in_slice <= 0 && !merged.has_flush) {
            continue;
        }

        if (merged.has_flush && !merged.kafka_topic.empty()) {
            state.kafka_topic = merged.kafka_topic;
            state.kafka_partition = merged.kafka_partition;
            state.kafka_offset = merged.kafka_offset;
        } else if (const auto cit = cursor_states.find(lake_key); cit != cursor_states.end()) {
            state.kafka_topic = cit->second.kafka_topic;
            state.kafka_partition = cit->second.kafka_partition;
            state.kafka_offset = cit->second.kafka_offset;
        }
        const auto tp = std::make_pair(state.kafka_topic, state.kafka_partition);
        if (const auto lit = last_offsets.find(tp); lit != last_offsets.end()) {
            state.kafka_offset = std::max(state.kafka_offset, lit->second);
        }

        if (!state.kafka_topic.empty() && state.kafka_partition >= 0 && state.kafka_offset >= 0) {
            if (const auto cached = part_lag_cache.find(tp); cached != part_lag_cache.end()) {
                state.partition_lag = cached->second;
            } else {
                state.partition_lag = compute_kafka_partition_lag(
                    rk, state.kafka_topic, state.kafka_partition, state.kafka_offset);
                part_lag_cache[tp] = state.partition_lag;
            }
            state.table_lag = state.partition_lag < 0 ? 0 : state.partition_lag;
            state.lag_scan_complete = state.partition_lag >= 0;
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
    // Seed only missing rows — full upsert every slice raced on apply_position_object_uk/pkey.
    std::set<long long> missing;
    {
        const char* vals[] = {conn_id.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            SELECT c.catalog_id
            FROM cdc_catalog.catalog c
            WHERE c.conn_id = $1
              AND c.active = true
              AND c.has_pk = true
              AND NOT EXISTS (
                  SELECT 1 FROM cdc_catalog.apply_position ap
                  WHERE ap.catalog_id = c.catalog_id
              )
            )",
            1,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
            for (int i = 0; i < PQntuples(res); ++i) {
                missing.insert(std::stoll(PQgetvalue(res, i, 0)));
            }
        }
        if (res) {
            PQclear(res);
        }
    }
    if (missing.empty()) {
        return;
    }

    for (const auto& [lake_key, meta] : meta_by_key) {
        if (!missing.count(meta.catalog_id)) {
            continue;
        }
        const std::string topic = topic_for_catalog_table(
            topic_prefix, lake_key.first, lake_key.second, topic_mode, topic_buckets);
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
    rd_kafka_t* rk = nullptr) {
    (void)rk;
    std::map<std::pair<std::string, int>, long long> batch_offsets;
    if (pending.empty()) {
        return batch_offsets;
    }

    std::vector<ApplyEvent> to_apply = std::move(pending);
    pending.clear();

    int batch_table_errors = 0;
    std::vector<ApplyEvent> failed_events;
    long long applied_n = 0;
    try {
        ApplyBatchOptions options;
        options.source_system = source_system;
        options.apply_staleness_seconds = apply_staleness_seconds;
        options.apply_inactive_seconds = apply_inactive_seconds;
        failed_events.clear();
        options.table_errors_out = &batch_table_errors;
        options.failed_events_out = &failed_events;
        options.parse_skipped_by_table = &parse_skipped_by_table;
        options.dropped_unrecoverable_by_table = dropped_unrecoverable_by_table;
        options.slice_flush_stats = slice_flush_stats;
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

KafkaApplySession::~KafkaApplySession() { reset(); }

void KafkaApplySession::reset() { kafka_apply_session_reset(impl); }

int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    KafkaApplySession* session) {
    const auto start = std::chrono::steady_clock::now();
    const std::string batch_id = make_batch_id();
    json stats = {
        {"batch_id", batch_id},
        {"events_seen", 0},
        {"events_applied", 0},
        {"errors", 0},
    };

    if (!app_pg || !lake_pg) {
        return 1;
    }
    PGconn* log_pg = app_pg;

    configure_apply_session_resources(app_pg);
    configure_lake_apply_session(lake_pg);

    if (worker_id == 0) {
        const auto idx_stats = backfill_mirror_apply_pk_indexes(app_pg, lake_pg, conn_id);
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
    clear_stale_cdc_in_progress(app_pg, conn_id, db_engine);

    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    const std::string bootstrap = kafka.bootstrap;
    if (worker_count <= 0) {
        worker_count = pipeline_defaults::kApplyWorkerCount;
    }
    const std::string consumer_group =
        kafka_apply_consumer_group(conn_id, worker_id, worker_count);
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
    const int batch_size = pipeline_defaults::kApplyBatchSizeDefault;
    const int staleness = pipeline_defaults::kApplyMaxTableStalenessSeconds;
    const int inactive_seconds = pipeline_defaults::kApplyInactiveSeconds;
    const int queued_min_messages = pipeline_defaults::kApplyQueuedMinMessages;
    const int fetch_wait_max_ms = pipeline_defaults::kApplyFetchWaitMaxMs;
    const int topic_partitions = pipeline_defaults::kKafkaTopicPartitions;
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
        app_pg, conn_id, worker_id, worker_count, meta_by_key, db_engine);
    if (meta_by_key.empty()) {
        const ApplySkipReasonCounts reasons =
            fetch_apply_skip_reasons(app_pg, conn_id, db_engine);
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

    std::set<TableKey> wanted;
    std::vector<std::pair<std::string, std::string>> table_pairs;
    std::map<TableKey, std::vector<std::string>> pk_by_table;
    for (const auto& [key, meta] : meta_by_key) {
        wanted.insert(key);
        table_pairs.push_back(key);
        pk_by_table[key] = split_pk_columns(meta.pk_columns);
    }

    if (worker_id == 0) {
        ensure_apply_positions(app_pg, meta_by_key, conn_id, topic_prefix, topic_mode, topic_buckets);
    }

    std::set<TableKey> quarantined = fetch_quarantined(app_pg, conn_id, meta_by_key);

    const auto topics =
        topics_for_tables(topic_prefix, table_pairs, topic_mode, topic_buckets);

    KafkaApplySession local_session;
    if (!session) {
        session = &local_session;
    }
    const AcquireConsumerResult acquired = acquire_or_reuse_consumer(
        session,
        log_pg,
        batch_id,
        conn_id,
        worker_id,
        worker_count,
        bootstrap,
        consumer_group,
        topics,
        topic_partitions,
        fetch_max_bytes,
        max_partition_fetch_bytes,
        queued_min_messages,
        fetch_wait_max_ms);
    rd_kafka_t* rk = acquired.rk;
    if (!rk) {
        const std::string msg = acquired.stop_reason == "kafka_init_failed"
            ? "kafka consumer create failed"
            : (acquired.stop_reason == "kafka_subscribe_failed" ? "kafka subscribe failed"
                                                               : "kafka ensure topics failed");
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply",
            .message = msg,
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"error", acquired.error},
                {"topic_count", static_cast<int>(topics.size())},
            },
        });
        stats["stop_reason"] = acquired.stop_reason.empty() ? "kafka_init_failed" : acquired.stop_reason;
        stats["errors"] = 1;
        stats["duration_ms"] = 0;
        std::cout << stats.dump() << std::endl;
        return 1;
    }

    std::map<TableKey, long long> catalog_id_by_lake_key;
    for (const auto& [key, meta] : meta_by_key) {
        catalog_id_by_lake_key[key] = meta.catalog_id;
    }
    const auto cursor_states = fetch_apply_cursors_for_tables(app_pg, catalog_id_by_lake_key);

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
                    app_pg,
                    lake_pg,
                    conn_id,
                    batch_id,
                    pending_batch,
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
                    rk);
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
                            clear_stale_cdc_in_progress(app_pg, conn_id, db_engine);
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

        if (events_seen > 0) {
            refresh_apply_position_health(app_pg, conn_id, staleness);
        }
        finalize_slice_table_lag(
            app_pg,
            rk,
            conn_id,
            batch_id,
            meta_by_key,
            seen_by_table,
            applied_by_table,
            slice_flush_stats,
            parse_skipped_by_table,
            dropped_unrecoverable_by_table,
            cursor_states,
            last_offsets,
            staleness,
            inactive_seconds);

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
                {"lag_kind", "partition"},
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
        session->reset();
        return 1;
    }

    try {
        const int cleared = clear_stale_cdc_in_progress(app_pg, conn_id, db_engine);
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

    std::cout << stats.dump() << std::endl;
    return 0;
}

#endif  // HAVE_RDKAFKA
