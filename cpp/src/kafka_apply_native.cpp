#ifdef HAVE_RDKAFKA

#include "kafka_apply.hpp"
#include "kafka_apply_detail.hpp"
#include "host_metrics.hpp"
#include "kafka_lag.hpp"
#include "kafka_topics.hpp"
#include "capture_common.hpp"

#include "config.hpp"
#include "mariadb_schema.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

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
using kafka_apply_detail::ApplyBatchOptions;
using kafka_apply_detail::QuietTableRef;
using kafka_apply_detail::apply_events_batch;
using kafka_apply_detail::filter_new_event_ids;
using kafka_apply_detail::parse_kafka_payload;
using kafka_apply_detail::record_quiet_table_batch_stats;

struct CatalogMeta {
    long long catalog_id{0};
    std::string pk_columns;
    std::string source_database;  // MSSQL only
    std::string source_schema;
    std::string source_table;
};

struct FairnessCounts {
    int tables_met_target{0};
    int tables_starved{0};
    int tables_quiet{0};
    int tables_active{0};
};

std::vector<std::string> split_pk(const std::string& pk_columns) {
    std::vector<std::string> out;
    std::string part;
    std::istringstream iss(pk_columns);
    while (std::getline(iss, part, ',')) {
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.front()))) {
            part.erase(part.begin());
        }
        while (!part.empty() && std::isspace(static_cast<unsigned char>(part.back()))) {
            part.pop_back();
        }
        if (!part.empty()) {
            out.push_back(part);
        }
    }
    return out;
}

bool quiet_in_slice(const TableKey& key, const std::map<TableKey, int>& seen, const std::map<TableKey, int>& applied) {
    const auto seen_it = seen.find(key);
    const auto applied_it = applied.find(key);
    return (seen_it == seen.end() || seen_it->second == 0) &&
           (applied_it == applied.end() || applied_it->second == 0);
}

bool slice_satisfied(
    const TableKey& key,
    const std::map<TableKey, bool>& met,
    const std::map<TableKey, int>& seen,
    const std::map<TableKey, int>& applied,
    int target,
    bool allow_quiet) {
    if (allow_quiet && quiet_in_slice(key, seen, applied)) {
        return true;
    }
    if (met.count(key) && met.at(key)) {
        return true;
    }
    const int seen_n = seen.count(key) ? seen.at(key) : 0;
    const int applied_n = applied.count(key) ? applied.at(key) : 0;
    if (target <= 0) {
        if (allow_quiet) {
            return seen_n == 0 || applied_n >= seen_n;
        }
        return false;
    }
    if (seen_n > 0 && applied_n >= seen_n) {
        return true;
    }
    if (applied_n >= target) {
        return true;
    }
    return false;
}

std::pair<bool, std::string> should_stop_slice(
    const std::set<TableKey>& wanted,
    const std::set<TableKey>& quarantined,
    const std::map<TableKey, int>& seen,
    const std::map<TableKey, int>& applied,
    const std::map<TableKey, bool>& met,
    int target,
    const std::set<TableKey>& stale_keys,
    bool allow_quiet,
    const std::map<TableKey, int>* processed) {
    std::vector<TableKey> active;
    for (const auto& key : wanted) {
        if (!quarantined.count(key)) {
            active.push_back(key);
        }
    }
    if (active.empty()) {
        return {true, "all_quarantined"};
    }

    const std::map<TableKey, int>& progress = processed ? *processed : applied;
    for (const auto& key : active) {
        if (stale_keys.count(key)) {
            const int seen_n = seen.count(key) ? seen.at(key) : 0;
            const int prog_n = progress.count(key) ? progress.at(key) : 0;
            if (seen_n > prog_n) {
                return {false, "stale_priority"};
            }
        }
    }

    bool all_ok = true;
    for (const auto& key : active) {
        if (!slice_satisfied(key, met, seen, applied, target, allow_quiet)) {
            all_ok = false;
            break;
        }
    }

    bool has_unapplied = false;
    for (const auto& key : active) {
        const int seen_n = seen.count(key) ? seen.at(key) : 0;
        const int prog_n = progress.count(key) ? progress.at(key) : 0;
        if (seen_n > prog_n) {
            has_unapplied = true;
            break;
        }
    }

    bool has_stale_unapplied = false;
    for (const auto& key : active) {
        if (stale_keys.count(key)) {
            const int seen_n = seen.count(key) ? seen.at(key) : 0;
            const int applied_n = applied.count(key) ? applied.at(key) : 0;
            if (seen_n > 0 && applied_n == 0) {
                has_stale_unapplied = true;
                break;
            }
        }
    }

    if (all_ok && !has_stale_unapplied && !has_unapplied) {
        return {true, "all_targets_met"};
    }
    return {false, ""};
}

FairnessCounts finalize_slice_reporting(
    const std::set<TableKey>& wanted,
    const std::set<TableKey>& quarantined,
    const std::map<TableKey, int>& seen,
    const std::map<TableKey, int>& applied,
    std::map<TableKey, bool>& met,
    int target) {
    FairnessCounts counts;
    for (const auto& key : wanted) {
        if (quarantined.count(key)) {
            continue;
        }
        if (quiet_in_slice(key, seen, applied)) {
            counts.tables_quiet += 1;
        } else {
            const int seen_n = seen.count(key) ? seen.at(key) : 0;
            const int applied_n = applied.count(key) ? applied.at(key) : 0;
            if (seen_n > 0 || applied_n > 0) {
                counts.tables_active += 1;
            }
        }
        if (slice_satisfied(key, met, seen, applied, target, true)) {
            met[key] = true;
        }
    }
    for (const auto& key : wanted) {
        if (met.count(key) && met.at(key)) {
            counts.tables_met_target += 1;
        }
    }
    for (const auto& key : wanted) {
        if (quarantined.count(key)) {
            continue;
        }
        const int seen_n = seen.count(key) ? seen.at(key) : 0;
        const int applied_n = applied.count(key) ? applied.at(key) : 0;
        if (seen_n > 0 && applied_n == 0 && !(met.count(key) && met.at(key))) {
            counts.tables_starved += 1;
        }
    }
    return counts;
}

std::string runtime_topic_prefix(RuntimeConfig& runtime, const std::string& conn_id, const std::string& db_engine = "mariadb") {
    if (db_engine == "mssql") {
        // Do not fall back to cdc_kafka_apply global prefix (MARIADB_LOCAL) — MSSQL topics are separate.
        return runtime.get_string("capture_topic_prefix", "MSSQL_LOCAL", "cdc_kafka_mssql_capture", conn_id);
    }
    if (db_engine == "mongodb") {
        return runtime.get_string("capture_topic_prefix", "MONGO_LOCAL", "cdc_kafka_mongo_capture", conn_id);
    }
    const std::string apply_prefix = runtime.get_string("kafka_topic_prefix", "", "cdc_kafka_apply", conn_id);
    if (!apply_prefix.empty()) {
        return apply_prefix;
    }
    return runtime.get_string("capture_topic_prefix", conn_id, "cdc_kafka_capture", conn_id);
}

std::vector<CatalogMeta> fetch_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    int worker_id,
    int worker_count,
    std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& db_engine = "mariadb") {
    std::ostringstream sql;
    std::vector<const char*> vals;
    vals.push_back(conn_id.c_str());
    int param_count = 1;
    std::string tier_val;
    std::string worker_count_str;
    std::string worker_id_str;

    if (db_engine == "mssql") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mssql'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else if (db_engine == "mongodb") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mongodb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else {
        sql << R"(
            SELECT catalog_id, conn_id, '' AS source_database, source_schema, source_table, pk_columns
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mariadb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND needs_full_load = false
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    }

    if (tier && !tier->empty()) {
        sql << " AND service_tier::text = lower($" << ++param_count << ")";
        tier_val = *tier;
        vals.push_back(tier_val.c_str());
    }
    if (worker_count > 1) {
        const int mod_param = ++param_count;
        const int eq_param = ++param_count;
        sql << " AND mod(catalog_id, $" << mod_param << ") = $" << eq_param;
        worker_count_str = std::to_string(worker_count);
        worker_id_str = std::to_string(worker_id);
        vals.push_back(worker_count_str.c_str());
        vals.push_back(worker_id_str.c_str());
    }
    sql << " ORDER BY source_schema, source_table";

    PGresult* res = PQexecParams(
        pg,
        sql.str().c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.data(),
        nullptr,
        nullptr,
        0);
    std::vector<CatalogMeta> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        CatalogMeta meta;
        meta.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        const std::string src_db = PQgetvalue(res, i, 2);
        const std::string src_schema = PQgetvalue(res, i, 3);
        const std::string src_table = PQgetvalue(res, i, 4);
        meta.pk_columns = PQgetvalue(res, i, 5);
        meta.source_table = src_table;
        meta.source_database = src_db;
        if (db_engine == "mongodb") {
            meta.source_schema = mongo_catalog_source_schema(src_db, src_schema);
        } else {
            meta.source_schema = src_schema;
        }
        TableKey key;
        if (db_engine == "mssql") {
            key = {mssql_pg_schema_name(src_db, src_schema), mssql_pg_table_name(src_table)};
        } else if (db_engine == "mongodb") {
            key = {mongo_pg_schema_name(src_db), mongo_pg_table_name(src_table)};
        } else {
            key = {src_schema, src_table};
        }
        meta_by_key[key] = meta;
        out.push_back(meta);
    }
    PQclear(res);
    return out;
}

long long prune_by_retention_fn(PGconn* pg, const char* fn_sql, int retention_days) {
    if (retention_days <= 0) {
        return 0;
    }
    const std::string days = std::to_string(retention_days);
    const char* vals[] = {days.c_str()};
    PGresult* res = PQexecParams(pg, fn_sql, 1, nullptr, vals, nullptr, nullptr, 0);
    long long pruned = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        pruned = std::atoll(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return pruned;
}

long long prune_applied_events(PGconn* pg, int retention_days) {
    return prune_by_retention_fn(pg, "SELECT cdc_catalog.prune_applied_events($1::integer)", retention_days);
}

long long prune_apply_batch_stats(PGconn* pg, int retention_days) {
    return prune_by_retention_fn(pg, "SELECT cdc_catalog.prune_apply_batch_stats($1::integer)", retention_days);
}

void ensure_apply_positions(
    PGconn* pg,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& conn_id,
    const std::string& topic_prefix,
    const std::string& topic_mode,
    int topic_buckets) {
    for (const auto& [key, meta] : meta_by_key) {
        const std::string topic = topic_for_catalog(topic_prefix, key.first, key.second, topic_mode, topic_buckets);
        const std::string cid = std::to_string(meta.catalog_id);
        const char* vals[] = {cid.c_str(), conn_id.c_str(), meta.source_schema.c_str(), meta.source_table.c_str(), topic.c_str()};
        pg_exec_params_simple(
            pg,
            R"(
            INSERT INTO cdc_catalog.apply_position
                (catalog_id, conn_id, source_schema, source_table, kafka_topic, status)
            VALUES ($1::bigint, $2, $3, $4, $5, 'healthy')
            ON CONFLICT (catalog_id) DO UPDATE SET
                conn_id = EXCLUDED.conn_id,
                source_schema = EXCLUDED.source_schema,
                source_table = EXCLUDED.source_table,
                kafka_topic = EXCLUDED.kafka_topic,
                updated_at = now()
            )",
            5,
            vals);
    }
}

void insert_fairness_metrics(
    PGconn* pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& tier,
    const std::string& stop_reason,
    const FairnessCounts& counts,
    long long events_seen,
    long long events_applied,
    long long duration_ms,
    int wanted) {
    const std::string tier_val = tier.value_or("");
    const std::string seen = std::to_string(events_seen);
    const std::string applied = std::to_string(events_applied);
    const std::string duration = std::to_string(duration_ms);
    const std::string total = std::to_string(wanted);
    const std::string met = std::to_string(counts.tables_met_target);
    const std::string starved = std::to_string(counts.tables_starved);
    const std::string quiet = std::to_string(counts.tables_quiet);
    const std::string ctx = json{{"tables_active", counts.tables_active}}.dump();
    const char* vals[] = {
        batch_id.c_str(),
        conn_id.c_str(),
        tier_val.c_str(),
        stop_reason.c_str(),
        total.c_str(),
        met.c_str(),
        starved.c_str(),
        quiet.c_str(),
        seen.c_str(),
        applied.c_str(),
        duration.c_str(),
        ctx.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.cdc_run_fairness_metrics
            (batch_id, conn_id, service_tier, stop_reason,
             tables_total, tables_met_target, tables_starved, tables_quiet,
             events_seen, events_applied, duration_ms, context)
        VALUES ($1, $2, NULLIF($3, ''), $4, $5::integer, $6::integer, $7::integer, $8::integer,
                $9::bigint, $10::bigint, $11::integer, $12::jsonb)
        )",
        12,
        vals);
}

std::set<TableKey> fetch_quarantined(PGconn* pg, const std::string& conn_id) {
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
            out.insert({PQgetvalue(res, i, 0), PQgetvalue(res, i, 1)});
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

std::set<TableKey> fetch_stale_keys(PGconn* pg, const std::string& conn_id, int staleness, const std::set<TableKey>& wanted) {
    std::set<TableKey> out;
    const std::string staleness_str = std::to_string(staleness);
    const char* vals[] = {conn_id.c_str(), staleness_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(SELECT source_schema, source_table FROM cdc_catalog.apply_position
           WHERE conn_id = $1
             AND last_applied_at IS NOT NULL
             AND extract(epoch from (now() - last_applied_at)) > $2::integer)",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res); ++i) {
            TableKey key{PQgetvalue(res, i, 0), PQgetvalue(res, i, 1)};
            if (wanted.count(key)) {
                out.insert(key);
            }
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

bool topics_have_lag(
    rd_kafka_t* rk,
    const std::vector<std::string>& topics,
    const std::map<std::pair<std::string, int>, long long>& last_offsets,
    int partition_count) {
    (void)topics;
    (void)partition_count;

    rd_kafka_topic_partition_list_t* assigned = nullptr;
    rd_kafka_assignment(rk, &assigned);
    if (assigned && assigned->cnt > 0) {
        rd_kafka_position(rk, assigned);
    }

    struct PartitionRef {
        std::string topic;
        int partition{0};
    };
    std::vector<PartitionRef> checks;
    if (assigned && assigned->cnt > 0) {
        checks.reserve(static_cast<std::size_t>(assigned->cnt));
        for (int i = 0; i < assigned->cnt; ++i) {
            const rd_kafka_topic_partition_t* tp = &assigned->elems[i];
            if (tp->partition >= 0) {
                checks.push_back({tp->topic, tp->partition});
            }
        }
    } else if (!last_offsets.empty()) {
        checks.reserve(last_offsets.size());
        for (const auto& [tp, _off] : last_offsets) {
            checks.push_back({tp.first, tp.second});
        }
    } else {
        if (assigned) {
            rd_kafka_topic_partition_list_destroy(assigned);
        }
        for (const auto& topic : topics) {
            for (int p = 0; p < partition_count; ++p) {
                int64_t low = 0;
                int64_t high = 0;
                if (rd_kafka_query_watermark_offsets(rk, topic.c_str(), p, &low, &high, 10000) !=
                    RD_KAFKA_RESP_ERR_NO_ERROR) {
                    continue;
                }
                if (high > low) {
                    return true;
                }
            }
        }
        return false;
    }

    auto position_for = [&](const std::string& topic, int partition) -> long long {
        const auto key = std::make_pair(topic, partition);
        const auto it = last_offsets.find(key);
        if (it != last_offsets.end()) {
            return it->second;
        }
        if (assigned) {
            for (int i = 0; i < assigned->cnt; ++i) {
                const rd_kafka_topic_partition_t* tp = &assigned->elems[i];
                if (topic == tp->topic && partition == tp->partition && tp->offset >= 0) {
                    return tp->offset > 0 ? tp->offset - 1 : -1;
                }
            }
        }
        return -1;
    };

    bool lag = false;
    for (const auto& check : checks) {
        int64_t low = 0;
        int64_t high = 0;
        if (rd_kafka_query_watermark_offsets(rk, check.topic.c_str(), check.partition, &low, &high, 10000) !=
            RD_KAFKA_RESP_ERR_NO_ERROR) {
            continue;
        }
        if (high <= low) {
            continue;
        }
        const long long consumed = position_for(check.topic, check.partition);
        if (consumed + 1 < high) {
            lag = true;
            break;
        }
    }
    if (assigned) {
        rd_kafka_topic_partition_list_destroy(assigned);
    }
    return lag;
}

std::unordered_set<std::string> fetch_catchup_tables_for_batch(
    PGconn* pg,
    const std::string& batch_id,
    const std::string& conn_id) {
    std::unordered_set<std::string> out;
    const char* vals[] = {batch_id.c_str(), conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT DISTINCT source_schema, source_table
        FROM cdc_catalog.logs
        WHERE batch_id = $1
          AND conn_id = $2
          AND message LIKE 'catchup%completed'
          AND source_schema IS NOT NULL
          AND source_table IS NOT NULL
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        const char* schema = PQgetvalue(res, i, 0);
        const char* table = PQgetvalue(res, i, 1);
        if (schema && table) {
            out.insert(std::string(schema) + "|" + std::string(table));
        }
    }
    PQclear(res);
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
    bool append_only,
    bool audit_enabled,
    long long& events_applied,
    int& errors,
    std::map<TableKey, int>& applied_by_table,
    const std::map<TableKey, int>& seen_by_table,
    const std::map<TableKey, CatalogMeta>& meta_by_key,
    const std::string& source_system = "MariaDB",
    const std::string& service_tier = "",
    int apply_staleness_seconds = 900,
    int apply_inactive_seconds = 3600,
    rd_kafka_t* rk = nullptr,
    const std::unordered_set<std::string>* catchup_tables = nullptr,
    const HostMetricsSampler* host_sampler = nullptr) {
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
            continue;
        }
        to_apply.push_back(std::move(e));
    }

    if (to_apply.empty()) {
        return batch_offsets;
    }

    try {
        ApplyBatchOptions options;
        options.append_only = append_only;
        options.audit_enabled = audit_enabled;
        options.source_system = source_system;
        options.service_tier = service_tier;
        options.apply_staleness_seconds = apply_staleness_seconds;
        options.apply_inactive_seconds = apply_inactive_seconds;
        options.host_sampler = host_sampler;
        if (catchup_tables) {
            options.catchup_tables = *catchup_tables;
        }
        for (const auto& [state_key, skipped] : dedup_skipped_by_state) {
            options.slice_table_state[state_key].dedup_skipped += skipped;
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
            const int applied_before = applied_by_table.count(lake_key) ? applied_by_table.at(lake_key) : 0;
            state.events_seen_in_slice = std::max(state.events_seen_in_slice, seen_n);
            state.is_starving = state.is_starving || (seen_n > applied_before);
            if (catchup_tables && catchup_tables->count(state_key) > 0) {
                state.catchup_triggered = true;
            }
            last_kafka_by_state[state_key] = {e.topic, e.partition, e.offset};
        }
        for (const auto& [state_key, kafka_ref] : last_kafka_by_state) {
            const auto& [topic, partition, offset] = kafka_ref;
            options.slice_table_state[state_key].kafka_consumer_lag =
                compute_kafka_consumer_lag(rk, topic, partition, offset);
        }
        json result = apply_events_batch(app_pg, lake_pg, conn_id, batch_id, to_apply, options);
        const long long applied_n = result.value("applied", 0LL);
        events_applied += applied_n;
        for (const auto& e : to_apply) {
            const TableKey key{e.schema_name, e.table_name};
            applied_by_table[key] = applied_by_table[key] + 1;
            batch_offsets[{e.topic, e.partition}] = e.offset;
        }
    } catch (const std::exception& ex) {
        errors += 1;
        throw std::runtime_error(ex.what());
    }

    return batch_offsets;
}

}  // namespace

int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    int worker_id,
    int worker_count) {
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

    const int retention_days = runtime.get_int("logs_retention_days", 7, "global");
    purge_logs(log_pg, retention_days);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_apply_cpp",
        .message = "kafka-apply started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"worker_id", worker_id},
            {"worker_count", worker_count},
            {"tier", service_tier.value_or("")},
            {"apply_batch_size", runtime.get_int("apply_batch_size", 50000, "cdc_kafka_apply", conn_id)},
        },
    });

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    runtime.reload(app_pg.raw);

    const std::string db_engine = conn_engine(cfg, conn_id);
    clear_stale_cdc_in_progress(app_pg.raw, conn_id, service_tier, db_engine);

    const std::string bootstrap = runtime.get_string("kafka_bootstrap_servers", "localhost:9092", "cdc_kafka_apply", conn_id);
    const std::string consumer_group =
        kafka_apply_consumer_group(runtime, log_pg, conn_id, service_tier.value_or(""));
    const std::string topic_prefix = runtime_topic_prefix(runtime, conn_id, db_engine);
    const std::string topic_mode = runtime.get_string("kafka_topic_mode", "bucketed", "cdc_kafka_apply", conn_id);
    const int topic_buckets = runtime.get_int("kafka_topic_buckets", 64, "cdc_kafka_apply", conn_id);
    const int max_seconds = cfg.cdc.slice_max_seconds > 0
        ? cfg.cdc.slice_max_seconds
        : runtime.get_int("apply_max_seconds", 300, "cdc_kafka_apply", conn_id);
    const int max_events = cfg.cdc.slice_max_events > 0
        ? cfg.cdc.slice_max_events
        : runtime.get_int("apply_max_events", 10000000, "cdc_kafka_apply", conn_id);
    const int target = runtime.get_int("apply_target_events_per_table", 0, "cdc_kafka_apply", conn_id);
    const int poll_timeout_ms = runtime.get_int("apply_poll_timeout_ms", 100, "cdc_kafka_apply", conn_id);
    const int fetch_max_bytes = runtime.get_int("apply_fetch_max_bytes", 52428800, "cdc_kafka_apply", conn_id);
    const int max_partition_fetch_bytes = runtime.get_int("apply_max_partition_fetch_bytes", 10485760, "cdc_kafka_apply", conn_id);
    const int empty_poll_quiet = runtime.get_int("apply_empty_poll_quiet_threshold", 10, "cdc_kafka_apply", conn_id);
    const int batch_size = runtime.get_int("apply_batch_size", 50000, "cdc_kafka_apply", conn_id);
    const int staleness = runtime.get_int("apply_max_table_staleness_seconds", 900, "cdc_kafka_apply", conn_id);
    const int inactive_seconds = runtime.get_int("apply_inactive_seconds", 3600, "cdc_kafka_apply", conn_id);
    const int applied_retention = runtime.get_int("applied_events_retention_days", 7, "cdc_kafka_apply", conn_id);
    const int stats_retention = runtime.get_int("apply_batch_stats_retention_days", 30, "cdc_kafka_apply", conn_id);
    const bool dedup_enabled = runtime.get_bool("apply_dedup_enabled", true, "cdc_kafka_apply", conn_id);
    const bool append_only = runtime.get_bool("apply_append_only", true, "cdc_kafka_apply", conn_id);
    const bool audit_enabled = runtime.get_bool("apply_audit_enabled", true, "cdc_kafka_apply", conn_id);
    const int queued_min_messages = runtime.get_int("apply_queued_min_messages", 100000, "cdc_kafka_apply", conn_id);
    const int fetch_wait_max_ms = runtime.get_int("apply_fetch_wait_max_ms", 500, "cdc_kafka_apply", conn_id);
    const long long memory_cap_mb =
        runtime.get_int("apply_process_rss_cap_mb", 10240, "cdc_kafka_apply", conn_id);
    long long memory_resume_mb =
        runtime.get_int("apply_process_rss_resume_mb", 9728, "cdc_kafka_apply", conn_id);
    const int memory_wait_ms =
        runtime.get_int("apply_memory_backpressure_wait_ms", 100, "cdc_kafka_apply", conn_id);
    if (memory_cap_mb > 0) {
        if (memory_resume_mb <= 0 || memory_resume_mb >= memory_cap_mb) {
            memory_resume_mb = memory_cap_mb - 512;
            if (memory_resume_mb < 1) {
                memory_resume_mb = memory_cap_mb * 9 / 10;
            }
        }
    }
    const int topic_partitions = (db_engine == "mssql")
        ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mssql_capture", conn_id)
        : (db_engine == "mongodb")
            ? runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_mongo_capture", conn_id)
            : runtime.get_int("kafka_topic_partitions", 6, "cdc_kafka_capture", conn_id);

    if (worker_count <= 0) {
        worker_count = runtime.get_int("apply_worker_count", 1, "cdc_kafka_apply", conn_id);
    }
    if (worker_count <= 0) {
        worker_count = 1;
    }

    std::map<TableKey, CatalogMeta> meta_by_key;
    fetch_catalog_tables(app_pg.raw, conn_id, service_tier, worker_id, worker_count, meta_by_key, db_engine);
    if (meta_by_key.empty()) {
        const ApplySkipReasonCounts reasons =
            fetch_apply_skip_reasons(app_pg.raw, conn_id, service_tier, db_engine);
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply_cpp",
            .message = "apply slice skipped: no tables",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"tier", service_tier.value_or("")},
                {"db_engine", db_engine},
                {"active_total", reasons.active_total},
                {"needs_full_load", reasons.needs_full_load},
                {"cdc_disabled", reasons.cdc_disabled},
                {"no_pk", reasons.no_pk},
                {"skipped_status", reasons.skipped_status},
                {"apply_ready", reasons.apply_ready},
            },
        });
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
        pk_by_table[key] = split_pk(meta.pk_columns);
    }

    const long long pruned = prune_applied_events(app_pg.raw, applied_retention);
    if (pruned > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply_cpp",
            .message = "pruned applied_events audit rows",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"deleted", pruned}, {"retention_days", applied_retention}},
        });
    }

    const std::unordered_set<std::string> catchup_tables =
        fetch_catchup_tables_for_batch(log_pg, batch_id, conn_id);

    const long long stats_pruned = prune_apply_batch_stats(app_pg.raw, stats_retention);
    if (stats_pruned > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_apply_cpp",
            .message = "pruned apply_batch_stats rows",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"deleted", stats_pruned}, {"retention_days", stats_retention}},
        });
    }

    ensure_apply_positions(app_pg.raw, meta_by_key, conn_id, topic_prefix, topic_mode, topic_buckets);

    std::set<TableKey> quarantined = fetch_quarantined(app_pg.raw, conn_id);
    std::set<TableKey> stale_keys = fetch_stale_keys(app_pg.raw, conn_id, staleness, wanted);

    const auto topics = topics_for_tables(topic_prefix, table_pairs, topic_mode, topic_buckets);

    try {
        ensure_kafka_topics_exist(bootstrap, topics, topic_partitions, 1);
    } catch (const std::exception& ex) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply_cpp",
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
    if (memory_cap_mb > 0) {
        const long long kafka_queue_kb = std::max(65536LL, memory_cap_mb * 256);
        rd_kafka_conf_set(
            conf,
            "queued.max.messages.kbytes",
            std::to_string(kafka_queue_kb).c_str(),
            errstr,
            sizeof(errstr));
    }
    // After Kafka topic purge, apply may subscribe before capture recreates buckets.
    rd_kafka_conf_set(conf, "allow.auto.create.topics", "true", errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply_cpp",
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
            .component = "cdc_kafka_apply_cpp",
            .message = "kafka subscribe failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"error", rd_kafka_err2str(sub_err)}},
        });
        rd_kafka_destroy(rk);
        stats["stop_reason"] = "kafka_subscribe_failed";
        stats["errors"] = 1;
        std::cout << stats.dump() << std::endl;
        return 1;
    }

    std::map<TableKey, int> seen_by_table;
    std::map<TableKey, int> applied_by_table;
    std::map<TableKey, int> processed_by_table;
    std::map<TableKey, bool> met;
    std::map<std::pair<std::string, int>, long long> last_offsets;

    long long events_seen = 0;
    long long events_applied = 0;
    long long parse_skipped = 0;
    int errors = 0;
    std::string stop_reason = "unknown";
    bool loop_exited_early = false;
    int empty_polls = 0;
    int memory_backpressure_waits = 0;
    bool memory_backpressure_logged = false;
    std::vector<ApplyEvent> pending_batch;
    HostMetricsSampler host_sampler;
    host_sampler.mark_start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(max_seconds);

    const std::string source_system =
        (db_engine == "mssql") ? "MSSQL" : (db_engine == "mongodb") ? "MongoDB" : "MariaDB";

    auto flush_batch = [&]() {
        if (pending_batch.empty()) {
            return;
        }
        try {
            auto batch_offsets = flush_pending_batch(
                app_pg.raw,
                lake_pg.raw,
                conn_id,
                batch_id,
                pending_batch,
                dedup_enabled,
                append_only,
                audit_enabled,
                events_applied,
                errors,
                applied_by_table,
                seen_by_table,
                meta_by_key,
                source_system,
                service_tier.value_or(""),
                staleness,
                inactive_seconds,
                rk,
                &catchup_tables,
                &host_sampler);
            commit_kafka_offsets(rk, batch_offsets);
            for (const auto& [tp, offset] : batch_offsets) {
                last_offsets[tp] = std::max(last_offsets[tp], offset);
            }
        } catch (const std::exception& ex) {
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_apply_cpp",
                .message = "apply batch flush failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"error", ex.what()}},
            });
        }
    };

    auto relieve_memory_pressure = [&]() {
        if (memory_cap_mb <= 0) {
            return;
        }
        const long long rss = process_rss_mb();
        if (rss >= 0 && rss < memory_cap_mb) {
            return;
        }
        flush_batch();
        trim_process_heap();
        int spins = 0;
        while (spins < 300) {
            const long long now_rss = process_rss_mb();
            if (now_rss >= 0 && now_rss < memory_resume_mb) {
                break;
            }
            flush_batch();
            trim_process_heap();
            rd_kafka_consumer_poll(rk, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(memory_wait_ms));
            memory_backpressure_waits += 1;
            spins += 1;
        }
        if (!memory_backpressure_logged) {
            memory_backpressure_logged = true;
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_apply_cpp",
                .message = "apply memory backpressure active",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"process_rss_mb", process_rss_mb()},
                    {"cap_mb", memory_cap_mb},
                    {"resume_mb", memory_resume_mb},
                },
            });
        }
    };

    auto stop_slice_if_ready = [&](bool allow_quiet) -> bool {
        auto [stop, reason] = should_stop_slice(
            wanted,
            quarantined,
            seen_by_table,
            applied_by_table,
            met,
            target,
            stale_keys,
            allow_quiet,
            &processed_by_table);
        if (!stop) {
            return false;
        }
        if (reason == "all_targets_met" || reason == "idle_complete" || reason == "slice_complete") {
            flush_batch();
            if (topics_have_lag(rk, topics, last_offsets, topic_partitions)) {
                return false;
            }
        }
        stop_reason = reason.empty() ? "slice_complete" : reason;
        if (stop_reason == "all_quarantined") {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_apply",
                .message = "apply slice skipped: all tables quarantined",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"tier", service_tier.value_or("")},
                    {"quarantined_tables", static_cast<int>(quarantined.size())},
                    {"wanted_tables", static_cast<int>(wanted.size())},
                },
            });
        }
        loop_exited_early = true;
        return true;
    };

    try {
        while (std::chrono::steady_clock::now() < deadline && events_seen < max_events) {
            relieve_memory_pressure();

            rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, poll_timeout_ms);
            if (!msg) {
                empty_polls += 1;
                if (empty_polls >= empty_poll_quiet) {
                    flush_batch();
                    const bool no_lag = !topics_have_lag(rk, topics, last_offsets, topic_partitions);
                    if (events_seen == 0 && pending_batch.empty() && no_lag) {
                        stop_reason = "idle_no_messages";
                        loop_exited_early = true;
                        break;
                    }
                    const bool allow_quiet = no_lag;
                    if (stop_slice_if_ready(allow_quiet)) {
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
                        const bool no_lag = !topics_have_lag(rk, topics, last_offsets, topic_partitions);
                        if (events_seen == 0 && pending_batch.empty() && no_lag) {
                            stop_reason = "idle_no_messages";
                            loop_exited_early = true;
                            break;
                        }
                        const bool allow_quiet = no_lag;
                        if (stop_slice_if_ready(allow_quiet)) {
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
                continue;
            }
            event.catalog_id = meta_by_key[key].catalog_id;

            seen_by_table[key] = seen_by_table[key] + 1;
            processed_by_table[key] = processed_by_table[key] + 1;

            if (target > 0) {
                const int applied_n = applied_by_table.count(key) ? applied_by_table.at(key) : 0;
                if (applied_n >= target) {
                    continue;
                }
            }

            pending_batch.push_back(std::move(event));
            if (memory_cap_mb > 0) {
                const long long rss = process_rss_mb();
                if (rss >= memory_cap_mb) {
                    flush_batch();
                    trim_process_heap();
                } else if (static_cast<int>(pending_batch.size()) >= batch_size) {
                    flush_batch();
                }
            } else if (static_cast<int>(pending_batch.size()) >= batch_size) {
                flush_batch();
            }

            if (stop_slice_if_ready(false)) {
                break;
            }
        }

        flush_batch();

        if (!loop_exited_early) {
            if (events_seen >= max_events) {
                stop_reason = "max_events";
            } else if (std::chrono::steady_clock::now() >= deadline) {
                stop_reason = "max_seconds";
            } else if (stop_reason == "unknown") {
                stop_reason = "idle_complete";
            }
        }

        const FairnessCounts counts =
            finalize_slice_reporting(wanted, quarantined, seen_by_table, applied_by_table, met, target);
        const long long duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::steady_clock::now() - start)
                                          .count();

        {
            ApplyBatchOptions quiet_options;
            quiet_options.service_tier = service_tier.value_or("");
            quiet_options.apply_staleness_seconds = staleness;
            quiet_options.apply_inactive_seconds = inactive_seconds;
            quiet_options.catchup_tables = catchup_tables;
            quiet_options.host_sampler = &host_sampler;
            std::vector<QuietTableRef> inactive_tables;
            std::vector<QuietTableRef> starving_tables;
            inactive_tables.reserve(wanted.size());
            starving_tables.reserve(wanted.size());
            for (const auto& key : wanted) {
                if (quarantined.count(key)) {
                    continue;
                }
                const int seen_n = seen_by_table.count(key) ? seen_by_table.at(key) : 0;
                const int applied_n = applied_by_table.count(key) ? applied_by_table.at(key) : 0;
                if (applied_n > 0) {
                    continue;
                }
                const auto meta_it = meta_by_key.find(key);
                if (meta_it == meta_by_key.end()) {
                    continue;
                }
                QuietTableRef ref;
                ref.catalog_id = meta_it->second.catalog_id;
                ref.source_schema = meta_it->second.source_schema;
                ref.source_table = meta_it->second.source_table;
                if (seen_n == 0) {
                    inactive_tables.push_back(ref);
                } else {
                    starving_tables.push_back(ref);
                }
            }
            if (!inactive_tables.empty()) {
                record_quiet_table_batch_stats(
                    app_pg.raw,
                    batch_id,
                    conn_id,
                    service_tier.value_or(""),
                    quiet_options,
                    inactive_tables,
                    0,
                    false,
                    true);
            }
            for (const auto& ref : starving_tables) {
                TableKey lake_key{ref.source_schema, ref.source_table};
                for (const auto& [k, meta] : meta_by_key) {
                    if (meta.source_schema == ref.source_schema && meta.source_table == ref.source_table) {
                        lake_key = k;
                        break;
                    }
                }
                const int seen_n = seen_by_table.count(lake_key) ? seen_by_table.at(lake_key) : 0;
                record_quiet_table_batch_stats(
                    app_pg.raw,
                    batch_id,
                    conn_id,
                    service_tier.value_or(""),
                    quiet_options,
                    {ref},
                    seen_n,
                    true,
                    false);
            }
        }

        insert_fairness_metrics(
            app_pg.raw,
            batch_id,
            conn_id,
            service_tier,
            stop_reason,
            counts,
            events_seen,
            events_applied,
            duration_ms,
            static_cast<int>(wanted.size()));

        stats["events_seen"] = events_seen;
        stats["events_applied"] = events_applied;
        stats["parse_skipped"] = parse_skipped;
        stats["errors"] = errors;
        stats["stop_reason"] = stop_reason;
        stats["duration_ms"] = duration_ms;
        if (memory_cap_mb > 0) {
            stats["memory_cap_mb"] = memory_cap_mb;
            stats["memory_backpressure_waits"] = memory_backpressure_waits;
            stats["process_rss_mb"] = process_rss_mb();
        }

        log_write(log_pg, {
            .level = (errors > 0 || parse_skipped > 0) ? LogLevel::Warning : LogLevel::Info,
            .component = "cdc_kafka_apply_cpp",
            .message = errors > 0 ? "kafka-apply completed with errors"
                : (parse_skipped > 0 ? "kafka-apply completed with parse skips" : "kafka-apply completed"),
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"stop_reason", stop_reason},
                {"events_seen", events_seen},
                {"events_applied", events_applied},
                {"parse_skipped", parse_skipped},
                {"tables_starved", counts.tables_starved},
                {"duration_ms", duration_ms},
                {"errors", errors},
            },
        });
    } catch (const std::exception& ex) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_apply_cpp",
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
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 1;
    }

    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
    std::cout << stats.dump() << std::endl;
    return errors > 0 ? 1 : 0;
}

#endif  // HAVE_RDKAFKA
