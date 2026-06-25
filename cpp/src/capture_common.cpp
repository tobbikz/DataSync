#include "capture_common.hpp"

#include "pipeline_defaults.hpp"

#include <atomic>
#include <cstdlib>

std::atomic<bool> g_shutdown{false};

#ifdef HAVE_FREETDS
#include "mssql_kafka_capture.hpp"
#endif
#ifdef HAVE_MONGOC
#include "mongo_kafka_capture.hpp"
#endif
#include "kafka_topics.hpp"
#include "cdc_envelope.hpp"
#include "mariadb_binlog.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"

#include <algorithm>
#include <sstream>
#include <chrono>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>
#include <stdexcept>

#ifdef HAVE_RDKAFKA
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-qualifiers"
#include <librdkafka/rdkafka.h>
#pragma GCC diagnostic pop

namespace {

bool kafka_watermark_err_is_transient(rd_kafka_resp_err_t err) {
    switch (err) {
    case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
    case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
    case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
        return true;
    default:
        return false;
    }
}

rd_kafka_resp_err_t query_kafka_watermark_offsets_retry(
    rd_kafka_t* rk,
    const char* topic,
    int partition,
    int64_t* low,
    int64_t* high,
    int timeout_ms,
    int max_attempts) {
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__FAIL;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        err = rd_kafka_query_watermark_offsets(rk, topic, partition, low, high, timeout_ms);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR || err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
            return err;
        }
        if (!kafka_watermark_err_is_transient(err) || attempt + 1 >= max_attempts) {
            return err;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250 * (attempt + 1)));
    }
    return err;
}

}  // namespace

#endif

KafkaBootstrapResolved resolve_kafka_bootstrap() {
    KafkaBootstrapResolved out;
    if (const char* env = std::getenv("KAFKA_BOOTSTRAP")) {
        const std::string trimmed(env);
        if (!trimmed.empty()) {
            out.bootstrap = trimmed;
            out.source = "env";
            return out;
        }
    }
    out.bootstrap = std::string(pipeline_defaults::kKafkaBootstrapDefault);
    out.source = "default";
    return out;
}

std::string kafka_apply_consumer_group(
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    bool hot_path) {
    std::string group(pipeline_defaults::kKafkaConsumerGroupPrefix);
    group += '-';
    if (hot_path) {
        group += "hot-";
    }
    group += conn_id;
    if (worker_count > 1) {
        group += "-w";
        group += std::to_string(worker_id);
    }
    return group;
}

namespace {

int capture_slice_max_seconds(const CdcConfig* cdc) {
    return (cdc && cdc->slice_max_seconds > 0) ? cdc->slice_max_seconds : 15;
}

int capture_slice_max_events(const CdcConfig* cdc) {
    if (cdc && cdc->slice_max_events > 0) {
        return static_cast<int>(cdc->slice_max_events);
    }
    return pipeline_defaults::kCaptureMaxEventsDefault;
}

}  // namespace

CaptureRuntimeConfig load_mariadb_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    (void)pg;
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = capture_slice_max_seconds(cdc);
    cfg.max_events = capture_slice_max_events(cdc);
    cfg.topic_prefix = topic_prefix_for_conn(conn_id);
    cfg.topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    cfg.topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    cfg.bootstrap = resolve_kafka_bootstrap().bootstrap;
    cfg.linger_ms = pipeline_defaults::kCaptureProducerLingerMs;
    cfg.producer_batch = pipeline_defaults::kCaptureProducerBatchSize;
    cfg.producer_queue_max_messages = pipeline_defaults::kCaptureProducerQueueMaxMessages;
    cfg.producer_queue_max_kbytes = pipeline_defaults::kCaptureProducerQueueMaxKbytes;
    cfg.topic_partitions = runtime.get_int(
        "kafka_topic_partitions",
        pipeline_defaults::kKafkaTopicPartitions,
        "cdc_kafka_capture",
        conn_id);
    cfg.idle_poll_seconds = pipeline_defaults::kCaptureIdlePollSeconds;
    cfg.quiet_exit_lagging_chunks = pipeline_defaults::kCaptureQuietExitLaggingChunks;
    cfg.heartbeat_seconds = pipeline_defaults::kCaptureHeartbeatSeconds;
    return cfg;
}

CaptureRuntimeConfig load_mssql_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    (void)pg;
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = capture_slice_max_seconds(cdc);
    cfg.max_events = capture_slice_max_events(cdc);
    cfg.topic_prefix = topic_prefix_for_conn(conn_id);
    cfg.topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    cfg.topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    cfg.bootstrap = resolve_kafka_bootstrap().bootstrap;
    cfg.linger_ms = pipeline_defaults::kCaptureProducerLingerMs;
    cfg.producer_batch = pipeline_defaults::kCaptureProducerBatchSize;
    cfg.topic_partitions = runtime.get_int(
        "kafka_topic_partitions",
        pipeline_defaults::kKafkaTopicPartitions,
        "cdc_kafka_capture",
        conn_id);
    cfg.idle_poll_seconds = pipeline_defaults::kCaptureIdlePollSeconds;
    cfg.heartbeat_seconds = pipeline_defaults::kCaptureHeartbeatSeconds;
    cfg.mssql_replay_on_idle = pipeline_defaults::kMssqlCaptureReplayOnIdle;
    return cfg;
}

CaptureRuntimeConfig load_mongo_capture_runtime(
    RuntimeConfig& runtime,
    PGconn* pg,
    const std::string& conn_id,
    const CdcConfig* cdc) {
    (void)pg;
    CaptureRuntimeConfig cfg;
    cfg.max_seconds = capture_slice_max_seconds(cdc);
    cfg.max_events = capture_slice_max_events(cdc);
    cfg.topic_prefix = topic_prefix_for_conn(conn_id);
    cfg.topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    cfg.topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    cfg.bootstrap = resolve_kafka_bootstrap().bootstrap;
    cfg.linger_ms = pipeline_defaults::kCaptureProducerLingerMs;
    cfg.producer_batch = pipeline_defaults::kCaptureProducerBatchSize;
    cfg.topic_partitions = runtime.get_int(
        "kafka_topic_partitions",
        pipeline_defaults::kKafkaTopicPartitions,
        "cdc_kafka_capture",
        conn_id);
    cfg.idle_poll_seconds = pipeline_defaults::kCaptureIdlePollSeconds;
    cfg.heartbeat_seconds = pipeline_defaults::kCaptureHeartbeatSeconds;
    return cfg;
}

void bump_mariadb_capture_heartbeat(MYSQL* mysql) {
    if (!mysql) {
        return;
    }
    (void)mysql_query(mysql, "UPDATE cdc_meta.heartbeat SET note = 'idle' WHERE id = 1");
}

void bump_capture_heartbeat_pg(PGconn* pg, const std::string& conn_id, const std::string& db_engine) {
    if (!pg || conn_id.empty()) {
        return;
    }
    if (db_engine == "mssql") {
        const char* vals[] = {conn_id.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            UPDATE cdc_catalog.cdc_mssql_lsn
            SET updated_at = now()
            WHERE conn_id = $1
            )",
            1,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        if (res) {
            PQclear(res);
        }
        return;
    }
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.capture_position
        SET last_event_ts = now(), updated_at = now()
        WHERE conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void touch_capture_position_slice(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.capture_position
        SET updated_at = now()
        WHERE conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void mark_capture_position_failed(PGconn* pg, const std::string& conn_id, const std::string& error) {
    const std::string err = error.size() > 2000 ? error.substr(0, 2000) : error;
    const char* vals[] = {conn_id.c_str(), err.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.capture_position
        SET status = 'failed'::cdc_catalog.cdc_health_status,
            last_error = $2,
            updated_at = now()
        WHERE conn_id = $1
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

std::vector<std::string> split_pk_columns(const std::string& pk_columns) {
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

std::vector<CaptureCatalogTable> fetch_conn_catalog_tables(
    PGconn* pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count,
    const std::string& db_engine,
    CatalogPipeline pipeline,
    CatalogHotTier hot_tier) {
    // Apply: only tables with baseline snapshot in lake.
    // Capture: MariaDB/Mongo may stream binlog during full load (capture_during_full_load).
    // MSSQL fn_cdc_get_all_changes scans LSN ranges — only capture tables that finished initial COPY.
    std::string load_filter;
    if (pipeline == CatalogPipeline::Capture) {
        if (db_engine == "mssql") {
            load_filter = "(NOT needs_full_load)";
        } else {
            load_filter = "(NOT needs_full_load OR capture_during_full_load = true)";
        }
    } else {
        load_filter = "needs_full_load = false";
    }

    std::ostringstream sql;
    std::vector<const char*> vals;
    vals.push_back(conn_id.c_str());
    int param_count = 1;
    std::string worker_count_str;
    std::string worker_id_str;

    if (db_engine == "mssql") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns, engine_meta::text, hot
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mssql'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND )" << load_filter << R"(
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else if (db_engine == "mongodb") {
        sql << R"(
            SELECT catalog_id, conn_id, source_database, source_schema, source_table, pk_columns, '{}'::text, hot
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mongodb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND )" << load_filter << R"(
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    } else {
        sql << R"(
            SELECT catalog_id, conn_id, '' AS source_database, source_schema, source_table, pk_columns, '{}'::text, hot
            FROM cdc_catalog.catalog
            WHERE db_engine = 'mariadb'
              AND conn_id = $1
              AND active = true
              AND cdc_enabled = true
              AND )" << load_filter << R"(
              AND has_pk = true
              AND status NOT IN ('skipped', 'disabled')
        )";
    }

    if (hot_tier == CatalogHotTier::ColdOnly) {
        sql << " AND hot = false";
    } else if (hot_tier == CatalogHotTier::HotOnly) {
        sql << " AND hot = true";
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

    std::vector<CaptureCatalogTable> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            log_write(pg, {
                .level = LogLevel::Error,
                .component = pipeline == CatalogPipeline::Capture ? "cdc_kafka_capture" : "cdc_kafka_apply_cpp",
                .message = "fetch_conn_catalog_tables query failed",
                .batch_id = std::nullopt,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"db_engine", db_engine},
                    {"pipeline", pipeline == CatalogPipeline::Capture ? "capture" : "apply"},
                    {"error", PQresultErrorMessage(res)},
                },
            });
            PQclear(res);
        }
        return out;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        CaptureCatalogTable row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.source_database = PQgetvalue(res, i, 2);
        row.source_schema = PQgetvalue(res, i, 3);
        row.source_table = PQgetvalue(res, i, 4);
        row.pk_columns = PQgetvalue(res, i, 5);
        const char* meta_txt = PQgetvalue(res, i, 6);
        if (meta_txt && *meta_txt) {
            try {
                row.engine_meta = nlohmann::json::parse(meta_txt);
            } catch (...) {
                row.engine_meta = nlohmann::json::object();
            }
        }
        const char* hot_val = PQgetvalue(res, i, 7);
        row.hot = hot_val && (hot_val[0] == 't' || hot_val[0] == 'T' || hot_val[0] == '1');
        if (db_engine == "mssql") {
            row.lake_schema = mssql_pg_schema_name(row.source_database, row.source_schema);
            row.lake_table = mssql_pg_table_name(row.source_table);
        } else if (db_engine == "mongodb") {
            row.lake_schema = mongo_pg_schema_name(row.source_database);
            row.lake_table = mongo_pg_table_name(row.source_table);
        } else {
            row.lake_schema = row.source_schema;
            row.lake_table = row.source_table;
        }
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

namespace {

bool engine_meta_has_stream_bookmark(const std::string& engine_meta_text) {
    if (engine_meta_text.empty()) {
        return false;
    }
    try {
        const auto meta = nlohmann::json::parse(engine_meta_text);
        return meta.contains("stream_kafka_offsets") && meta["stream_kafka_offsets"].is_object() &&
               !meta["stream_kafka_offsets"].empty();
    } catch (...) {
        return false;
    }
}

int topic_partition_count(const struct rd_kafka_metadata* metadata, const std::string& topic) {
    if (!metadata) {
        return -1;
    }
    for (int i = 0; i < metadata->topic_cnt; ++i) {
        const rd_kafka_metadata_topic_t& meta_topic = metadata->topics[i];
        if (!meta_topic.topic || topic != meta_topic.topic) {
            continue;
        }
        if (meta_topic.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            return -1;
        }
        return meta_topic.partition_cnt;
    }
    return -1;
}

void ensure_kafka_topic_partition_count(
    rd_kafka_t* rk,
    const std::vector<std::string>& topics,
    int target_partitions) {
    if (!rk || topics.empty() || target_partitions <= 0) {
        return;
    }

    const struct rd_kafka_metadata* metadata = nullptr;
    const rd_kafka_resp_err_t md_err = rd_kafka_metadata(rk, 1, nullptr, &metadata, 30000);
    if (md_err != RD_KAFKA_RESP_ERR_NO_ERROR || !metadata) {
        if (metadata) {
            rd_kafka_metadata_destroy(metadata);
        }
        throw std::runtime_error(
            std::string("ensure_kafka_topic_partition_count: metadata failed: ") + rd_kafka_err2str(md_err));
    }

    char errstr[512]{0};
    std::vector<rd_kafka_NewPartitions_t*> increases;
    increases.reserve(topics.size());
    for (const auto& topic : topics) {
        const int current = topic_partition_count(metadata, topic);
        if (current < 0 || current >= target_partitions) {
            continue;
        }
        rd_kafka_NewPartitions_t* np = rd_kafka_NewPartitions_new(
            topic.c_str(), static_cast<size_t>(target_partitions), errstr, sizeof(errstr));
        if (!np) {
            rd_kafka_metadata_destroy(metadata);
            for (auto* entry : increases) {
                rd_kafka_NewPartitions_destroy(entry);
            }
            throw std::runtime_error(
                std::string("NewPartitions failed for ") + topic + ": " + errstr);
        }
        increases.push_back(np);
    }
    rd_kafka_metadata_destroy(metadata);

    if (increases.empty()) {
        return;
    }

    rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATEPARTITIONS);
    rd_kafka_AdminOptions_set_request_timeout(options, 60000, errstr, sizeof(errstr));
    rd_kafka_AdminOptions_set_operation_timeout(options, 120000, errstr, sizeof(errstr));

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_CreatePartitions(rk, increases.data(), increases.size(), options, queue);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_NewPartitions_destroy_array(increases.data(), increases.size());

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 120000);
    rd_kafka_queue_destroy(queue);
    if (!event) {
        throw std::runtime_error("ensure_kafka_topic_partition_count: CreatePartitions timed out");
    }

    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error("ensure_kafka_topic_partition_count: " + msg);
    }

    const rd_kafka_CreatePartitions_result_t* result = rd_kafka_event_CreatePartitions_result(event);
    if (result) {
        size_t res_cnt = 0;
        const rd_kafka_topic_result_t** res = rd_kafka_CreatePartitions_result_topics(result, &res_cnt);
        for (size_t i = 0; i < res_cnt; ++i) {
            const rd_kafka_resp_err_t code = rd_kafka_topic_result_error(res[i]);
            if (code == RD_KAFKA_RESP_ERR_NO_ERROR) {
                continue;
            }
            const char* topic_name = rd_kafka_topic_result_name(res[i]);
            const char* msg = rd_kafka_topic_result_error_string(res[i]);
            rd_kafka_event_destroy(event);
            throw std::runtime_error(
                std::string("ensure_kafka_topic_partition_count: topic ") + (topic_name ? topic_name : "?") + ": " +
                (msg ? msg : rd_kafka_err2str(code)));
        }
    }
    rd_kafka_event_destroy(event);
}

}  // namespace

void ensure_apply_positions_for_conn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine);

bool seed_stream_capture_bookmark_if_needed(
    PGconn* pg,
    const std::string& conn_id,
    long long catalog_id,
    const std::string& db_engine,
    const std::string& batch_id) {
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT source_database, source_schema, source_table,
               capture_during_full_load, engine_meta::text, hot
        FROM cdc_catalog.catalog
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return false;
    }
    const char* sd = PQgetvalue(res, 0, 0);
    const char* ss = PQgetvalue(res, 0, 1);
    const char* st = PQgetvalue(res, 0, 2);
    const char* st4 = PQgetvalue(res, 0, 3);
    const char* emt = PQgetvalue(res, 0, 4);
    const char* hot_raw = PQgetvalue(res, 0, 5);
    const std::string source_database = sd ? sd : "";
    const std::string source_schema = ss ? ss : "";
    const std::string source_table = st ? st : "";
    const bool streaming = st4 ? (std::string(st4) == "t") : false;
    const bool table_hot = hot_raw && (hot_raw[0] == 't' || hot_raw[0] == 'T' || hot_raw[0] == '1');
    const std::string engine_meta_text = emt ? emt : "";
    PQclear(res);
    if (!streaming) {
        return false;
    }
    if (engine_meta_has_stream_bookmark(engine_meta_text)) {
        return true;
    }

    ensure_apply_positions_for_conn(pg, conn_id, db_engine);

    std::string lake_schema;
    std::string lake_table;
    if (db_engine == "mssql") {
        lake_schema = mssql_pg_schema_name(source_database, source_schema);
        lake_table = mssql_pg_table_name(source_table);
    } else if (db_engine == "mongodb") {
        lake_schema = mongo_pg_schema_name(source_database);
        lake_table = mongo_pg_table_name(source_table);
    } else {
        lake_schema = source_schema;
        lake_table = source_table;
    }

    const std::string topic_prefix = topic_prefix_for_conn(conn_id);
    const std::string topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    const int topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    const std::string topic = topic_for_catalog_table(
        topic_prefix, lake_schema, lake_table, topic_mode, topic_buckets, table_hot);
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();

    nlohmann::json partition_offsets = nlohmann::json::object();
#ifdef HAVE_RDKAFKA
    const int topic_partitions = pipeline_defaults::kKafkaTopicPartitions;

    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", kafka.bootstrap.c_str(), errstr, sizeof(errstr));
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "stream capture bookmark skipped: kafka client failed",
            .batch_id = batch_id.empty() ? std::nullopt : std::make_optional(batch_id),
            .conn_id = conn_id,
            .source_schema = source_schema,
            .source_table = source_table,
            .context = {{"error", errstr}, {"topic", topic}},
        });
        rd_kafka_conf_destroy(conf);
        return false;
    }

    nlohmann::json topic_offsets = nlohmann::json::object();
    for (int partition = 0; partition < topic_partitions; ++partition) {
        int64_t low = 0;
        int64_t high = 0;
        const rd_kafka_resp_err_t err = query_kafka_watermark_offsets_retry(
            rk, topic.c_str(), partition, &low, &high, 15000, 12);
        if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
            continue;
        }
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            rd_kafka_destroy(rk);
            log_write(pg, {
                .level = LogLevel::Warning,
                .component = "cdc_catalog_onboard",
                .message = "stream capture bookmark skipped: watermark query failed",
                .batch_id = batch_id.empty() ? std::nullopt : std::make_optional(batch_id),
                .conn_id = conn_id,
                .source_schema = source_schema,
                .source_table = source_table,
                .context = {
                    {"topic", topic},
                    {"partition", partition},
                    {"error", rd_kafka_err2str(err)},
                },
            });
            return false;
        }
        topic_offsets[std::to_string(partition)] = high;
    }
    rd_kafka_destroy(rk);
    if (!topic_offsets.empty()) {
        partition_offsets[topic] = std::move(topic_offsets);
    }
#else
    (void)topic;
    log_write(pg, {
        .level = LogLevel::Warning,
        .component = "cdc_catalog_onboard",
        .message = "stream capture bookmark skipped: built without HAVE_RDKAFKA",
        .batch_id = batch_id.empty() ? std::nullopt : std::make_optional(batch_id),
        .conn_id = conn_id,
        .source_schema = source_schema,
        .source_table = source_table,
    });
    return false;
#endif

    if (partition_offsets.empty()) {
        return false;
    }

    nlohmann::json patch = {
        {"stream_kafka_offsets", partition_offsets},
        {"stream_bookmarked_at", utc_iso_timestamp_now()},
    };
    const std::string patch_text = patch.dump();
    const char* upd_vals[] = {patch_text.c_str(), cid.c_str()};
    PGresult* upd = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET engine_meta = engine_meta || $1::jsonb,
            updated_at = now()
        WHERE catalog_id = $2::bigint
        )",
        2,
        nullptr,
        upd_vals,
        nullptr,
        nullptr,
        0);
    if (!upd || PQresultStatus(upd) != PGRES_COMMAND_OK) {
        if (upd) {
            PQclear(upd);
        }
        return false;
    }
    PQclear(upd);

    log_write(pg, {
        .level = LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = "stream capture bookmark seeded before full load",
        .batch_id = batch_id.empty() ? std::nullopt : std::make_optional(batch_id),
        .conn_id = conn_id,
        .source_schema = source_schema,
        .source_table = source_table,
        .context = {
            {"catalog_id", catalog_id},
            {"topic", topic},
            {"kafka_bootstrap", kafka.bootstrap},
            {"stream_kafka_offsets", partition_offsets},
        },
    });
    return true;
}

namespace {

bool conn_had_capture_baseline(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            EXISTS (
                SELECT 1 FROM cdc_catalog.capture_position
                WHERE conn_id = $1
                  AND binlog_file IS NOT NULL
                  AND length(trim(binlog_file)) > 0
            ) AS has_capture,
            EXISTS (
                SELECT 1 FROM cdc_catalog.catalog
                WHERE conn_id = $1
                  AND last_full_load_at IS NOT NULL
            ) AS has_full_load
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return false;
    }
    const bool ok = std::string(PQgetvalue(res, 0, 0)) == "t" || std::string(PQgetvalue(res, 0, 1)) == "t";
    PQclear(res);
    return ok;
}

}  // namespace

BinlogGapRebootResult reboot_conn_after_mariadb_binlog_gap(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& conn_id,
    const std::string& batch_id) {
    BinlogGapRebootResult out;
    if (!conn_had_capture_baseline(pg, conn_id)) {
        capture_binlog_position_t0(pg, mysql, conn_id);
        out.ran = true;
        out.t0_reset = true;
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_capture",
            .message = "binlog gap: capture T0 seeded (conn not yet baselined)",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return out;
    }

    capture_binlog_position_t0(pg, mysql, conn_id);
    out.t0_reset = true;

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = true,
            cdc_enabled = true,
            status = 'pending',
            last_error = 'binlog purged: auto full-load reboot',
            engine_meta = engine_meta - 'stream_kafka_offsets' - 'stream_bookmarked_at',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = 'mariadb'
          AND active = true
          AND has_pk = true
          AND status NOT IN ('skipped', 'disabled')
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_COMMAND_OK) {
        out.tables_flagged = std::atoi(PQcmdTuples(res));
    }
    if (res) {
        PQclear(res);
    }

    out.ran = true;
    log_write(pg, {
        .level = LogLevel::Warning,
        .component = "cdc_kafka_capture",
        .message = "binlog gap: capture T0 reset and tables flagged for full-load reboot",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"tables_flagged", out.tables_flagged}},
    });
    return out;
}

#ifdef HAVE_FREETDS

namespace {

bool conn_had_mssql_capture_baseline(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            EXISTS (
                SELECT 1 FROM cdc_catalog.cdc_mssql_lsn
                WHERE conn_id = $1
                  AND last_start_lsn IS NOT NULL
            ) AS has_capture,
            EXISTS (
                SELECT 1 FROM cdc_catalog.catalog
                WHERE conn_id = $1
                  AND last_full_load_at IS NOT NULL
            ) AS has_full_load
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return false;
    }
    const bool ok = std::string(PQgetvalue(res, 0, 0)) == "t" || std::string(PQgetvalue(res, 0, 1)) == "t";
    PQclear(res);
    return ok;
}

}  // namespace

BinlogGapRebootResult reboot_conn_after_mssql_cdc_gap(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& batch_id) {
    BinlogGapRebootResult out;
    if (!conn_had_mssql_capture_baseline(pg, conn_id)) {
        seed_mssql_cdc_lsn_for_conn(cfg, pg, conn_id, batch_id);
        out.ran = true;
        out.t0_reset = true;
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_mssql_capture",
            .message = "mssql cdc gap: capture T0 seeded (conn not yet baselined)",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return out;
    }

    seed_mssql_cdc_lsn_for_conn(cfg, pg, conn_id, batch_id);
    out.t0_reset = true;

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = true,
            cdc_enabled = true,
            status = 'pending',
            last_error = 'mssql cdc lsn purged: auto full-load reboot',
            engine_meta = engine_meta - 'stream_kafka_offsets' - 'stream_bookmarked_at',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = 'mssql'
          AND active = true
          AND has_pk = true
          AND status NOT IN ('skipped', 'disabled')
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_COMMAND_OK) {
        out.tables_flagged = std::atoi(PQcmdTuples(res));
    }
    if (res) {
        PQclear(res);
    }

    out.ran = true;
    log_write(pg, {
        .level = LogLevel::Warning,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql cdc gap: capture T0 reset and tables flagged for full-load reboot",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"tables_flagged", out.tables_flagged}},
    });
    return out;
}

#else

BinlogGapRebootResult reboot_conn_after_mssql_cdc_gap(
    const AppConfig&,
    PGconn*,
    const std::string&,
    const std::string&) {
    return {};
}

#endif

void enable_cdc_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id,
    bool expect_updates) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET cdc_enabled = true,
            status = 'success',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND has_pk = true
          AND needs_full_load = false
          AND NOT cdc_enabled
          AND status = 'full_load_in_progress'
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const int count = (res && PQresultStatus(res) == PGRES_COMMAND_OK) ? std::atoi(PQcmdTuples(res)) : 0;
    if (res) {
        PQclear(res);
    }

    log_write(pg, {
        .level = (expect_updates && count == 0) ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = (expect_updates && count == 0) ? "enable cdc after full-load: no rows updated"
                                                   : "enabled cdc for loaded tables",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"count", count},
            {"db_engine", db_engine},
            {"expect_updates", expect_updates},
        },
    });
}

bool enable_cdc_after_full_load_table(
    PGconn* pg,
    long long catalog_id,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& source_schema,
    const std::string& source_table) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET cdc_enabled = true,
            status = 'success',
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND active = true
          AND has_pk = true
          AND needs_full_load = false
          AND NOT cdc_enabled
          AND status = 'full_load_in_progress'
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const int count = (res && PQresultStatus(res) == PGRES_COMMAND_OK) ? std::atoi(PQcmdTuples(res)) : 0;
    if (res) {
        PQclear(res);
    }

    log_write(pg, {
        .level = count == 0 ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = count == 0 ? "enable cdc after table full-load: no row updated"
                              : "enabled cdc after table full-load",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = source_schema,
        .source_table = source_table,
        .context = {{"catalog_id", catalog_id}, {"count", count}},
    });
    return count > 0;
}

void mark_catalog_full_load_in_progress(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'full_load_in_progress',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_full_load_data_ready(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = false,
            cdc_enabled = false,
            status = 'full_load_in_progress',
            last_full_load_at = now(),
            last_error_at = NULL,
            last_error = NULL,
            engine_meta = COALESCE(engine_meta, '{}'::jsonb) - 'full_load_fail_count',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_skipped(PGconn* pg, long long catalog_id, const std::string& reason) {
    const std::string id = std::to_string(catalog_id);
    const std::string trunc = reason.substr(0, 1000);
    const char* vals[] = {id.c_str(), trunc.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'skipped',
            needs_full_load = false,
            cdc_enabled = false,
            last_error = $2,
            last_error_at = now(),
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        vals);
}

void mark_catalog_cdc_in_progress(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'cdc_in_progress',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

namespace {

bool catalog_needs_full_load(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* sel = PQexecParams(
        pg,
        R"(
        SELECT needs_full_load
        FROM cdc_catalog.catalog
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    bool needs_full_load = false;
    if (sel && PQresultStatus(sel) == PGRES_TUPLES_OK && PQntuples(sel) > 0) {
        needs_full_load = std::string(PQgetvalue(sel, 0, 0)) == "t";
    }
    if (sel) {
        PQclear(sel);
    }
    return needs_full_load;
}

}  // namespace

void mark_catalog_cdc_success(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    const bool needs_full_load = catalog_needs_full_load(pg, catalog_id);
    if (needs_full_load) {
        pg_exec_params_simple(
            pg,
            R"(
            UPDATE cdc_catalog.catalog
            SET last_cdc_at = now(),
                status = CASE
                    WHEN status = 'success'::cdc_catalog.replication_status
                    THEN 'pending'::cdc_catalog.replication_status
                    ELSE status
                END,
                updated_at = now()
            WHERE catalog_id = $1::bigint
            )",
            1,
            vals);
        return;
    }
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET last_cdc_at = now(),
            status = 'success',
            last_error_at = NULL,
            last_error = NULL,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        vals);
}

void mark_catalog_cdc_failed(PGconn* pg, long long catalog_id, const std::string& error) {
    const std::string id = std::to_string(catalog_id);
    const std::string trunc = error.substr(0, 1000);
    const char* vals[] = {id.c_str(), trunc.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'failed',
            last_error = $2,
            last_error_at = now(),
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        vals);
}

void mark_catalog_reconcile_failed(
    PGconn* pg,
    long long catalog_id,
    const std::string& error,
    bool needs_full_load) {
    const std::string id = std::to_string(catalog_id);
    const std::string trunc = error.substr(0, 1000);
    const char* needs_fl = needs_full_load ? "true" : "false";
    const char* vals[] = {id.c_str(), trunc.c_str(), needs_fl};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'failed',
            last_error = $2,
            last_error_at = now(),
            needs_full_load = $3::boolean,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        3,
        vals);
}

void mark_catalog_reconcile_healed(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    if (catalog_needs_full_load(pg, catalog_id)) {
        return;
    }
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'success',
            last_error = NULL,
            last_error_at = NULL,
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND last_error LIKE 'reconcile:%'
          AND NOT needs_full_load
        )",
        1,
        vals);
}

void clear_stale_full_load_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    int stale_minutes) {
    const std::string mins = std::to_string(std::max(1, stale_minutes));
    const char* vals[] = {conn_id.c_str(), db_engine.c_str(), mins.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'pending',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND status = 'full_load_in_progress'
          AND needs_full_load = true
          AND updated_at < now() - make_interval(mins => $3::int)
        )",
        3,
        vals);
}

void reset_full_load_in_progress_for_conn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = 'pending',
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND status = 'full_load_in_progress'
          AND needs_full_load = true
        )",
        2,
        vals);
}

void rollback_cdc_in_progress_ids(PGconn* pg, const std::set<long long>& catalog_ids) {
    for (long long catalog_id : catalog_ids) {
        if (catalog_id <= 0) {
            continue;
        }
        const std::string id = std::to_string(catalog_id);
        const char* vals[] = {id.c_str()};
        pg_exec_params_simple(
            pg,
            R"(
            UPDATE cdc_catalog.catalog
            SET status = CASE
                WHEN needs_full_load THEN 'pending'::cdc_catalog.replication_status
                ELSE 'success'::cdc_catalog.replication_status
            END,
                updated_at = now()
            WHERE catalog_id = $1::bigint
              AND status = 'cdc_in_progress'
            )",
            1,
            vals);
    }
}

int clear_stale_cdc_in_progress(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET status = CASE
            WHEN needs_full_load THEN 'pending'::cdc_catalog.replication_status
            ELSE 'success'::cdc_catalog.replication_status
        END,
            updated_at = now()
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND status = 'cdc_in_progress'
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res) {
        throw std::runtime_error(std::string("clear stale cdc_in_progress failed: ") + PQerrorMessage(pg));
    }
    const auto st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK) {
        const std::string err = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error(std::string("clear stale cdc_in_progress failed: ") + err);
    }
    const int cleared = std::atoi(PQcmdTuples(res));
    PQclear(res);
    return cleared;
}

int count_full_load_pending(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = true
          AND status NOT IN ('skipped', 'disabled')
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

int count_full_load_pending_onboard(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND needs_full_load = false
          AND NOT cdc_enabled
          AND status = 'full_load_in_progress'
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        count = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

ApplySkipReasonCounts fetch_apply_skip_reasons(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    ApplySkipReasonCounts out;
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            count(*)::int AS active_total,
            count(*) FILTER (WHERE needs_full_load)::int AS needs_full_load,
            count(*) FILTER (WHERE NOT cdc_enabled)::int AS cdc_disabled,
            count(*) FILTER (WHERE NOT has_pk)::int AS no_pk,
            count(*) FILTER (WHERE status IN ('skipped', 'disabled'))::int AS skipped_status,
            count(*) FILTER (
                WHERE cdc_enabled
                  AND NOT needs_full_load
                  AND has_pk
                  AND status NOT IN ('skipped', 'disabled')
            )::int AS apply_ready
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out.active_total = std::atoi(PQgetvalue(res, 0, 0));
        out.needs_full_load = std::atoi(PQgetvalue(res, 0, 1));
        out.cdc_disabled = std::atoi(PQgetvalue(res, 0, 2));
        out.no_pk = std::atoi(PQgetvalue(res, 0, 3));
        out.skipped_status = std::atoi(PQgetvalue(res, 0, 4));
        out.apply_ready = std::atoi(PQgetvalue(res, 0, 5));
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

void log_cdc_skip_no_tables(
    PGconn* pg,
    const std::string& component,
    const std::string& pipeline,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& db_engine) {
    const ApplySkipReasonCounts reasons = fetch_apply_skip_reasons(pg, conn_id, db_engine);

    nlohmann::json catalog_sample = nlohmann::json::array();
    const char* sample_vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* sample_res = PQexecParams(
        pg,
        R"(
        SELECT source_schema, source_table, active, cdc_enabled, needs_full_load, has_pk,
               status::text AS status,
               (active AND cdc_enabled AND NOT needs_full_load AND has_pk
                AND status NOT IN ('skipped', 'disabled')) AS capture_ready
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
        ORDER BY capture_ready DESC, updated_at DESC
        LIMIT 8
        )",
        2,
        nullptr,
        sample_vals,
        nullptr,
        nullptr,
        0);
    if (sample_res && PQresultStatus(sample_res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(sample_res); ++i) {
            catalog_sample.push_back({
                {"schema", PQgetvalue(sample_res, i, 0)},
                {"table", PQgetvalue(sample_res, i, 1)},
                {"active", PQgetvalue(sample_res, i, 2)},
                {"cdc_enabled", PQgetvalue(sample_res, i, 3)},
                {"needs_full_load", PQgetvalue(sample_res, i, 4)},
                {"has_pk", PQgetvalue(sample_res, i, 5)},
                {"status", PQgetvalue(sample_res, i, 6)},
                {"capture_ready", PQgetvalue(sample_res, i, 7)},
            });
        }
    }
    if (sample_res) {
        PQclear(sample_res);
    }

    std::string hint = "capture requires active=true, cdc_enabled=true, needs_full_load=false, has_pk=true, status not skipped/disabled";
    if (reasons.active_total == 0 && !catalog_sample.empty()) {
        const bool any_active = std::any_of(
            catalog_sample.begin(),
            catalog_sample.end(),
            [](const nlohmann::json& row) {
                const auto it = row.find("active");
                return it != row.end() && it->is_string() && *it == "t";
            });
        if (!any_active) {
            hint = "rows exist but active=false on all — run: UPDATE cdc_catalog.catalog SET active=true WHERE ...";
        }
    } else if (reasons.apply_ready == 0 && reasons.needs_full_load > 0) {
        hint = "needs_full_load still true — run full-load or UPDATE needs_full_load=false after load";
    } else if (reasons.apply_ready == 0 && reasons.cdc_disabled > 0) {
        hint = "cdc_enabled=false — enable CDC after full-load completes";
    }

    log_write(pg, {
        .level = LogLevel::Info,
        .component = component,
        .message = pipeline + " skipped: no tables",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"pipeline", pipeline},
            {"db_engine", db_engine},
            {"hint", hint},
            {"active_total", reasons.active_total},
            {"needs_full_load", reasons.needs_full_load},
            {"cdc_disabled", reasons.cdc_disabled},
            {"no_pk", reasons.no_pk},
            {"skipped_status", reasons.skipped_status},
            {"capture_ready", reasons.apply_ready},
            {"catalog_sample", catalog_sample},
        },
    });
}

bool upsert_apply_position(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& source_schema,
    const std::string& source_table,
    const std::string& kafka_topic,
    std::string* error_out) {
    const std::string catalog_id_str = std::to_string(catalog_id);
    const char* del_vals[] = {
        catalog_id_str.c_str(),
        conn_id.c_str(),
        source_schema.c_str(),
        source_table.c_str(),
    };
    pg_exec_params_simple(
        pg,
        R"(
        DELETE FROM cdc_catalog.apply_position
        WHERE catalog_id = $1::bigint
           OR (conn_id = $2 AND source_schema = $3 AND source_table = $4 AND catalog_id <> $1::bigint)
        )",
        4,
        del_vals);

    const char* ins_vals[] = {
        catalog_id_str.c_str(),
        conn_id.c_str(),
        source_schema.c_str(),
        source_table.c_str(),
        kafka_topic.c_str(),
    };
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.apply_position
            (catalog_id, conn_id, source_schema, source_table, kafka_topic, status)
        VALUES ($1::bigint, $2, $3, $4, $5, 'healthy')
        )",
        5,
        nullptr,
        ins_vals,
        nullptr,
        nullptr,
        0);
    if (!res) {
        if (error_out) {
            *error_out = "PQexecParams returned null";
        }
        return false;
    }
    const auto st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK) {
        if (error_out) {
            std::string err = PQerrorMessage(pg);
            if (const char* msg = PQresultErrorMessage(res)) {
                if (msg[0]) {
                    err += " | ";
                    err += msg;
                }
            }
            *error_out = err;
        }
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

void ensure_apply_positions_for_conn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine) {
    const std::string topic_prefix = topic_prefix_for_conn(conn_id);
    const std::string topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    const int topic_buckets = pipeline_defaults::kKafkaTopicBuckets;

    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT catalog_id, source_database, source_schema, source_table, hot
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = $2::cdc_catalog.db_engine
          AND active = true
          AND has_pk = true
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
        return;
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string catalog_id = PQgetvalue(res, i, 0);
        const std::string source_database = PQgetvalue(res, i, 1);
        const std::string source_schema = PQgetvalue(res, i, 2);
        const std::string source_table = PQgetvalue(res, i, 3);
        const char* hot_raw = PQgetvalue(res, i, 4);
        const bool table_hot = hot_raw && (hot_raw[0] == 't' || hot_raw[0] == 'T' || hot_raw[0] == '1');
        std::string lake_schema;
        std::string lake_table;
        if (db_engine == "mssql") {
            lake_schema = mssql_pg_schema_name(source_database, source_schema);
            lake_table = mssql_pg_table_name(source_table);
        } else if (db_engine == "mongodb") {
            lake_schema = mongo_pg_schema_name(source_database);
            lake_table = mongo_pg_table_name(source_table);
        } else {
            lake_schema = source_schema;
            lake_table = source_table;
        }
        const std::string topic = topic_for_catalog_table(
            topic_prefix, lake_schema, lake_table, topic_mode, topic_buckets, table_hot);
        const std::string pos_schema = (db_engine == "mongodb")
            ? mongo_catalog_source_schema(source_database, source_schema)
            : (db_engine == "mssql" ? source_schema : lake_schema);
        auto mark_ap_failed = [&](const std::string& err_msg) {
            const std::string trunc = err_msg.substr(0, 950);
            const char* fail_vals[] = {catalog_id.c_str(), trunc.c_str()};
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
                std::stoll(catalog_id),
                conn_id,
                pos_schema,
                lake_table,
                topic,
                &ap_err)) {
            mark_ap_failed("apply_position upsert failed: " + ap_err);
        }
    }
    PQclear(res);
}

void ensure_apply_position_for_catalog(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id) {
    const std::string topic_prefix = topic_prefix_for_conn(conn_id);
    const std::string topic_mode = std::string(pipeline_defaults::kKafkaTopicMode);
    const int topic_buckets = pipeline_defaults::kKafkaTopicBuckets;
    const std::string catalog_id_str = std::to_string(catalog_id);

    const char* vals[] = {catalog_id_str.c_str(), conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT catalog_id, source_database, source_schema, source_table, hot
        FROM cdc_catalog.catalog
        WHERE catalog_id = $1::bigint
          AND conn_id = $2
          AND db_engine = $3::cdc_catalog.db_engine
          AND active = true
          AND has_pk = true
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return;
    }

    const std::string row_catalog_id = PQgetvalue(res, 0, 0);
    const std::string source_database = PQgetvalue(res, 0, 1);
    const std::string source_schema = PQgetvalue(res, 0, 2);
    const std::string source_table = PQgetvalue(res, 0, 3);
    const char* hot_raw = PQgetvalue(res, 0, 4);
    const bool table_hot = hot_raw && (hot_raw[0] == 't' || hot_raw[0] == 'T' || hot_raw[0] == '1');
    PQclear(res);

    std::string lake_schema;
    std::string lake_table;
    if (db_engine == "mssql") {
        lake_schema = mssql_pg_schema_name(source_database, source_schema);
        lake_table = mssql_pg_table_name(source_table);
    } else if (db_engine == "mongodb") {
        lake_schema = mongo_pg_schema_name(source_database);
        lake_table = mongo_pg_table_name(source_table);
    } else {
        lake_schema = source_schema;
        lake_table = source_table;
    }
    const std::string topic = topic_for_catalog_table(
        topic_prefix, lake_schema, lake_table, topic_mode, topic_buckets, table_hot);
    const std::string pos_schema = (db_engine == "mongodb")
        ? mongo_catalog_source_schema(source_database, source_schema)
        : (db_engine == "mssql" ? source_schema : lake_schema);
    auto mark_ap_failed = [&](const std::string& err_msg) {
        const std::string trunc = err_msg.substr(0, 950);
        const char* fail_vals[] = {row_catalog_id.c_str(), trunc.c_str()};
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
        if (fail) {
            PQclear(fail);
        }
    };
    std::string ap_err;
    if (!upsert_apply_position(
            pg, catalog_id, conn_id, pos_schema, lake_table, topic, &ap_err)) {
        mark_ap_failed("apply_position upsert failed: " + ap_err);
    }
}

struct OnboardApplyRow {
    long long catalog_id;
    std::string schema;
    std::string table;
    std::string topic;
    int partition;
};

static FullLoadKafkaResetStats execute_kafka_onboard_reset(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id,
    const std::string& bootstrap,
    const std::vector<OnboardApplyRow>& rows,
    const std::optional<long long>& single_catalog_id) {
    FullLoadKafkaResetStats stats;
    stats.tables = static_cast<int>(rows.size());
    if (rows.empty()) {
        return stats;
    }

#ifdef HAVE_RDKAFKA
    const int topic_partitions = pipeline_defaults::kKafkaTopicPartitions;
    const int apply_workers = kApplyWorkerCount;

    std::set<std::string> unique_topic_names;
    for (const auto& row : rows) {
        unique_topic_names.insert(row.topic);
    }

    std::set<std::pair<std::string, int>> unique_topics;
    for (const auto& topic : unique_topic_names) {
        for (int partition = 0; partition < topic_partitions; ++partition) {
            unique_topics.emplace(topic, partition);
        }
    }

    std::map<std::pair<std::string, int>, long long> bookmark_targets;
    std::vector<long long> stream_catalog_ids;
    const char* conn_vals[] = {conn_id.c_str(), db_engine.c_str()};
    const std::string catalog_filter = single_catalog_id
        ? " AND c.catalog_id = " + std::to_string(*single_catalog_id)
        : "";
    const std::string stream_sql =
        R"(
        SELECT c.catalog_id, c.engine_meta::text, ap.kafka_topic
        FROM cdc_catalog.catalog c
        JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.conn_id = $1
          AND c.db_engine = $2::cdc_catalog.db_engine
          AND c.capture_during_full_load = true
          AND NOT c.needs_full_load
          AND c.status = 'full_load_in_progress'
          AND NOT c.cdc_enabled
        )" + catalog_filter;
    PGresult* stream_res = PQexecParams(
        pg,
        stream_sql.c_str(),
        2,
        nullptr,
        conn_vals,
        nullptr,
        nullptr,
        0);
    if (stream_res && PQresultStatus(stream_res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(stream_res); ++i) {
            const long long cid = std::atoll(PQgetvalue(stream_res, i, 0));
            const std::string meta_text = PQgetvalue(stream_res, i, 1);
            const std::string topic = PQgetvalue(stream_res, i, 2);
            stream_catalog_ids.push_back(cid);
            try {
                const auto meta = nlohmann::json::parse(meta_text);
                if (!meta.contains("stream_kafka_offsets") || !meta["stream_kafka_offsets"].is_object()) {
                    continue;
                }
                const auto& topics = meta["stream_kafka_offsets"];
                if (topics.contains(topic) && topics[topic].is_object()) {
                    for (auto it = topics[topic].begin(); it != topics[topic].end(); ++it) {
                        const int partition = std::stoi(it.key());
                        const long long offset = it.value().get<long long>();
                        const auto key = std::make_pair(topic, partition);
                        const auto existing = bookmark_targets.find(key);
                        if (existing == bookmark_targets.end() || offset < existing->second) {
                            bookmark_targets[key] = offset;
                        }
                    }
                }
            } catch (...) {
                stats.errors += 1;
            }
        }
    }
    if (stream_res) {
        PQclear(stream_res);
    }

    std::map<std::pair<std::string, int>, long long> topic_offsets;
    for (const auto& topic_key : unique_topics) {
        const auto& topic = topic_key.first;
        const int partition = topic_key.second;
        const auto bookmark_it = bookmark_targets.find(topic_key);
        const bool use_bookmark = bookmark_it != bookmark_targets.end();
        bool reset_ok = false;
        for (int attempt = 0; attempt < 3; ++attempt) {
            try {
                for (int w = 0; w < apply_workers; ++w) {
                    const std::string consumer_group =
                        kafka_apply_consumer_group(conn_id, w, apply_workers, false);
                    const long long stored_offset =
                        use_bookmark
                            ? reset_apply_consumer_offset(
                                  bootstrap, consumer_group, topic, partition, bookmark_it->second)
                            : reset_apply_offset_to_end(bootstrap, consumer_group, topic, partition);
                    if (w == 0 && stored_offset >= 0) {
                        topic_offsets.emplace(topic_key, stored_offset);
                    }
                }
                for (int w = 0; w < pipeline_defaults::kHotApplyConsumerCount; ++w) {
                    const std::string hot_group = kafka_apply_consumer_group(
                        conn_id, w, pipeline_defaults::kHotApplyConsumerCount, true);
                    if (use_bookmark) {
                        reset_apply_consumer_offset(
                            bootstrap, hot_group, topic, partition, bookmark_it->second);
                    } else {
                        reset_apply_offset_to_end(bootstrap, hot_group, topic, partition);
                    }
                }
                stats.topics_reset += 1;
                reset_ok = true;
                break;
            } catch (const std::exception& ex) {
                if (attempt == 2) {
                    stats.errors += 1;
                    log_write(pg, {
                        .level = LogLevel::Warning,
                        .component = "cdc_catalog_onboard",
                        .message = use_bookmark
                                       ? "stream replay kafka offset reset failed after retries"
                                       : "full-load kafka offset reset failed after retries",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"topic", topic},
                            {"partition", partition},
                            {"use_stream_bookmark", use_bookmark},
                            {"error", ex.what()},
                        },
                    });
                } else {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                }
            }
        }
        if (!reset_ok) {
            log_write(pg, {
                .level = LogLevel::Warning,
                .component = "cdc_catalog_onboard",
                .message = "full-load kafka reset aborted: dedup preserved",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"db_engine", db_engine},
                    {"topics_reset", stats.topics_reset},
                    {"topics_expected", static_cast<int>(unique_topics.size())},
                    {"errors", stats.errors},
                    {"single_table", single_catalog_id.has_value()},
                },
            });
            return stats;
        }
    }

    for (const auto& row : rows) {
        const auto topic_key = std::make_pair(row.topic, row.partition);
        const auto it = topic_offsets.find(topic_key);
        if (it == topic_offsets.end()) {
            stats.errors += 1;
            continue;
        }
        const std::string offset_str = std::to_string(it->second);
        const std::string catalog_id_str = std::to_string(row.catalog_id);
        const char* upd_vals[] = {offset_str.c_str(), catalog_id_str.c_str()};
        PGresult* upd = PQexecParams(
            pg,
            R"(
            UPDATE cdc_catalog.apply_position
            SET kafka_offset = $1::bigint,
                last_applied_at = now(),
                apply_lag_seconds = 0,
                status = 'healthy'::cdc_catalog.cdc_health_status,
                last_error = NULL,
                updated_at = now()
            WHERE catalog_id = $2::bigint
            )",
            2,
            nullptr,
            upd_vals,
            nullptr,
            nullptr,
            0);
        if (!upd || PQresultStatus(upd) != PGRES_COMMAND_OK) {
            stats.errors += 1;
        }
        if (upd) {
            PQclear(upd);
        }
    }

    if (!stream_catalog_ids.empty()) {
        for (long long catalog_id : stream_catalog_ids) {
            const std::string cid = std::to_string(catalog_id);
            const char* clr_vals[] = {cid.c_str()};
            PGresult* clr = PQexecParams(
                pg,
                R"(
                UPDATE cdc_catalog.catalog
                SET capture_during_full_load = false,
                    engine_meta = engine_meta - 'stream_kafka_offsets' - 'stream_bookmarked_at',
                    updated_at = now()
                WHERE catalog_id = $1::bigint
                )",
                1,
                nullptr,
                clr_vals,
                nullptr,
                nullptr,
                0);
            if (!clr || PQresultStatus(clr) != PGRES_COMMAND_OK) {
                stats.errors += 1;
            }
            if (clr) {
                PQclear(clr);
            }
        }
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_catalog_onboard",
            .message = "stream capture replay offsets applied after full load",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"stream_tables", static_cast<int>(stream_catalog_ids.size())},
                {"bookmark_partitions", static_cast<int>(bookmark_targets.size())},
                {"single_table", single_catalog_id.has_value()},
            },
        });
    }
#else
    (void)bootstrap;
#endif

    if (stats.errors > 0) {
        return stats;
    }

    if (single_catalog_id) {
        const std::string cid = std::to_string(*single_catalog_id);
        const char* dedup_vals[] = {conn_id.c_str(), db_engine.c_str(), cid.c_str()};
        PGresult* dedup = PQexecParams(
            pg,
            R"(
            DELETE FROM cdc_catalog.cdc_applied_events ae
            USING cdc_catalog.catalog c
            WHERE ae.conn_id = $1
              AND c.conn_id = ae.conn_id
              AND c.source_schema = ae.source_schema
              AND c.source_table = ae.source_table
              AND c.catalog_id = $3::bigint
              AND c.conn_id = $1
              AND c.db_engine = $2::cdc_catalog.db_engine
              AND c.active = true
              AND c.needs_full_load = false
              AND NOT c.cdc_enabled
              AND c.status = 'full_load_in_progress'
            )",
            3,
            nullptr,
            dedup_vals,
            nullptr,
            nullptr,
            0);
        if (dedup && PQresultStatus(dedup) == PGRES_COMMAND_OK) {
            stats.dedup_deleted = std::atoll(PQcmdTuples(dedup));
        } else {
            stats.errors += 1;
        }
        if (dedup) {
            PQclear(dedup);
        }
    } else {
        const char* dedup_vals[] = {conn_id.c_str(), db_engine.c_str()};
        PGresult* dedup = PQexecParams(
            pg,
            R"(
            DELETE FROM cdc_catalog.cdc_applied_events ae
            USING cdc_catalog.catalog c
            WHERE ae.conn_id = $1
              AND c.conn_id = ae.conn_id
              AND c.source_schema = ae.source_schema
              AND c.source_table = ae.source_table
              AND c.conn_id = $1
              AND c.db_engine = $2::cdc_catalog.db_engine
              AND c.active = true
              AND c.needs_full_load = false
              AND NOT c.cdc_enabled
              AND c.status = 'full_load_in_progress'
            )",
            2,
            nullptr,
            dedup_vals,
            nullptr,
            nullptr,
            0);
        if (dedup && PQresultStatus(dedup) == PGRES_COMMAND_OK) {
            stats.dedup_deleted = std::atoll(PQcmdTuples(dedup));
        } else {
            stats.errors += 1;
        }
        if (dedup) {
            PQclear(dedup);
        }
    }

    log_write(pg, {
        .level = stats.errors > 0 ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = stats.errors > 0 ? "full-load kafka reset completed with errors"
                                     : "full-load kafka reset completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"db_engine", db_engine},
            {"tables", stats.tables},
            {"topics_reset", stats.topics_reset},
            {"dedup_deleted", stats.dedup_deleted},
            {"errors", stats.errors},
            {"single_table", single_catalog_id.has_value()},
        },
    });

    return stats;
}

FullLoadKafkaResetStats reset_kafka_apply_after_full_load_table(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id,
    const std::string& batch_id) {
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    ensure_apply_position_for_catalog(pg, conn_id, db_engine, catalog_id);

    const std::string catalog_id_str = std::to_string(catalog_id);
    const char* vals[] = {conn_id.c_str(), db_engine.c_str(), catalog_id_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT ap.catalog_id, ap.source_schema, ap.source_table, ap.kafka_topic, ap.kafka_partition
        FROM cdc_catalog.apply_position ap
        JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
        WHERE ap.conn_id = $1
          AND c.db_engine = $2::cdc_catalog.db_engine
          AND c.catalog_id = $3::bigint
          AND c.active = true
          AND c.needs_full_load = false
          AND NOT c.cdc_enabled
          AND c.status = 'full_load_in_progress'
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        FullLoadKafkaResetStats stats;
        stats.errors += 1;
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "table full-load kafka reset skipped: apply_position query failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"db_engine", db_engine}, {"catalog_id", catalog_id}},
        });
        return stats;
    }

    std::vector<OnboardApplyRow> rows;
    if (PQntuples(res) > 0) {
        rows.push_back({
            std::atoll(PQgetvalue(res, 0, 0)),
            PQgetvalue(res, 0, 1),
            PQgetvalue(res, 0, 2),
            PQgetvalue(res, 0, 3),
            std::stoi(PQgetvalue(res, 0, 4)),
        });
    }
    PQclear(res);

    if (rows.empty()) {
        FullLoadKafkaResetStats stats;
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_catalog_onboard",
            .message = "table full-load kafka reset skipped: table not pending onboard",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"db_engine", db_engine}, {"catalog_id", catalog_id}},
        });
        return stats;
    }

    return execute_kafka_onboard_reset(
        pg, conn_id, db_engine, batch_id, kafka.bootstrap, rows, catalog_id);
}

bool onboard_table_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    long long catalog_id,
    const std::string& batch_id,
    const std::string& source_schema,
    const std::string& source_table) {
    const auto stats =
        reset_kafka_apply_after_full_load_table(pg, conn_id, db_engine, catalog_id, batch_id);
    const bool enabled = enable_cdc_after_full_load_table(
        pg, catalog_id, batch_id, conn_id, source_schema, source_table);
    if (stats.errors > 0) {
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = enabled
                ? "table full-load kafka reset had errors; cdc enabled anyway"
                : "table full-load kafka reset had errors; cdc enable did not update catalog",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = source_schema,
            .source_table = source_table,
            .context = {
                {"db_engine", db_engine},
                {"catalog_id", catalog_id},
                {"kafka_errors", stats.errors},
                {"kafka_tables", stats.tables},
            },
        });
    }
    return enabled;
}

FullLoadKafkaResetStats reset_kafka_apply_after_full_load(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id) {
    FullLoadKafkaResetStats stats;

    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    const std::string bootstrap = kafka.bootstrap;
    ensure_apply_positions_for_conn(pg, conn_id, db_engine);

    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT ap.catalog_id, ap.source_schema, ap.source_table, ap.kafka_topic, ap.kafka_partition
        FROM cdc_catalog.apply_position ap
        JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
        WHERE ap.conn_id = $1
          AND c.db_engine = $2::cdc_catalog.db_engine
          AND c.active = true
          AND c.needs_full_load = false
          AND NOT c.cdc_enabled
          AND c.status = 'full_load_in_progress'
        ORDER BY ap.source_schema, ap.source_table
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
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "full-load kafka reset skipped: apply_position query failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"db_engine", db_engine}},
        });
        stats.errors += 1;
        return stats;
    }

    struct ApplyRow {
        long long catalog_id;
        std::string schema;
        std::string table;
        std::string topic;
        int partition;
    };
    std::vector<ApplyRow> rows;
    rows.reserve(static_cast<size_t>(PQntuples(res)));
    for (int i = 0; i < PQntuples(res); ++i) {
        rows.push_back({
            std::atoll(PQgetvalue(res, i, 0)),
            PQgetvalue(res, i, 1),
            PQgetvalue(res, i, 2),
            PQgetvalue(res, i, 3),
            std::stoi(PQgetvalue(res, i, 4)),
        });
        stats.tables += 1;
    }
    PQclear(res);

    if (rows.empty()) {
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_catalog_onboard",
            .message = "full-load kafka reset skipped: no tables pending onboard",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"db_engine", db_engine}},
        });
        return stats;
    }

    std::vector<OnboardApplyRow> onboard_rows;
    onboard_rows.reserve(rows.size());
    for (const auto& row : rows) {
        onboard_rows.push_back({row.catalog_id, row.schema, row.table, row.topic, row.partition});
    }

    return execute_kafka_onboard_reset(
        pg, conn_id, db_engine, batch_id, bootstrap, onboard_rows, std::nullopt);
}

bool onboard_conn_after_full_load(
    const AppConfig& cfg,
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id) {
#ifdef HAVE_FREETDS
    if (db_engine == "mssql") {
        seed_mssql_cdc_lsn_for_conn(cfg, pg, conn_id, batch_id);
    }
#endif
#ifdef HAVE_MONGOC
    if (db_engine == "mongodb") {
        seed_mongo_cdc_resume_for_conn(cfg, pg, conn_id, batch_id);
    }
#endif
    const auto stats = reset_kafka_apply_after_full_load(pg, conn_id, db_engine, batch_id);
    enable_cdc_after_full_load(pg, conn_id, db_engine, batch_id, false);
    const int pending = count_full_load_pending_onboard(pg, conn_id, db_engine);
    if (stats.errors > 0) {
        log_write(pg, {
            .level = pending > 0 ? LogLevel::Warning : LogLevel::Info,
            .component = "cdc_catalog_onboard",
            .message = pending > 0
                ? "conn full-load kafka reset had errors; some tables still pending onboard"
                : "conn full-load kafka reset had errors; cdc enabled for loaded tables",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"db_engine", db_engine},
                {"kafka_errors", stats.errors},
                {"kafka_tables", stats.tables},
                {"pending_onboard", pending},
            },
        });
    }
    return pending == 0;
}

#ifdef HAVE_RDKAFKA

long long reset_apply_offset_to_end(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("kafka client create failed: ") + errstr);
    }

    int64_t low = 0;
    int64_t high = 0;
    rd_kafka_resp_err_t err =
        query_kafka_watermark_offsets_retry(rk, topic.c_str(), partition, &low, &high, 15000, 12);
    if (err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION) {
        rd_kafka_destroy(rk);
        return -1;
    }
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(rk);
        throw std::runtime_error(std::string("watermark query failed: ") + rd_kafka_err2str(err));
    }

    const long long new_offset = std::max<int64_t>(0, high - 1);
    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(parts, topic.c_str(), partition)->offset = high;

    rd_kafka_AlterConsumerGroupOffsets_t* alter =
        rd_kafka_AlterConsumerGroupOffsets_new(consumer_group.c_str(), parts);
    rd_kafka_AdminOptions_t* options =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
    if (rd_kafka_AdminOptions_set_request_timeout(options, 30000, errstr, sizeof(errstr)) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(rk);
        throw std::runtime_error(std::string("admin options failed: ") + errstr);
    }

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_AlterConsumerGroupOffsets(rk, &alter, 1, options, queue);

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 30000);

    rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_topic_partition_list_destroy(parts);
    rd_kafka_queue_destroy(queue);
    rd_kafka_destroy(rk);

    if (!event) {
        throw std::runtime_error("kafka alter consumer group offsets timed out");
    }
    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error(std::string("kafka alter offsets failed: ") + msg);
    }

    const rd_kafka_AlterConsumerGroupOffsets_result_t* result =
        rd_kafka_event_AlterConsumerGroupOffsets_result(event);
    if (result) {
        size_t group_count = 0;
        const rd_kafka_group_result_t* const* groups =
            rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &group_count);
        for (size_t i = 0; i < group_count; ++i) {
            const rd_kafka_error_t* group_err = rd_kafka_group_result_error(groups[i]);
            if (group_err) {
                const std::string msg = rd_kafka_error_string(group_err);
                rd_kafka_event_destroy(event);
                throw std::runtime_error(std::string("kafka alter offsets group failed: ") + msg);
            }
        }
    }
    rd_kafka_event_destroy(event);

    return new_offset;
}

long long reset_apply_consumer_offset(
    const std::string& bootstrap,
    const std::string& consumer_group,
    const std::string& topic,
    int partition,
    long long offset) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("kafka client create failed: ") + errstr);
    }

    const long long new_offset = std::max<int64_t>(0, offset - 1);
    rd_kafka_topic_partition_list_t* parts = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(parts, topic.c_str(), partition)->offset = offset;

    rd_kafka_AlterConsumerGroupOffsets_t* alter =
        rd_kafka_AlterConsumerGroupOffsets_new(consumer_group.c_str(), parts);
    rd_kafka_AdminOptions_t* options =
        rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
    if (rd_kafka_AdminOptions_set_request_timeout(options, 30000, errstr, sizeof(errstr)) !=
        RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_AdminOptions_destroy(options);
        rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
        rd_kafka_topic_partition_list_destroy(parts);
        rd_kafka_destroy(rk);
        throw std::runtime_error(std::string("admin options failed: ") + errstr);
    }

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_AlterConsumerGroupOffsets(rk, &alter, 1, options, queue);

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 30000);

    rd_kafka_AlterConsumerGroupOffsets_destroy(alter);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_topic_partition_list_destroy(parts);
    rd_kafka_queue_destroy(queue);
    rd_kafka_destroy(rk);

    if (!event) {
        throw std::runtime_error("kafka alter consumer group offsets timed out");
    }
    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        throw std::runtime_error(std::string("kafka alter offsets failed: ") + msg);
    }

    const rd_kafka_AlterConsumerGroupOffsets_result_t* result =
        rd_kafka_event_AlterConsumerGroupOffsets_result(event);
    if (result) {
        size_t group_count = 0;
        const rd_kafka_group_result_t* const* groups =
            rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &group_count);
        for (size_t i = 0; i < group_count; ++i) {
            const rd_kafka_error_t* group_err = rd_kafka_group_result_error(groups[i]);
            if (group_err) {
                const std::string msg = rd_kafka_error_string(group_err);
                rd_kafka_event_destroy(event);
                throw std::runtime_error(std::string("kafka alter offsets group failed: ") + msg);
            }
        }
    }
    rd_kafka_event_destroy(event);

    return new_offset;
}

void ensure_capture_kafka_topics(
    PGconn* pg,
    const std::string& component,
    const std::string& batch_id,
    const std::string& conn_id,
    const CaptureRuntimeConfig& rcfg,
    const std::vector<std::pair<std::string, std::string>>& tables,
    const std::set<std::pair<std::string, std::string>>& hot_tables) {
    if (tables.empty()) {
        return;
    }
    const auto topics =
        topics_for_tables(rcfg.topic_prefix, tables, rcfg.topic_mode, rcfg.topic_buckets, hot_tables);
    try {
        ensure_kafka_topics_exist(rcfg.bootstrap, topics, rcfg.topic_partitions, 1);
    } catch (const std::exception& ex) {
        log_write(pg, {
            .level = LogLevel::Error,
            .component = component,
            .message = "capture kafka ensure topics failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"error", ex.what()},
                {"topic_count", static_cast<int>(topics.size())},
                {"kafka_bootstrap", rcfg.bootstrap},
            },
        });
        throw;
    }
    log_write(pg, {
        .level = LogLevel::Info,
        .component = component,
        .message = "capture kafka topics ready",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"topic_count", static_cast<int>(topics.size())},
            {"kafka_bootstrap", rcfg.bootstrap},
            {"topic_mode", rcfg.topic_mode},
            {"topic_partitions", rcfg.topic_partitions},
        },
    });
}

void ensure_kafka_topics_exist(
    const std::string& bootstrap,
    const std::vector<std::string>& topics,
    int partition_count,
    int replication_factor) {
    if (topics.empty() || partition_count <= 0) {
        return;
    }

    // Match docker-compose broker defaults (3d time cap + 1GiB/partition + 128MiB segments).
    constexpr const char* kRetentionMs = "259200000";
    constexpr const char* kRetentionBytes = "1073741824";
    constexpr const char* kSegmentBytes = "134217728";
    constexpr const char* kCleanupPolicy = "delete";

    char errstr[512]{0};
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("kafka ensure topics conf failed: ") + errstr);
    }

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("kafka ensure topics client failed: ") + errstr);
    }

    std::vector<rd_kafka_NewTopic_t*> new_topics;
    new_topics.reserve(topics.size());
    for (const auto& topic : topics) {
        rd_kafka_NewTopic_t* nt =
            rd_kafka_NewTopic_new(topic.c_str(), partition_count, replication_factor, errstr, sizeof(errstr));
        if (!nt) {
            rd_kafka_NewTopic_destroy_array(new_topics.data(), new_topics.size());
            rd_kafka_destroy(rk);
            throw std::runtime_error(
                std::string("NewTopic failed for ") + topic + ": " + errstr);
        }
        rd_kafka_NewTopic_set_config(nt, "retention.ms", kRetentionMs);
        rd_kafka_NewTopic_set_config(nt, "retention.bytes", kRetentionBytes);
        rd_kafka_NewTopic_set_config(nt, "segment.bytes", kSegmentBytes);
        rd_kafka_NewTopic_set_config(nt, "cleanup.policy", kCleanupPolicy);
        new_topics.push_back(nt);
    }

    rd_kafka_AdminOptions_t* options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    rd_kafka_AdminOptions_set_request_timeout(options, 30000, errstr, sizeof(errstr));
    rd_kafka_AdminOptions_set_operation_timeout(options, 30000, errstr, sizeof(errstr));

    rd_kafka_queue_t* queue = rd_kafka_queue_new(rk);
    rd_kafka_CreateTopics(rk, new_topics.data(), new_topics.size(), options, queue);
    rd_kafka_AdminOptions_destroy(options);
    rd_kafka_NewTopic_destroy_array(new_topics.data(), new_topics.size());

    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, 60000);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        rd_kafka_destroy(rk);
        throw std::runtime_error("ensure_kafka_topics_exist: CreateTopics timed out");
    }

    if (rd_kafka_event_error(event)) {
        const std::string msg = rd_kafka_event_error_string(event);
        rd_kafka_event_destroy(event);
        rd_kafka_destroy(rk);
        throw std::runtime_error("ensure_kafka_topics_exist: " + msg);
    }

    const rd_kafka_CreateTopics_result_t* result = rd_kafka_event_CreateTopics_result(event);
    if (result) {
        size_t res_cnt = 0;
        const rd_kafka_topic_result_t** res = rd_kafka_CreateTopics_result_topics(result, &res_cnt);
        for (size_t i = 0; i < res_cnt; ++i) {
            const rd_kafka_resp_err_t code = rd_kafka_topic_result_error(res[i]);
            if (code == RD_KAFKA_RESP_ERR_NO_ERROR || code == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                continue;
            }
            const char* msg = rd_kafka_topic_result_error_string(res[i]);
            rd_kafka_event_destroy(event);
            rd_kafka_destroy(rk);
            throw std::runtime_error(
                std::string("ensure_kafka_topics_exist: ") + (msg ? msg : rd_kafka_err2str(code)));
        }
    }
    rd_kafka_event_destroy(event);

    ensure_kafka_topic_partition_count(rk, topics, partition_count);
    rd_kafka_destroy(rk);
}

#else

long long reset_apply_offset_to_end(
    const std::string&,
    const std::string&,
    const std::string&,
    int) {
    return 0;
}

long long reset_apply_consumer_offset(
    const std::string&,
    const std::string&,
    const std::string&,
    int,
    long long) {
    return 0;
}

void ensure_kafka_topics_exist(
    const std::string&,
    const std::vector<std::string>&,
    int,
    int) {}

void ensure_capture_kafka_topics(
    PGconn*,
    const std::string&,
    const std::string&,
    const std::string&,
    const CaptureRuntimeConfig&,
    const std::vector<std::pair<std::string, std::string>>&,
    const std::set<std::pair<std::string, std::string>>&) {}

#endif
