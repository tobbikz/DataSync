#include "mongo_kafka_capture.hpp"

#include "capture_common.hpp"
#include "cdc_envelope.hpp"
#include "kafka_producer.hpp"
#include "kafka_topics.hpp"
#include "mongo_conn.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "runtime_config.hpp"

#include <algorithm>
#include <bson/bson.h>
#include <chrono>
#include <set>
#include <stdexcept>
#include <vector>

#ifndef HAVE_MONGOC

MongoCaptureStats run_mongo_kafka_capture_slice(
    const AppConfig&,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>&,
    const std::string& batch_id,
    int,
    int) {
    log_write(log_pg, {
        .level = LogLevel::Warning,
        .component = "cdc_kafka_mongo_capture",
        .message = "mongo capture skipped: rebuild with libmongoc",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"hint", "install libmongoc"}},
    });
    return {};
}

int seed_mongo_cdc_resume_for_conn(
    const AppConfig&,
    PGconn*,
    const std::string&,
    const std::optional<std::string>&,
    const std::string&) {
    return 0;
}

#else

#include <thread>

namespace {

nlohmann::json bson_to_json(const bson_t* doc) {
    if (!doc) {
        return nullptr;
    }
    char* json = bson_as_relaxed_extended_json(doc, nullptr);
    if (!json) {
        return nullptr;
    }
    auto parsed = nlohmann::json::parse(json, nullptr, false);
    bson_free(json);
    return parsed.is_discarded() ? nlohmann::json(nullptr) : parsed;
}

nlohmann::json get_stored_resume_token(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& database,
    const std::string& collection) {
    const char* vals[] = {conn_id.c_str(), database.c_str(), collection.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT resume_token FROM cdc_catalog.cdc_mongo_resume
        WHERE conn_id = $1 AND database = $2 AND collection = $3
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
        return nullptr;
    }
    const char* txt = PQgetvalue(res, 0, 0);
    nlohmann::json token = nullptr;
    if (txt && *txt) {
        token = nlohmann::json::parse(txt, nullptr, false);
        if (token.is_discarded()) {
            token = nullptr;
        }
    }
    PQclear(res);
    return token;
}

void clear_stored_resume_token(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& database,
    const std::string& collection) {
    const char* vals[] = {conn_id.c_str(), database.c_str(), collection.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        DELETE FROM cdc_catalog.cdc_mongo_resume
        WHERE conn_id = $1 AND database = $2 AND collection = $3
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void upsert_resume_token(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& database,
    const std::string& collection,
    const nlohmann::json& resume_token) {
    const std::string token_json = resume_token.is_null() ? "" : resume_token.dump();
    const char* vals[] = {conn_id.c_str(), database.c_str(), collection.c_str(), token_json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.cdc_mongo_resume
            (conn_id, database, collection, resume_token, updated_at)
        VALUES ($1, $2, $3, $4::jsonb, now())
        ON CONFLICT (conn_id, database, collection) DO UPDATE SET
            resume_token = EXCLUDED.resume_token,
            updated_at = now()
        )",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

std::string mongo_op_char(const std::string& op_type) {
    if (op_type == "insert" || op_type == "replace") {
        return "c";
    }
    if (op_type == "update") {
        return "u";
    }
    if (op_type == "delete") {
        return "d";
    }
    return {};
}

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

MongoCaptureStats run_mongo_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id,
    int worker_id,
    int worker_count) {
    MongoCaptureStats stats;
    stats.batch_id = batch_id;
    const auto start = std::chrono::steady_clock::now();

    const MongoSource* source = find_mongo_source(cfg, conn_id);
    if (!source) {
        throw std::runtime_error("MongoDB source not found: " + conn_id);
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const CaptureRuntimeConfig rcfg =
        load_mongo_capture_runtime(runtime, log_pg, conn_id, &cfg.cdc);
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap(runtime, conn_id);
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_mongo_capture",
        .message = "mongo capture slice started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tier", service_tier.value_or("all")},
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix", rcfg.topic_prefix},
        },
    });
    const auto collections =
        fetch_capture_catalog_tables(log_pg, conn_id, service_tier, worker_id, worker_count, "mongodb");
    clear_stale_cdc_in_progress(log_pg, conn_id, service_tier, "mongodb");
    if (collections.empty()) {
        log_cdc_skip_no_tables(
            log_pg, "cdc_kafka_mongo_capture", "capture", batch_id, conn_id, service_tier, "mongodb");
        return stats;
    }

    std::vector<std::pair<std::string, std::string>> table_pairs;
    table_pairs.reserve(collections.size());
    for (const auto& tbl : collections) {
        table_pairs.emplace_back(tbl.source_schema, tbl.source_table);
    }
    ensure_capture_kafka_topics(
        log_pg, "cdc_kafka_mongo_capture", batch_id, conn_id, rcfg, table_pairs);

    KafkaProducer producer(rcfg.bootstrap, rcfg.linger_ms, rcfg.producer_batch);
    if (!producer.available()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_mongo_capture",
            .message = "mongo capture kafka producer unavailable",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"kafka_bootstrap", rcfg.bootstrap}},
        });
        throw std::runtime_error("Kafka producer unavailable for Mongo capture");
    }

    MongoConn mongo(*source);
    int published = 0;
    int changes_read = 0;
    struct MongoPendingCommit {
        long long catalog_id{0};
        std::string source_database;
        std::string source_table;
        nlohmann::json resume_token;
        bool has_token{false};
    };
    std::vector<MongoPendingCommit> pending_commits;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(rcfg.max_seconds);

    for (const auto& coll : collections) {
        if (published >= rcfg.max_events || std::chrono::steady_clock::now() >= deadline) {
            break;
        }

        mark_catalog_cdc_in_progress(log_pg, coll.catalog_id);

        bool coll_failed = false;
        nlohmann::json resume_after = get_stored_resume_token(
            log_pg, conn_id, coll.source_database, coll.source_table);
        nlohmann::json last_token = resume_after;

        mongoc_collection_t* collection =
            mongoc_client_get_collection(mongo.client, coll.source_database.c_str(), coll.source_table.c_str());
        bson_t pipeline = BSON_INITIALIZER;
        bson_t* opts = BCON_NEW("fullDocument", BCON_UTF8("updateLookup"));
        const int await_ms = std::max(500, std::min(30000, (rcfg.idle_poll_seconds > 0 ? rcfg.idle_poll_seconds : 3) * 1000));
        BSON_APPEND_INT32(opts, "maxAwaitTimeMS", await_ms);
        if (!resume_after.is_null()) {
            bson_t resume_bson;
            const std::string resume_str = resume_after.dump();
            if (bson_init_from_json(&resume_bson, resume_str.c_str(), resume_str.size(), nullptr)) {
                BSON_APPEND_DOCUMENT(opts, "resumeAfter", &resume_bson);
                bson_destroy(&resume_bson);
            }
        }

        mongoc_change_stream_t* stream = mongoc_collection_watch(collection, &pipeline, opts);
        bson_destroy(opts);

        if (!stream) {
            mongoc_collection_destroy(collection);
            bson_destroy(&pipeline);
            rollback_cdc_in_progress_ids(log_pg, {coll.catalog_id});
            stats.errors += 1;
            continue;
        }

        const bson_t* change = nullptr;
        while (published < rcfg.max_events && std::chrono::steady_clock::now() < deadline) {
            if (!mongoc_change_stream_next(stream, &change)) {
                bson_error_t stream_err;
                if (mongoc_change_stream_error_document(stream, &stream_err, nullptr)) {
                    const std::string err_msg = stream_err.message;
                    const bool invalidate_resume =
                        err_msg.find("invalidate") != std::string::npos;
                    if (invalidate_resume) {
                        clear_stored_resume_token(
                            log_pg, conn_id, coll.source_database, coll.source_table);
                        log_write(log_pg, {
                            .level = LogLevel::Warning,
                            .component = "cdc_kafka_mongo_capture",
                            .message = "mongo resume cleared after invalidate",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .source_schema = coll.source_database,
                            .source_table = coll.source_table,
                        });
                        mongoc_change_stream_destroy(stream);
                        mongoc_collection_destroy(collection);
                        bson_destroy(&pipeline);
                        resume_after = nlohmann::json();
                        last_token = nlohmann::json();
                        collection = mongoc_client_get_collection(
                            mongo.client, coll.source_database.c_str(), coll.source_table.c_str());
                        bson_reinit(&pipeline);
                        opts = BCON_NEW("fullDocument", BCON_UTF8("updateLookup"));
                        BSON_APPEND_INT32(opts, "maxAwaitTimeMS", await_ms);
                        stream = mongoc_collection_watch(collection, &pipeline, opts);
                        bson_destroy(opts);
                        if (!stream) {
                            mongoc_collection_destroy(collection);
                            bson_destroy(&pipeline);
                            rollback_cdc_in_progress_ids(log_pg, {coll.catalog_id});
                            stats.errors += 1;
                            coll_failed = true;
                            break;
                        }
                        continue;
                    }
                    stats.errors += 1;
                    rollback_cdc_in_progress_ids(log_pg, {coll.catalog_id});
                    coll_failed = true;
                    log_write(log_pg, {
                        .level = LogLevel::Error,
                        .component = "cdc_kafka_mongo_capture",
                        .message = "mongo change stream error",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = coll.source_database,
                        .source_table = coll.source_table,
                        .context = {{"error", err_msg}},
                    });
                    break;
                }
                const bson_t* idle_token = mongoc_change_stream_get_resume_token(stream);
                if (idle_token) {
                    last_token = bson_to_json(idle_token);
                }
                if (std::chrono::steady_clock::now() >= deadline) {
                    break;
                }
                continue;
            }
            changes_read += 1;
            const bson_t* token_bson = mongoc_change_stream_get_resume_token(stream);
            if (token_bson) {
                last_token = bson_to_json(token_bson);
            }

            const nlohmann::json change_json = bson_to_json(change);
            if (!change_json.is_object()) {
                continue;
            }
            const std::string op_type = change_json.value("operationType", "");
            const std::string op = mongo_op_char(op_type);
            if (op.empty()) {
                continue;
            }

            nlohmann::json before = nullptr;
            nlohmann::json after = nullptr;
            if (op == "d") {
                const auto doc_key = change_json.find("documentKey");
                if (doc_key != change_json.end() && doc_key->is_object()) {
                    const auto id_it = doc_key->find("_id");
                    before = nlohmann::json::object();
                    if (id_it != doc_key->end() && !id_it->is_null()) {
                        before["mongo_id"] = mongo_object_id_text(*id_it);
                    } else {
                        before["mongo_id"] = nullptr;
                    }
                }
            } else {
                const auto full_doc = change_json.find("fullDocument");
                if (full_doc != change_json.end() && full_doc->is_object()) {
                    const auto flat = flatten_mongo_document(*full_doc);
                    after = nlohmann::json::object();
                    for (const auto& [k, v] : flat) {
                        after[k] = v;
                    }
                }
            }

            CdcEvent event;
            event.op = op;
            event.conn_id = conn_id;
            event.db_engine = "mongodb";
            event.source_database = coll.source_database;
            event.schema_name = coll.source_database;
            event.table_name = coll.source_table;
            event.collection = coll.source_table;
            event.before = before;
            event.after = after;
            if (change_json.contains("clusterTime") && change_json["clusterTime"].is_object()) {
                event.gtid = change_json["clusterTime"].dump();
            } else if (change_json.contains("wallTime")) {
                event.gtid = change_json["wallTime"].dump();
            } else {
                event.gtid = last_token.is_null() ? "" : last_token.dump();
            }
            event.resume_token = last_token;
            event.ts_ms = now_ms();
            event.ingestion_ts = utc_iso_timestamp_now();

            const std::string topic = topic_for_catalog(
                rcfg.topic_prefix, coll.lake_schema, coll.lake_table, rcfg.topic_mode, rcfg.topic_buckets);
            const nlohmann::json* row_for_key = (op == "d") ? &before : &after;
            const std::string msg_key = kafka_message_key_for_row(
                coll.lake_schema, coll.lake_table, row_for_key, coll.pk_columns);
            producer.produce(topic, msg_key, event.to_kafka_dict().dump());
            published += 1;
        }

        if (!coll_failed && stream && mongoc_change_stream_error_document(stream, nullptr, nullptr)) {
            coll_failed = true;
            stats.errors += 1;
            rollback_cdc_in_progress_ids(log_pg, {coll.catalog_id});
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_mongo_capture",
                .message = "mongo collection capture failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = coll.source_database,
                .source_table = coll.source_table,
            });
        } else if (!coll_failed && !last_token.is_null()) {
            MongoPendingCommit pending;
            pending.catalog_id = coll.catalog_id;
            pending.source_database = coll.source_database;
            pending.source_table = coll.source_table;
            pending.resume_token = last_token;
            pending.has_token = true;
            pending_commits.push_back(std::move(pending));
        }

        if (stream) {
            mongoc_change_stream_destroy(stream);
        }
        mongoc_collection_destroy(collection);
        bson_destroy(&pipeline);
    }

    const int queued = producer.flush(30);
    const KafkaProducerStats pstats = producer.stats();
    if (pstats.errors > 0 || queued > 0) {
        std::set<long long> rollback_ids;
        for (const auto& p : pending_commits) {
            rollback_ids.insert(p.catalog_id);
        }
        if (!rollback_ids.empty()) {
            rollback_cdc_in_progress_ids(log_pg, rollback_ids);
        }
        const std::string err_detail =
            !pstats.first_error.empty() ? pstats.first_error : std::to_string(queued) + " messages still queued";
        throw std::runtime_error("kafka publish failed: " + err_detail);
    }

    for (const auto& p : pending_commits) {
        if (p.has_token) {
            upsert_resume_token(log_pg, conn_id, p.source_database, p.source_table, p.resume_token);
        }
        mark_catalog_cdc_success(log_pg, p.catalog_id);
        stats.collections += 1;
    }

    stats.events_published = pstats.events_published;
    stats.errors = pstats.errors;
    stats.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();

    if (changes_read > 0 && pstats.events_published == 0) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_kafka_mongo_capture",
            .message = "capture change stream events read but none published to kafka",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"changes_read", changes_read},
                {"events_published", pstats.events_published},
                {"kafka_bootstrap", rcfg.bootstrap},
                {"catalog_collections", static_cast<int>(collections.size())},
            },
        });
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_mongo_capture",
        .message = "mongo capture slice completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"events_published", stats.events_published},
            {"changes_read", changes_read},
            {"collections", stats.collections},
            {"duration_ms", stats.duration_ms},
            {"tier", service_tier.value_or("all")},
        },
    });

    return stats;
}

bool seed_mongo_cdc_resume_for_collection_if_absent(
    PGconn* log_pg,
    MongoConn& mongo,
    const std::string& conn_id,
    const std::string& database,
    const std::string& collection) {
    const nlohmann::json existing = get_stored_resume_token(log_pg, conn_id, database, collection);
    if (!existing.is_null()) {
        return false;
    }

    mongoc_collection_t* coll = mongoc_client_get_collection(mongo.client, database.c_str(), collection.c_str());
    bson_t pipeline = BSON_INITIALIZER;
    bson_t* opts = BCON_NEW("fullDocument", BCON_UTF8("updateLookup"));
    mongoc_change_stream_t* stream = mongoc_collection_watch(coll, &pipeline, opts);
    bson_destroy(opts);

    if (!stream) {
        mongoc_collection_destroy(coll);
        return false;
    }

    bool seeded = false;
    const bson_t* token_bson = mongoc_change_stream_get_resume_token(stream);
    if (token_bson) {
        const nlohmann::json token = bson_to_json(token_bson);
        if (!token.is_null()) {
            upsert_resume_token(log_pg, conn_id, database, collection, token);
            seeded = true;
        }
    }

    mongoc_change_stream_destroy(stream);
    mongoc_collection_destroy(coll);
    return seeded;
}

int seed_mongo_cdc_resume_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& service_tier,
    const std::string& batch_id) {
    const MongoSource* source = find_mongo_source(cfg, conn_id);
    if (!source) {
        return 0;
    }

    std::string sql = R"(
        SELECT source_database, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = 'mongodb'
          AND active = true
          AND cdc_enabled = true
    )";
    std::vector<const char*> vals = {conn_id.c_str()};
    std::string tier_val;
    if (service_tier && !service_tier->empty()) {
        sql += " AND service_tier::text = lower($2)";
        tier_val = *service_tier;
        vals.push_back(tier_val.c_str());
    }

    PGresult* res = PQexecParams(
        log_pg,
        sql.c_str(),
        static_cast<int>(vals.size()),
        nullptr,
        vals.data(),
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_catalog_onboard",
            .message = "mongo resume seed skipped: catalog query failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
        });
        return 1;
    }

    int seeded = 0;
    int errors = 0;

    try {
        MongoConn mongo(*source);
        for (int i = 0; i < PQntuples(res); ++i) {
            const std::string database = PQgetvalue(res, i, 0);
            const std::string collection = PQgetvalue(res, i, 1);

            if (!get_stored_resume_token(log_pg, conn_id, database, collection).is_null()) {
                continue;
            }

            mongoc_collection_t* coll =
                mongoc_client_get_collection(mongo.client, database.c_str(), collection.c_str());
            bson_t pipeline = BSON_INITIALIZER;
            bson_t* opts = BCON_NEW("fullDocument", BCON_UTF8("updateLookup"));
            mongoc_change_stream_t* stream = mongoc_collection_watch(coll, &pipeline, opts);
            bson_destroy(opts);

            if (!stream) {
                mongoc_collection_destroy(coll);
                errors += 1;
                continue;
            }

            const bson_t* token_bson = mongoc_change_stream_get_resume_token(stream);
            if (token_bson) {
                const nlohmann::json token = bson_to_json(token_bson);
                if (!token.is_null()) {
                    upsert_resume_token(log_pg, conn_id, database, collection, token);
                    seeded += 1;
                }
            }

            mongoc_change_stream_destroy(stream);
            mongoc_collection_destroy(coll);
        }
    } catch (const std::exception& ex) {
        errors += 1;
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_catalog_onboard",
            .message = "mongo resume seed failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .context = {{"error", ex.what()}},
        });
    }

    PQclear(res);

    log_write(log_pg, {
        .level = errors ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = errors ? "mongo resume seed completed with errors" : "mongo resume seed completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .context = {{"seeded", seeded}, {"errors", errors}},
    });
    return errors;
}

#endif
