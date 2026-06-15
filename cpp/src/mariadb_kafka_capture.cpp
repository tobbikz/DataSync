#include "mariadb_kafka_capture.hpp"

#include "capture_common.hpp"
#include "cdc_envelope.hpp"
#include "kafka_producer.hpp"
#include "kafka_topics.hpp"
#include "mariadb_binlog.hpp"
#include "mariadb_binlog_cli.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "obs_log.hpp"
#include "runtime_config.hpp"

#include <cctype>
#include <chrono>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <algorithm>
#include <thread>

namespace {

using TableKey = std::pair<std::string, std::string>;

std::string to_lower_copy(std::string value) {
    std::transform(
        value.begin(),
        value.end(),
        value.begin(),
        [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

/** Map binlog schema.table → canonical catalog source_schema/source_table. */
struct CaptureBinlogResolver {
    std::set<TableKey> catalog_keys;
    std::map<TableKey, TableKey> lower_to_catalog;
    std::map<std::string, std::vector<TableKey>> catalog_keys_by_lower_table;

    void add(const TableKey& catalog_key) {
        catalog_keys.insert(catalog_key);
        const TableKey lower_key{to_lower_copy(catalog_key.first), to_lower_copy(catalog_key.second)};
        lower_to_catalog[lower_key] = catalog_key;
        catalog_keys_by_lower_table[lower_key.second].push_back(catalog_key);
    }

    enum class ResolveKind { Exact, CaseFold, NoMatch, SchemaMismatch };

    struct ResolveResult {
        ResolveKind kind{ResolveKind::NoMatch};
        TableKey catalog_key;
    };

    ResolveResult resolve(const std::string& binlog_schema, const std::string& binlog_table) const {
        const TableKey binlog_key{binlog_schema, binlog_table};
        if (catalog_keys.count(binlog_key) > 0) {
            return {ResolveKind::Exact, binlog_key};
        }
        const TableKey lower_key{to_lower_copy(binlog_schema), to_lower_copy(binlog_table)};
        if (const auto it = lower_to_catalog.find(lower_key); it != lower_to_catalog.end()) {
            return {ResolveKind::CaseFold, it->second};
        }
        if (const auto tit = catalog_keys_by_lower_table.find(lower_key.second);
            tit != catalog_keys_by_lower_table.end()) {
            for (const auto& catalog_key : tit->second) {
                if (to_lower_copy(catalog_key.first) != lower_key.first) {
                    return {ResolveKind::SchemaMismatch, catalog_key};
                }
            }
        }
        return {ResolveKind::NoMatch, {}};
    }
};

struct CapturePosition {
    std::string binlog_file;
    long long binlog_position{0};
    std::string server_uuid;
    bool from_capture_position{false};
};

std::string mariadb_scalar(MYSQL* mysql, const std::string& sql) {
    if (mysql_query(mysql, sql.c_str()) != 0) {
        throw std::runtime_error(std::string("MariaDB query failed: ") + mysql_error(mysql));
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return {};
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    std::string out;
    if (row && row[0]) {
        out = row[0];
    }
    mysql_free_result(res);
    return out;
}

std::vector<std::string> fetch_table_columns(MYSQL* mysql, const std::string& schema, const std::string& table) {
    auto esc = [mysql](const std::string& val) {
        std::string out(val.size() * 2 + 1, '\0');
        const unsigned long len = mysql_real_escape_string(mysql, out.data(), val.c_str(), static_cast<unsigned long>(val.size()));
        out.resize(len);
        return out;
    };
    std::ostringstream sql;
    sql << "SELECT column_name FROM information_schema.columns "
        << "WHERE table_schema='" << esc(schema) << "' AND table_name='" << esc(table)
        << "' ORDER BY ordinal_position";
    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        throw std::runtime_error(std::string("MariaDB columns query failed: ") + mysql_error(mysql));
    }
    std::vector<std::string> cols;
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return cols;
    }
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (row[0]) {
            cols.emplace_back(row[0]);
        }
    }
    mysql_free_result(res);
    return cols;
}

std::string mariadb_server_uuid(MYSQL* mysql) {
    try {
        const std::string uuid = mariadb_scalar(mysql, "SELECT @@server_uuid");
        if (!uuid.empty() && uuid != "unknown") {
            return uuid;
        }
    } catch (...) {
    }
    try {
        const std::string uid = mariadb_scalar(mysql, "SELECT @@server_uid");
        if (!uid.empty()) {
            return uid;
        }
    } catch (...) {
    }
    return {};
}

bool mariadb_legacy_server_identity(const std::string& id) {
    if (id.empty() || id == "unknown") {
        return true;
    }
    return std::all_of(id.begin(), id.end(), [](unsigned char c) { return std::isdigit(c) != 0; });
}

struct MasterStatusLite {
    std::string file;
    long long position{0};
};

MasterStatusLite fetch_master_status(MYSQL* mysql) {
    const MasterBinlogStatus st = fetch_master_binlog_status(mysql);
    return {st.file, st.position};
}

CapturePosition read_capture_position(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT binlog_file, binlog_position, server_uuid FROM cdc_catalog.capture_position WHERE conn_id = $1",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("failed to read capture_position");
    }

    if (PQntuples(res) == 0 || !PQgetvalue(res, 0, 0) || !PQgetvalue(res, 0, 0)[0]) {
        PQclear(res);
        throw std::runtime_error("no capture position; run full-load first");
    }

    CapturePosition pos;
    const char* bf = PQgetvalue(res, 0, 0);
    pos.binlog_file = bf ? bf : "";
    const char* bp = PQgetvalue(res, 0, 1);
    pos.binlog_position = bp ? std::atoll(bp) : 0;
    if (PQgetvalue(res, 0, 2)) {
        pos.server_uuid = PQgetvalue(res, 0, 2);
    }
    pos.from_capture_position = true;
    PQclear(res);
    return pos;
}

void upsert_capture_position(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& binlog_file,
    long long binlog_pos,
    const std::string& server_uuid_val,
    const std::string& status = "healthy",
    const std::string& last_error = "",
    int capture_lag_seconds = 0) {
    const std::string pos_str = std::to_string(binlog_pos);
    const std::string lag_str = std::to_string(capture_lag_seconds);
    const char* vals1[] = {
        conn_id.c_str(),
        binlog_file.c_str(),
        pos_str.c_str(),
        server_uuid_val.c_str(),
        status.c_str(),
        last_error.empty() ? nullptr : last_error.c_str(),
        lag_str.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.capture_position
            (conn_id, gtid_set, binlog_file, binlog_position, server_uuid, status, last_error,
             capture_lag_seconds, last_event_ts, updated_at)
        VALUES ($1, '', $2, $3::bigint, $4, $5::cdc_catalog.cdc_health_status, $6, $7::integer, now(), now())
        ON CONFLICT (conn_id) DO UPDATE SET
            binlog_file = EXCLUDED.binlog_file,
            binlog_position = EXCLUDED.binlog_position,
            server_uuid = EXCLUDED.server_uuid,
            status = EXCLUDED.status,
            last_error = EXCLUDED.last_error,
            capture_lag_seconds = EXCLUDED.capture_lag_seconds,
            last_event_ts = now(),
            updated_at = now()
        )",
        7,
        vals1);
}

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

MariaDbCaptureStats run_mariadb_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int worker_id,
    int worker_count) {
    MariaDbCaptureStats stats;
    stats.batch_id = batch_id;
    const auto start = std::chrono::steady_clock::now();

    const MariaDbSource* source = find_mariadb_source(cfg, conn_id);
    if (!source) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_capture",
            .message = "mariadb connect failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"reason", "source not found in config"}},
        });
        throw std::runtime_error("MariaDB source not found: " + conn_id);
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const CaptureRuntimeConfig rcfg =
        load_mariadb_capture_runtime(runtime, log_pg, conn_id, &cfg.cdc);
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture slice started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"worker_id", worker_id},
            {"worker_count", worker_count},
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix", rcfg.topic_prefix},
            {"topic_mode", rcfg.topic_mode},
            {"topic_buckets", rcfg.topic_buckets},
            {"max_seconds", rcfg.max_seconds},
            {"max_events", rcfg.max_events},
        },
    });

    // One binlog cursor per conn: serialize capture per connection.
    static std::map<std::string, std::unique_ptr<std::mutex>> g_mariadb_capture_mutexes;
    static std::mutex g_mariadb_capture_registry_mu;
    std::unique_lock<std::mutex> registry_lock(g_mariadb_capture_registry_mu);
    auto& conn_mu_ptr = g_mariadb_capture_mutexes[conn_id];
    if (!conn_mu_ptr) {
        conn_mu_ptr = std::make_unique<std::mutex>();
    }
    registry_lock.unlock();
    std::lock_guard<std::mutex> capture_lock(*conn_mu_ptr);

    const auto tables =
        fetch_capture_catalog_tables(log_pg, conn_id, worker_id, worker_count, "mariadb");
    clear_stale_cdc_in_progress(log_pg, conn_id, "mariadb");
    if (tables.empty()) {
        log_cdc_skip_no_tables(
            log_pg, "cdc_kafka_capture", "capture", batch_id, conn_id, "mariadb");
        return stats;
    }

    std::set<TableKey> wanted;
    CaptureBinlogResolver binlog_resolver;
    std::map<TableKey, std::string> pk_by_table;
    std::map<TableKey, long long> catalog_id_by_table;
    nlohmann::json catalog_tables_json = nlohmann::json::array();
    for (const auto& tbl : tables) {
        const TableKey key{tbl.source_schema, tbl.source_table};
        wanted.insert(key);
        binlog_resolver.add(key);
        pk_by_table[key] = tbl.pk_columns;
        catalog_id_by_table[key] = tbl.catalog_id;
        catalog_tables_json.push_back(tbl.source_schema + "." + tbl.source_table);
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture tables selected",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"table_count", static_cast<int>(tables.size())},
            {"catalog_tables", catalog_tables_json},
        },
    });

    std::set<long long> cdc_active_catalog_ids;

    try {
    MariaDbConn mariadb(*source);
    touch_capture_position_slice(log_pg, conn_id);
    std::map<std::pair<std::string, std::string>, std::vector<std::string>> col_cache;
    for (const auto& key : wanted) {
        col_cache[key] = fetch_table_columns(mariadb.handle, key.first, key.second);
    }

    std::vector<std::pair<std::string, std::string>> table_pairs;
    table_pairs.reserve(wanted.size());
    for (const auto& key : wanted) {
        table_pairs.emplace_back(key.first, key.second);
    }
    ensure_capture_kafka_topics(
        log_pg, "cdc_kafka_capture", batch_id, conn_id, rcfg, table_pairs);

    CapturePosition start_pos = read_capture_position(log_pg, conn_id);
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture binlog resume",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"binlog_file", start_pos.binlog_file},
            {"binlog_position", start_pos.binlog_position},
            {"server_uuid", start_pos.server_uuid},
        },
    });
    const std::string live_uuid = mariadb_server_uuid(mariadb.handle);
    if (!start_pos.server_uuid.empty() && !live_uuid.empty() && start_pos.server_uuid != live_uuid) {
        if (mariadb_legacy_server_identity(start_pos.server_uuid) ||
            mariadb_legacy_server_identity(live_uuid)) {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_capture",
                .message = "capture server identity reconciled",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"stored", start_pos.server_uuid},
                    {"live", live_uuid},
                },
            });
            upsert_capture_position(
                log_pg,
                conn_id,
                start_pos.binlog_file,
                start_pos.binlog_position,
                live_uuid,
                "healthy",
                "");
        } else {
            upsert_capture_position(
                log_pg,
                conn_id,
                start_pos.binlog_file,
                start_pos.binlog_position,
                live_uuid,
                "gap_detected",
                "server_uuid changed " + start_pos.server_uuid + " -> " + live_uuid);
            throw std::runtime_error("server_uuid changed; gap_detected — run recovery");
        }
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture kafka producer connecting",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"kafka_bootstrap", rcfg.bootstrap}, {"kafka_bootstrap_source", kafka.source}},
    });
    KafkaProducer producer(
        rcfg.bootstrap,
        rcfg.linger_ms,
        rcfg.producer_batch,
        rcfg.producer_queue_max_messages,
        rcfg.producer_queue_max_kbytes);
    if (!producer.available()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_capture",
            .message = "capture kafka producer unavailable",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"kafka_bootstrap", rcfg.bootstrap}},
        });
        throw std::runtime_error("Kafka producer unavailable for capture");
    }

    const std::string binlog_cli = find_mariadb_binlog_binary();
    if (binlog_cli.empty()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_capture",
            .message = "capture binlog cli missing",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"hint", "install mariadb-client in runtime image or set MARIADB_BINLOG_PATH"},
            },
        });
        throw std::runtime_error("mariadb-binlog not found in PATH");
    }

    const MasterStatusLite master_now = fetch_master_status(mariadb.handle);
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture binlog cli ready",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"binlog_cli", binlog_cli},
            {"resume_file", start_pos.binlog_file},
            {"resume_position", start_pos.binlog_position},
            {"master_file", master_now.file},
            {"master_position", master_now.position},
            {"mariadb_host", source->host},
            {"mariadb_port", source->port},
        },
    });

    BinlogPosition binlog_start{start_pos.binlog_file, start_pos.binlog_position};
    bool first_kafka_publish_logged = false;
    int watched_binlog_events = 0;
    int unwatched_binlog_events = 0;
    int schema_mismatch_events = 0;
    int case_fold_resolved_events = 0;
    int schema_mismatch_logs = 0;
    std::set<TableKey> schema_mismatch_samples;
    const auto publish_row = [&](
                               const std::string& schema,
                               const std::string& table,
                               const std::string& op,
                               const std::vector<std::string>& col_values,
                               const std::vector<std::string>* before_col_values,
                               long long event_position) {
        const CaptureBinlogResolver::ResolveResult resolved = binlog_resolver.resolve(schema, table);
        if (resolved.kind == CaptureBinlogResolver::ResolveKind::NoMatch) {
            unwatched_binlog_events += 1;
            return;
        }
        if (resolved.kind == CaptureBinlogResolver::ResolveKind::SchemaMismatch) {
            schema_mismatch_events += 1;
            const TableKey sample_key{schema, table};
            if (schema_mismatch_samples.size() < 8) {
                schema_mismatch_samples.insert(sample_key);
            }
            if (schema_mismatch_logs < 5) {
                schema_mismatch_logs += 1;
                log_write(log_pg, {
                    .level = LogLevel::Warning,
                    .component = "cdc_kafka_capture",
                    .message = "capture binlog schema mismatch for catalog table",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = resolved.catalog_key.first,
                    .source_table = resolved.catalog_key.second,
                    .context = {
                        {"binlog_schema", schema},
                        {"binlog_table", table},
                        {"catalog_schema", resolved.catalog_key.first},
                        {"catalog_table", resolved.catalog_key.second},
                        {"op", op},
                    },
                });
            }
            return;
        }
        if (resolved.kind == CaptureBinlogResolver::ResolveKind::CaseFold) {
            case_fold_resolved_events += 1;
            if (case_fold_resolved_events <= 3) {
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_capture",
                    .message = "capture binlog table resolved via case fold",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = resolved.catalog_key.first,
                    .source_table = resolved.catalog_key.second,
                    .context = {
                        {"binlog_schema", schema},
                        {"binlog_table", table},
                        {"catalog_schema", resolved.catalog_key.first},
                        {"catalog_table", resolved.catalog_key.second},
                    },
                });
            }
        }

        const TableKey& key = resolved.catalog_key;
        watched_binlog_events += 1;
        const auto cols_it = col_cache.find(key);
        if (cols_it == col_cache.end()) {
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_capture",
                .message = "capture column cache missing for catalog table",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = key.first,
                .source_table = key.second,
                .context = {
                    {"binlog_schema", schema},
                    {"binlog_table", table},
                },
            });
            return;
        }
        const std::string op_char = op_char_from_mysql(op);
        if (op_char.empty()) {
            return;
        }
        if (auto cid_it = catalog_id_by_table.find(key); cid_it != catalog_id_by_table.end()) {
            cdc_active_catalog_ids.insert(cid_it->second);
        }
        nlohmann::json before = nullptr;
        nlohmann::json after = nullptr;
        const nlohmann::json row_json = row_dict_from_columns(cols_it->second, col_values);
        if (op == "DELETE") {
            before = row_json;
        } else if (before_col_values != nullptr) {
            before = row_dict_from_columns(cols_it->second, *before_col_values);
            after = row_json;
        } else {
            after = row_json;
        }

        CdcEvent event;
        event.op = op_char;
        event.conn_id = conn_id;
        // Always publish canonical catalog names (apply matches on source_schema/source_table).
        event.schema_name = key.first;
        event.table_name = key.second;
        event.before = before;
        event.after = after;
        event.binlog_file = binlog_start.file;
        event.binlog_pos = event_position > 0 ? event_position : binlog_start.position;
        event.ts_ms = now_ms();
        event.ingestion_ts = utc_iso_timestamp_now();

        const std::string topic = topic_for_catalog(
            rcfg.topic_prefix, key.first, key.second, rcfg.topic_mode, rcfg.topic_buckets);
        const nlohmann::json* row_for_key = (op_char == "d") ? &before : &after;
        const std::string msg_key = kafka_message_key_for_row(
            key.first, key.second, row_for_key, pk_by_table[key]);
        try {
            producer.produce(topic, msg_key, event.to_kafka_dict().dump());
        } catch (const std::exception& ex) {
            log_write(log_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_capture",
                .message = "capture row skipped: kafka publish failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = key.first,
                .source_table = key.second,
                .context = {
                    {"error", ex.what()},
                    {"topic", topic},
                    {"binlog_schema", schema},
                    {"binlog_table", table},
                },
            });
            return;
        }
        if (!first_kafka_publish_logged) {
            first_kafka_publish_logged = true;
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_capture",
                .message = "capture kafka first event published",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = key.first,
                .source_table = key.second,
                .context = {
                    {"topic", topic},
                    {"op", op_char},
                    {"topic_prefix", rcfg.topic_prefix},
                    {"kafka_bootstrap", rcfg.bootstrap},
                },
            });
        }
    };

    const auto slice_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(std::max(1, rcfg.max_seconds));
    auto last_heartbeat = std::chrono::steady_clock::now() - std::chrono::hours(24);
    int lagging_idle_chunks = 0;
    BinlogCliStats read_stats;
    read_stats.last_file = binlog_start.file;
    read_stats.last_position = binlog_start.position;

    {
        const MasterBinlogStatus master_start = fetch_master_binlog_status(mariadb.handle);
        int roll_count = 0;
        const BinlogPosition roll_from = binlog_start;
        while (roll_count < 512 && binlog_cursor_is_behind(binlog_start, master_start) &&
               advance_binlog_cursor_to_next_file(mariadb.handle, binlog_start)) {
            ++roll_count;
        }
        if (roll_count > 0) {
            read_stats.last_file = binlog_start.file;
            read_stats.last_position = binlog_start.position;
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_capture",
                .message = "capture binlog rolled forward past EOF files",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"files_advanced", roll_count},
                    {"from_file", roll_from.file},
                    {"from_position", roll_from.position},
                    {"to_file", binlog_start.file},
                    {"to_position", binlog_start.position},
                    {"master_file", master_start.file},
                    {"master_position", master_start.position},
                },
            });
        }
    }

    while (read_stats.events < rcfg.max_events && std::chrono::steady_clock::now() < slice_deadline) {
        const auto remaining_sec = std::chrono::duration_cast<std::chrono::seconds>(
                                       slice_deadline - std::chrono::steady_clock::now())
                                       .count();
        if (remaining_sec <= 0) {
            break;
        }
        const BinlogPosition cursor_now{binlog_start.file, binlog_start.position};
        const MasterBinlogStatus master_now = fetch_master_binlog_status(mariadb.handle);
        const bool lagging = binlog_cursor_is_behind(cursor_now, master_now);
        const int idle_cap = rcfg.idle_poll_seconds > 0 ? rcfg.idle_poll_seconds : 3;
        const int chunk_sec = lagging
                                  ? std::max(1, std::min(static_cast<int>(remaining_sec), 30))
                                  : std::max(1, std::min(idle_cap, static_cast<int>(remaining_sec)));
        const long long events_left = rcfg.max_events - read_stats.events;

        const BinlogPosition chunk_start = binlog_start;
        const BinlogCliStats chunk = read_remote_binlog_cli(
            *source,
            binlog_start,
            chunk_sec,
            static_cast<int>(std::min<long long>(events_left, 2147483647)),
            publish_row,
            [&]() { return std::chrono::steady_clock::now() >= slice_deadline; });

        read_stats.events += chunk.events;
        read_stats.upserts += chunk.upserts;
        read_stats.deletes += chunk.deletes;
        if (!chunk.last_file.empty()) {
            read_stats.last_file = chunk.last_file;
            read_stats.last_position = chunk.last_position;
            binlog_start = BinlogPosition{chunk.last_file, chunk.last_position};
        }

        if (chunk.cli_missing) {
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_capture",
                .message = "capture binlog cli missing",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"stderr", chunk.stderr_tail}},
            });
            throw std::runtime_error("mariadb-binlog not found in PATH");
        }

        if (chunk.events == 0) {
            const MasterBinlogStatus master_chunk = fetch_master_binlog_status(mariadb.handle);
            const BinlogPosition cursor_pos{read_stats.last_file, read_stats.last_position};
            const bool caught_up = binlog_position_caught_up(cursor_pos, master_chunk);
            const bool cli_failed = chunk.exit_code != 0 && !chunk.stderr_tail.empty();
            const bool chunk_stalled = chunk.exit_code == 0 && chunk.last_file == chunk_start.file &&
                                       chunk.last_position == chunk_start.position;

            const BinlogPosition before_advance = binlog_start;
            bool advanced = advance_binlog_cursor_to_next_file(mariadb.handle, binlog_start);
            if (!advanced && chunk_stalled && binlog_cursor_is_behind(cursor_pos, master_chunk) &&
                cursor_pos.file != master_chunk.file) {
                const auto master_before_advance = fetch_master_binlog_status(mariadb.handle);
                if (binlog_cursor_is_behind(cursor_pos, master_before_advance)) {
                    advanced = advance_binlog_to_next_file(mariadb.handle, binlog_start);
                }
            }
            if (advanced) {
                lagging_idle_chunks = 0;
                read_stats.last_file = binlog_start.file;
                read_stats.last_position = binlog_start.position;
                upsert_capture_position(
                    log_pg,
                    conn_id,
                    read_stats.last_file,
                    read_stats.last_position,
                    live_uuid);
                const auto master_after_advance = fetch_master_binlog_status(mariadb.handle);
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_capture",
                    .message = chunk_stalled && !binlog_cursor_at_file_eof(mariadb.handle, before_advance)
                                   ? "capture binlog advanced after idle stall"
                                   : "capture binlog advanced to next file",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"from_file", before_advance.file},
                        {"from_position", before_advance.position},
                        {"to_file", binlog_start.file},
                        {"to_position", binlog_start.position},
                        {"master_file", master_after_advance.file},
                        {"master_position", master_after_advance.position},
                        {"chunk_stalled", chunk_stalled},
                    },
                });
                continue;
            }

            if (lagging && chunk_stalled) {
                lagging_idle_chunks += 1;
                const int quiet_exit = rcfg.quiet_exit_lagging_chunks > 0 ? rcfg.quiet_exit_lagging_chunks : 3;
                if (lagging_idle_chunks >= quiet_exit) {
                    log_write(log_pg, {
                        .level = LogLevel::Info,
                        .component = "cdc_kafka_capture",
                        .message = "capture slice exiting after lagging idle chunks",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = std::nullopt,
                        .source_table = std::nullopt,
                        .context = {
                            {"lagging_idle_chunks", lagging_idle_chunks},
                            {"quiet_exit_lagging_chunks", quiet_exit},
                            {"binlog_file", read_stats.last_file},
                            {"binlog_position", read_stats.last_position},
                        },
                    });
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            } else {
                lagging_idle_chunks = 0;
            }

            log_write(log_pg, {
                .level = cli_failed ? LogLevel::Error : LogLevel::Info,
                .component = "cdc_kafka_capture",
                .message = cli_failed ? "capture binlog cli failed"
                                     : (caught_up ? "capture binlog caught up with master"
                                                  : (lagging ? "capture binlog draining lag"
                                                             : "capture binlog idle")),
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"binlog_file", read_stats.last_file},
                    {"binlog_position", read_stats.last_position},
                    {"events_total", read_stats.events},
                    {"cli_exit_code", chunk.exit_code},
                    {"master_file", master_chunk.file},
                    {"master_position", master_chunk.position},
                    {"caught_up", caught_up},
                    {"lagging", lagging},
                    {"stderr", chunk.stderr_tail.substr(0, 500)},
                },
            });
            if (cli_failed) {
                const std::string cli_err = chunk.stderr_tail.substr(0, 2000);
                if (is_mariadb_binlog_purged_error(cli_err)) {
                    const auto reboot = reboot_conn_after_mariadb_binlog_gap(log_pg, mariadb.handle, conn_id, batch_id);
                    rollback_cdc_in_progress_ids(log_pg, cdc_active_catalog_ids);
                    stats.errors = reboot.ran ? 0 : 1;
                    return stats;
                }
                stats.errors = 1;
                throw std::runtime_error(
                    "mariadb-binlog failed (exit " + std::to_string(chunk.exit_code) + "): " +
                    cli_err.substr(0, 300));
            }
            if (caught_up) {
                const auto now = std::chrono::steady_clock::now();
                if (rcfg.heartbeat_seconds > 0 &&
                    std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >=
                        rcfg.heartbeat_seconds) {
                    bump_mariadb_capture_heartbeat(mariadb.handle);
                    bump_capture_heartbeat_pg(log_pg, conn_id, "mariadb");
                    last_heartbeat = now;
                }
                break;
            }
            if (lagging || binlog_cursor_is_behind(cursor_pos, master_chunk)) {
                continue;
            }
            const auto now = std::chrono::steady_clock::now();
            if (rcfg.heartbeat_seconds > 0 &&
                std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >=
                    rcfg.heartbeat_seconds) {
                bump_mariadb_capture_heartbeat(mariadb.handle);
                bump_capture_heartbeat_pg(log_pg, conn_id, "mariadb");
                last_heartbeat = now;
            }
            break;
        }
        if (chunk.events > 0) {
            lagging_idle_chunks = 0;
        }
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_capture",
            .message = "capture binlog chunk processed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"chunk_events", chunk.events},
                {"chunk_upserts", chunk.upserts},
                {"chunk_deletes", chunk.deletes},
                {"binlog_file", read_stats.last_file},
                {"binlog_position", read_stats.last_position},
                {"events_total", read_stats.events},
            },
        });
        last_heartbeat = std::chrono::steady_clock::now();
    }

    const int queued = producer.flush(15);
    const KafkaProducerStats pstats = producer.stats();
    if (pstats.errors > 0 || queued > 0 || pstats.pending > 0) {
        const std::string err_detail =
            !pstats.first_error.empty() ? pstats.first_error : std::to_string(queued) + " messages still queued";
        upsert_capture_position(
            log_pg,
            conn_id,
            read_stats.last_file,
            read_stats.last_position,
            live_uuid,
            "failed",
            "kafka publish failed: " + err_detail.substr(0, 2000));
        rollback_cdc_in_progress_ids(log_pg, cdc_active_catalog_ids);
        throw std::runtime_error("kafka publish failed: " + err_detail);
    }

    for (long long catalog_id : cdc_active_catalog_ids) {
        mark_catalog_cdc_success(log_pg, catalog_id);
    }
    const MasterBinlogStatus master_end = fetch_master_binlog_status(mariadb.handle);
    const BinlogPosition cursor_end{read_stats.last_file, read_stats.last_position};
    const int capture_lag_sec =
        binlog_position_caught_up(cursor_end, master_end) ? 0 : 1;
    upsert_capture_position(
        log_pg,
        conn_id,
        read_stats.last_file,
        read_stats.last_position,
        live_uuid,
        "healthy",
        "",
        capture_lag_sec);

    stats.events_published = pstats.events_published;
    stats.errors = pstats.errors;
    stats.binlog_file = read_stats.last_file;
    stats.binlog_position = read_stats.last_position;
    stats.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();

    if (watched_binlog_events > 0 && pstats.events_published == 0) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_kafka_capture",
            .message = "capture watched-table binlog events read but none published to kafka",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"watched_binlog_events", watched_binlog_events},
                {"binlog_events_read", read_stats.events},
                {"events_published", pstats.events_published},
                {"schema_mismatch_events", schema_mismatch_events},
                {"catalog_tables", catalog_tables_json},
                {"kafka_bootstrap", rcfg.bootstrap},
            },
        });
    } else if (read_stats.events > 0 && watched_binlog_events == 0 && pstats.events_published == 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "cdc_kafka_capture",
            .message = "capture binlog activity on unwatched tables only",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"binlog_events_read", read_stats.events},
                {"unwatched_binlog_events", unwatched_binlog_events},
                {"schema_mismatch_events", schema_mismatch_events},
                {"catalog_tables", catalog_tables_json},
            },
        });
    }

    nlohmann::json mismatch_samples = nlohmann::json::array();
    for (const auto& [binlog_schema, binlog_table] : schema_mismatch_samples) {
        mismatch_samples.push_back({
            {"binlog_schema", binlog_schema},
            {"binlog_table", binlog_table},
        });
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_capture",
        .message = "capture slice completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"events_published", stats.events_published},
            {"binlog_events_read", read_stats.events},
            {"watched_binlog_events", watched_binlog_events},
            {"unwatched_binlog_events", unwatched_binlog_events},
            {"schema_mismatch_events", schema_mismatch_events},
            {"case_fold_resolved_events", case_fold_resolved_events},
            {"schema_mismatch_samples", mismatch_samples},
            {"catalog_tables", catalog_tables_json},
            {"ddl_recorded", stats.ddl_recorded},
            {"binlog_file", stats.binlog_file},
            {"binlog_position", stats.binlog_position},
            {"duration_ms", stats.duration_ms},
            {"kafka_bootstrap", rcfg.bootstrap},
            {"topic_prefix", rcfg.topic_prefix},
            {"tables_watched", static_cast<int>(wanted.size())},
        },
    });

    return stats;
    } catch (const std::exception& ex) {
        if (is_mariadb_binlog_purged_error(ex.what())) {
            MariaDbConn recovery_conn(*source);
            const auto reboot =
                reboot_conn_after_mariadb_binlog_gap(log_pg, recovery_conn.handle, conn_id, batch_id);
            rollback_cdc_in_progress_ids(log_pg, cdc_active_catalog_ids);
            stats.errors = reboot.ran ? 0 : 1;
            log_write(log_pg, {
                .level = reboot.ran ? LogLevel::Warning : LogLevel::Error,
                .component = "cdc_kafka_capture",
                .message = reboot.ran ? "capture binlog gap reboot completed"
                                      : "capture binlog gap reboot failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {
                    {"error", ex.what()},
                    {"t0_reset", reboot.t0_reset},
                    {"tables_flagged", reboot.tables_flagged},
                },
            });
            return stats;
        }
        rollback_cdc_in_progress_ids(log_pg, cdc_active_catalog_ids);
        mark_capture_position_failed(log_pg, conn_id, ex.what());
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_capture",
            .message = "capture slice failed; cdc_in_progress rolled back",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"error", ex.what()},
                {"tables_touched", static_cast<int>(cdc_active_catalog_ids.size())},
            },
        });
        throw;
    }
}
