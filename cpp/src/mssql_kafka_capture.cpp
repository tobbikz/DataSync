#include "mssql_kafka_capture.hpp"

#include "capture_common.hpp"
#include "cdc_envelope.hpp"
#include "kafka_producer.hpp"
#include "kafka_topics.hpp"
#include "mssql_conn.hpp"
#include "mssql_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <chrono>
#include <iomanip>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>

#ifndef HAVE_FREETDS

MssqlCaptureStats run_mssql_kafka_capture_slice(
    const AppConfig&,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>&,
    const std::string& batch_id,
    int,
    int) {
    log_write(log_pg, {
        .level = LogLevel::Warning,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql capture skipped: rebuild with FreeTDS",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"hint", "install freetds"}},
    });
    return {};
}

int seed_mssql_cdc_lsn_for_conn(
    const AppConfig&,
    PGconn*,
    const std::string&,
    const std::string&,
    bool) {
    return 0;
}

#else

namespace {

bool validate_capture_instance(const std::string& name) {
    static const std::regex re(R"(^[A-Za-z0-9_]+$)");
    return !name.empty() && std::regex_match(name, re);
}

std::vector<uint8_t> lsn_as_bytes(const MssqlCell& cell) {
    if (cell.is_binary) {
        return cell.bytes;
    }
    if (cell.text.empty()) {
        return {};
    }
    return std::vector<uint8_t>(cell.text.begin(), cell.text.end());
}

std::string lsn_hex(const std::vector<uint8_t>& lsn) {
    std::ostringstream oss;
    for (auto b : lsn) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    }
    return oss.str();
}

std::string lsn_sql_literal(const std::vector<uint8_t>& lsn) {
    return "0x" + lsn_hex(lsn);
}

int lsn_compare(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    auto normalize = [](std::vector<uint8_t> lsn) {
        constexpr std::size_t kLsnLen = 10;
        if (lsn.size() > kLsnLen) {
            lsn.erase(lsn.begin(), lsn.end() - kLsnLen);
        }
        if (lsn.size() < kLsnLen) {
            lsn.insert(lsn.begin(), kLsnLen - lsn.size(), 0);
        }
        return lsn;
    };
    const auto left = normalize(a);
    const auto right = normalize(b);
    if (left == right) {
        return 0;
    }
    return left > right ? 1 : -1;
}

std::vector<uint8_t> get_stored_lsn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table) {
    const char* vals[] = {conn_id.c_str(), database.c_str(), schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT last_start_lsn FROM cdc_catalog.cdc_mssql_lsn
        WHERE conn_id = $1 AND database = $2 AND schema_name = $3 AND table_name = $4
        )",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        if (res) {
            PQclear(res);
        }
        return {};
    }
    const char* data = PQgetvalue(res, 0, 0);
    const int len = PQgetlength(res, 0, 0);
    std::vector<uint8_t> out = pg_bytea_to_bytes(data, len);
    PQclear(res);
    return out;
}

void upsert_lsn(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table,
    const std::vector<uint8_t>& start_lsn,
    const std::vector<uint8_t>& seqval) {
    const std::string start_hex = lsn_hex(start_lsn);
    const std::string seq_hex = seqval.empty() ? "" : lsn_hex(seqval);
    const char* vals[] = {
        conn_id.c_str(),
        database.c_str(),
        schema.c_str(),
        table.c_str(),
        start_hex.c_str(),
        seq_hex.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.cdc_mssql_lsn
            (conn_id, database, schema_name, table_name, last_start_lsn, last_seqval, updated_at)
        VALUES ($1, $2, $3, $4, decode($5, 'hex'), decode(NULLIF($6, ''), 'hex'), now())
        ON CONFLICT (conn_id, database, schema_name, table_name) DO UPDATE SET
            last_start_lsn = EXCLUDED.last_start_lsn,
            last_seqval = EXCLUDED.last_seqval,
            updated_at = now()
        )",
        6,
        vals);
}

std::optional<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> resolve_lsn_window(
    MssqlConn& mssql,
    const std::string& database,
    const std::string& capture_instance,
    const std::vector<uint8_t>& from_lsn) {
    if (from_lsn.empty() || from_lsn == std::vector<uint8_t>(from_lsn.size(), 0)) {
        return std::nullopt;
    }
    mssql.use_database(database);
    const auto min_rows = mssql.query("SELECT sys.fn_cdc_get_min_lsn('" + capture_instance + "')");
    const auto max_rows = mssql.query("SELECT sys.fn_cdc_get_max_lsn()");
    if (min_rows.rows.empty() || max_rows.rows.empty() || min_rows.rows[0].empty() || max_rows.rows[0].empty()) {
        return std::nullopt;
    }
    const auto min_lsn = lsn_as_bytes(min_rows.rows[0][0]);
    const auto to_lsn = lsn_as_bytes(max_rows.rows[0][0]);
    if (min_lsn.empty() || to_lsn.empty() || to_lsn == std::vector<uint8_t>(to_lsn.size(), 0)) {
        return std::nullopt;
    }
    if (lsn_compare(from_lsn, min_lsn) < 0 || lsn_compare(from_lsn, to_lsn) >= 0) {
        return std::nullopt;
    }
    return std::make_pair(from_lsn, to_lsn);
}

std::optional<std::vector<uint8_t>> fetch_max_lsn(MssqlConn& mssql, const std::string& database) {
    mssql.use_database(database);
    const auto max_rows = mssql.query("SELECT sys.fn_cdc_get_max_lsn()");
    if (max_rows.rows.empty() || max_rows.rows[0].empty()) {
        return std::nullopt;
    }
    const auto max_lsn = lsn_as_bytes(max_rows.rows[0][0]);
    if (max_lsn.empty()) {
        return std::nullopt;
    }
    return max_lsn;
}

std::optional<std::vector<uint8_t>> fetch_min_lsn(
    MssqlConn& mssql,
    const std::string& database,
    const std::string& capture_instance) {
    mssql.use_database(database);
    const auto min_rows = mssql.query("SELECT sys.fn_cdc_get_min_lsn('" + capture_instance + "')");
    if (min_rows.rows.empty() || min_rows.rows[0].empty()) {
        return std::nullopt;
    }
    const auto min_lsn = lsn_as_bytes(min_rows.rows[0][0]);
    if (min_lsn.empty()) {
        return std::nullopt;
    }
    return min_lsn;
}

/** When stored LSN is before CDC retention min, rewind to min and log gap_detected (no full-load). */
bool recover_purged_lsn(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const CaptureCatalogTable& tbl,
    const std::string& capture_instance,
    const std::vector<uint8_t>& from_lsn,
    const std::string& batch_id) {
    const auto min_lsn = fetch_min_lsn(mssql, tbl.source_database, capture_instance);
    if (!min_lsn || lsn_compare(from_lsn, *min_lsn) >= 0) {
        return false;
    }
    upsert_lsn(log_pg, conn_id, tbl.source_database, tbl.source_schema, tbl.source_table, *min_lsn, {});
    log_write(log_pg, {
        .level = LogLevel::Warning,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql lsn purged; rewound to min_lsn",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = tbl.source_schema,
        .source_table = tbl.source_table,
        .context = {
            {"stop_reason", "lsn_purged"},
            {"from_lsn", lsn_hex(from_lsn)},
            {"min_lsn", lsn_hex(*min_lsn)},
            {"capture_instance", capture_instance},
        },
    });
    return true;
}

/** Keep idle cursor at the current CDC tip so retention cleanup cannot outrun it. */
bool bump_lsn_to_max(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const CaptureCatalogTable& tbl,
    const std::vector<uint8_t>& from_lsn,
    const std::string& batch_id,
    const char* reason) {
    const auto max_lsn = fetch_max_lsn(mssql, tbl.source_database);
    if (!max_lsn) {
        return false;
    }
    upsert_lsn(log_pg, conn_id, tbl.source_database, tbl.source_schema, tbl.source_table, *max_lsn, {});
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql capture lsn advanced to max_lsn",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = tbl.source_schema,
        .source_table = tbl.source_table,
        .context = {
            {"stop_reason", reason},
            {"from_lsn", from_lsn.empty() ? "" : lsn_hex(from_lsn)},
            {"max_lsn", lsn_hex(*max_lsn)},
        },
    });
    return true;
}

void cdc_scan(MssqlConn& mssql, const std::string& database) {
    mssql.use_database(database);
    mssql.query("SET NOCOUNT ON; EXECUTE sys.sp_cdc_scan @maxtrans = 500, @maxscans = 10, @continuous = 0");
}

std::string mssql_op_char(int op_code) {
    if (op_code == 1) {
        return "d";
    }
    if (op_code == 2) {
        return "c";
    }
    if (op_code == 4) {
        return "u";
    }
    return {};
}

nlohmann::json serialize_mssql_value(const MssqlCell& cell) {
    if (cell.is_binary) {
        return lsn_hex(cell.bytes);
    }
    return cell.text;
}

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

MssqlCaptureStats run_mssql_kafka_capture_slice(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    int worker_id,
    int worker_count) {
    MssqlCaptureStats stats;
    stats.batch_id = batch_id;
    const auto start = std::chrono::steady_clock::now();

    const MssqlSource* source = find_mssql_source(cfg, conn_id);
    if (!source) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_mssql_capture",
            .message = "mssql connect failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"reason", "source not found in config"}},
        });
        throw std::runtime_error("MSSQL source not found: " + conn_id);
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const CaptureRuntimeConfig rcfg =
        load_mssql_capture_runtime(runtime, log_pg, conn_id, &cfg.cdc);
    const KafkaBootstrapResolved kafka = resolve_kafka_bootstrap();
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql capture slice started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"kafka_bootstrap", kafka.bootstrap},
            {"kafka_bootstrap_source", kafka.source},
            {"topic_prefix", rcfg.topic_prefix},
        },
    });
    const auto tables =
        fetch_capture_catalog_tables(log_pg, conn_id, worker_id, worker_count, "mssql");
    clear_stale_cdc_in_progress(log_pg, conn_id, "mssql");
    if (tables.empty()) {
        const int pending_full_load = count_full_load_pending(log_pg, conn_id, "mssql");
        if (pending_full_load > 0) {
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_mssql_capture",
                .message = "mssql capture idle: tables awaiting initial full load (CDC starts after first COPY)",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = std::nullopt,
                .source_table = std::nullopt,
                .context = {{"pending_full_load", pending_full_load}},
            });
        } else {
            log_cdc_skip_no_tables(
                log_pg, "cdc_kafka_mssql_capture", "capture", batch_id, conn_id, "mssql");
        }
        return stats;
    }

    std::vector<std::pair<std::string, std::string>> table_pairs;
    std::set<std::pair<std::string, std::string>> hot_tables;
    table_pairs.reserve(tables.size());
    for (const auto& tbl : tables) {
        table_pairs.emplace_back(tbl.lake_schema, tbl.lake_table);
        if (tbl.hot) {
            hot_tables.emplace(tbl.lake_schema, tbl.lake_table);
        }
    }
    ensure_capture_kafka_topics(
        log_pg, "cdc_kafka_mssql_capture", batch_id, conn_id, rcfg, table_pairs, hot_tables);

    KafkaProducer producer(rcfg.bootstrap, rcfg.linger_ms, rcfg.producer_batch);
    if (!producer.available()) {
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_kafka_mssql_capture",
            .message = "mssql capture kafka producer unavailable",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"kafka_bootstrap", rcfg.bootstrap}},
        });
        throw std::runtime_error("Kafka producer unavailable for MSSQL capture");
    }

    MssqlConn mssql(*source);
    std::set<std::string> scanned_dbs;
    int published = 0;
    struct MssqlPendingCommit {
        long long catalog_id{0};
        std::string source_database;
        std::string source_schema;
        std::string source_table;
        std::vector<uint8_t> next_lsn;
        std::vector<uint8_t> last_sq;
        bool has_lsn{false};
    };
    std::vector<MssqlPendingCommit> pending_commits;
    int cdc_rows_read = 0;
    int skipped_no_lsn = 0;
    int skipped_no_window = 0;
    int skipped_bad_capture = 0;
    int lsn_recovered = 0;
    int lsn_auto_seeded = 0;
    int lsn_bumped_to_max = 0;
    std::set<long long> cdc_active_catalog_ids;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(rcfg.max_seconds);
    auto last_heartbeat = std::chrono::steady_clock::now() - std::chrono::hours(24);

    for (const auto& tbl : tables) {
        if (published >= rcfg.max_events || std::chrono::steady_clock::now() >= deadline) {
            break;
        }
        if (!scanned_dbs.count(tbl.source_database)) {
            scanned_dbs.insert(tbl.source_database);
        }
        cdc_scan(mssql, tbl.source_database);

        const std::string cap = tbl.engine_meta.value("capture_instance", "");
        if (!validate_capture_instance(cap)) {
            skipped_bad_capture += 1;
            continue;
        }

        auto from_lsn = get_stored_lsn(log_pg, conn_id, tbl.source_database, tbl.source_schema, tbl.source_table);
        if (!from_lsn.empty()) {
            const auto min_lsn_check = fetch_min_lsn(mssql, tbl.source_database, cap);
            if (min_lsn_check && lsn_compare(from_lsn, *min_lsn_check) < 0) {
                if (recover_purged_lsn(log_pg, mssql, conn_id, tbl, cap, from_lsn, batch_id)) {
                    from_lsn = *min_lsn_check;
                    lsn_recovered += 1;
                }
            }
        }
        if (from_lsn.empty()) {
            const auto min_lsn = fetch_min_lsn(mssql, tbl.source_database, cap);
            if (!min_lsn) {
                skipped_no_lsn += 1;
                continue;
            }
            upsert_lsn(log_pg, conn_id, tbl.source_database, tbl.source_schema, tbl.source_table, *min_lsn, {});
            from_lsn = *min_lsn;
            lsn_auto_seeded += 1;
            log_write(log_pg, {
                .level = LogLevel::Info,
                .component = "cdc_kafka_mssql_capture",
                .message = "mssql capture lsn auto-seeded from min_lsn",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = tbl.source_schema,
                .source_table = tbl.source_table,
                .context = {{"last_start_lsn", lsn_hex(from_lsn)}},
            });
        }

        auto window = resolve_lsn_window(mssql, tbl.source_database, cap, from_lsn);
        if (!window) {
            if (bump_lsn_to_max(log_pg, mssql, conn_id, tbl, from_lsn, batch_id, "idle_no_window")) {
                mark_catalog_cdc_success(log_pg, tbl.catalog_id);
                lsn_bumped_to_max += 1;
            } else {
                skipped_no_window += 1;
            }
            if (rcfg.idle_poll_seconds > 0 && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::seconds(rcfg.idle_poll_seconds));
            }
            continue;
        }

        mssql.use_database(tbl.source_database);
        mark_catalog_cdc_in_progress(log_pg, tbl.catalog_id);
        cdc_active_catalog_ids.insert(tbl.catalog_id);
        const std::string fn = "cdc.fn_cdc_get_all_changes_" + cap;
        const std::string q = "SELECT * FROM " + fn + "(" + lsn_sql_literal(window->first) + ", " +
                              lsn_sql_literal(window->second) + ", N'all update old')";
        const auto result = mssql.query(q);
        cdc_rows_read += static_cast<int>(result.rows.size());
        if (result.rows.empty()) {
            pending_commits.push_back(
                {tbl.catalog_id,
                 tbl.source_database,
                 tbl.source_schema,
                 tbl.source_table,
                 window->second,
                 {},
                 true});
            if (rcfg.idle_poll_seconds > 0 && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::seconds(rcfg.idle_poll_seconds));
            }
            continue;
        }

        const auto& col_names = result.columns;
        std::vector<uint8_t> last_sl;
        std::vector<uint8_t> last_sq;

        for (const auto& raw : result.rows) {
            if (published >= rcfg.max_events || std::chrono::steady_clock::now() >= deadline) {
                break;
            }
            nlohmann::json data = nlohmann::json::object();
            int op_code = 0;
            std::vector<uint8_t> sl;
            std::vector<uint8_t> sq;
            for (std::size_t i = 0; i < raw.size() && i < col_names.size(); ++i) {
                const std::string& col = col_names[i];
                if (col.rfind("__$", 0) == 0) {
                    if (col == "__$operation") {
                        op_code = raw[i].text.empty() ? 0 : std::stoi(raw[i].text);
                    } else if (col == "__$start_lsn") {
                        sl = lsn_as_bytes(raw[i]);
                    } else if (col == "__$seqval") {
                        sq = lsn_as_bytes(raw[i]);
                    }
                    continue;
                }
                data[col] = serialize_mssql_value(raw[i]);
            }
            if (op_code == 3) {
                continue;
            }
            const std::string op = mssql_op_char(op_code);
            if (op.empty()) {
                continue;
            }
            if (!sl.empty()) {
                last_sl = sl;
                last_sq = sq;
            }

            nlohmann::json before = nullptr;
            nlohmann::json after = nullptr;
            if (op == "d") {
                before = data;
            } else {
                after = data;
            }

            CdcEvent event;
            event.op = op;
            event.conn_id = conn_id;
            event.db_engine = "mssql";
            event.source_database = tbl.source_database;
            event.schema_name = tbl.source_schema;
            event.table_name = tbl.source_table;
            event.before = before;
            event.after = after;
            event.gtid = sl.empty() ? "" : lsn_hex(sl);
            event.mssql_seqval = sq.empty() ? "" : lsn_hex(sq);
            event.ts_ms = now_ms();
            event.ingestion_ts = utc_iso_timestamp_now();

            const std::string topic = topic_for_catalog_table(
                rcfg.topic_prefix, tbl.lake_schema, tbl.lake_table, rcfg.topic_mode, rcfg.topic_buckets, tbl.hot);
            const nlohmann::json* row_for_key = (op == "d") ? &before : &after;
            const std::string msg_key = kafka_message_key_for_row(
                tbl.lake_schema, tbl.lake_table, row_for_key, tbl.pk_columns);
            const int kafka_partition = kafka_produce_partition(
                tbl.catalog_id, msg_key, rcfg.topic_partitions, tbl.hot);
            try {
                producer.produce(topic, msg_key, event.to_kafka_dict().dump(), kafka_partition);
            } catch (const std::exception& ex) {
                log_write(log_pg, {
                    .level = LogLevel::Error,
                    .component = "cdc_kafka_mssql_capture",
                    .message = "mssql capture row skipped: kafka produce failed",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = tbl.lake_schema,
                    .source_table = tbl.lake_table,
                    .context = {{"error", ex.what()}, {"topic", topic}, {"op", op}},
                });
                continue;
            }
            published += 1;
            last_heartbeat = std::chrono::steady_clock::now();
        }

        MssqlPendingCommit pending;
        pending.catalog_id = tbl.catalog_id;
        pending.source_database = tbl.source_database;
        pending.source_schema = tbl.source_schema;
        pending.source_table = tbl.source_table;
        if (!last_sl.empty()) {
            const auto inc_rows =
                mssql.query("SELECT sys.fn_cdc_increment_lsn(" + lsn_sql_literal(last_sl) + ")");
            pending.next_lsn = last_sl;
            if (!inc_rows.rows.empty() && !inc_rows.rows[0].empty()) {
                const auto inc = lsn_as_bytes(inc_rows.rows[0][0]);
                if (!inc.empty()) {
                    pending.next_lsn = inc;
                }
            }
            pending.last_sq = last_sq;
            pending.has_lsn = true;
        }
        pending_commits.push_back(std::move(pending));
    }

    if (published == 0 && rcfg.heartbeat_seconds > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >= rcfg.heartbeat_seconds) {
            bump_capture_heartbeat_pg(log_pg, conn_id, "mssql");
            last_heartbeat = now;
        }
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
        if (p.has_lsn) {
            upsert_lsn(
                log_pg,
                conn_id,
                p.source_database,
                p.source_schema,
                p.source_table,
                p.next_lsn,
                p.last_sq);
        }
        mark_catalog_cdc_success(log_pg, p.catalog_id);
    }
    stats.tables = static_cast<int>(pending_commits.size());

    stats.events_published = pstats.events_published;
    stats.errors = pstats.errors;
    stats.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();

    if (cdc_rows_read > 0 && pstats.events_published == 0) {
        log_write(log_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_kafka_mssql_capture",
            .message = "capture cdc rows read but none published to kafka",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"cdc_rows_read", cdc_rows_read},
                {"events_published", pstats.events_published},
                {"kafka_bootstrap", rcfg.bootstrap},
                {"catalog_tables", static_cast<int>(tables.size())},
            },
        });
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "cdc_kafka_mssql_capture",
        .message = "mssql capture slice completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"events_published", stats.events_published},
            {"cdc_rows_read", cdc_rows_read},
            {"tables", stats.tables},
            {"catalog_tables", static_cast<int>(tables.size())},
            {"skipped_no_lsn", skipped_no_lsn},
            {"skipped_no_window", skipped_no_window},
            {"skipped_bad_capture", skipped_bad_capture},
            {"lsn_auto_seeded", lsn_auto_seeded},
            {"lsn_recovered", lsn_recovered},
            {"lsn_bumped_to_max", lsn_bumped_to_max},
            {"duration_ms", stats.duration_ms},
        },
    });

    return stats;
}

bool seed_mssql_cdc_lsn_t0_for_table(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table) {
    const char* vals[] = {conn_id.c_str(), database.c_str(), schema.c_str(), table.c_str()};
    PGresult* meta_res = PQexecParams(
        log_pg,
        R"(
        SELECT engine_meta::text FROM cdc_catalog.catalog
        WHERE conn_id = $1 AND source_database = $2 AND source_schema = $3 AND source_table = $4
          AND db_engine = 'mssql'
        LIMIT 1
        )",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!meta_res || PQresultStatus(meta_res) != PGRES_TUPLES_OK || PQntuples(meta_res) == 0) {
        if (meta_res) {
            PQclear(meta_res);
        }
        return false;
    }
    const char* meta_txt = PQgetvalue(meta_res, 0, 0);
    nlohmann::json meta = nlohmann::json::object();
    if (meta_txt && *meta_txt) {
        meta = nlohmann::json::parse(meta_txt, nullptr, false);
        if (meta.is_discarded()) {
            meta = nlohmann::json::object();
        }
    }
    PQclear(meta_res);

    const std::string cap = meta.value("capture_instance", "");
    if (!validate_capture_instance(cap)) {
        return false;
    }

    cdc_scan(mssql, database);
    mssql.use_database(database);
    const auto max_lsn = fetch_max_lsn(mssql, database);
    if (!max_lsn) {
        return false;
    }
    upsert_lsn(log_pg, conn_id, database, schema, table, *max_lsn, {});
    return true;
}

bool seed_mssql_cdc_lsn_for_table_if_absent(
    PGconn* log_pg,
    MssqlConn& mssql,
    const std::string& conn_id,
    const std::string& database,
    const std::string& schema,
    const std::string& table) {
    if (!get_stored_lsn(log_pg, conn_id, database, schema, table).empty()) {
        return false;
    }
    return seed_mssql_cdc_lsn_t0_for_table(log_pg, mssql, conn_id, database, schema, table);
}

int seed_mssql_cdc_lsn_for_conn(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    bool force_t0) {
    const MssqlSource* source = find_mssql_source(cfg, conn_id);
    if (!source) {
        return 0;
    }

    std::string sql = R"(
        SELECT source_database, source_schema, source_table, engine_meta::text
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = 'mssql'
          AND active = true
          AND cdc_enabled = true
    )";
    std::vector<const char*> vals = {conn_id.c_str()};

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
            .message = "mssql lsn seed skipped: catalog query failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
        });
        return 1;
    }

    int seeded = 0;
    int skipped = 0;
    int errors = 0;

    try {
        MssqlConn mssql(*source);
        std::set<std::string> scanned_dbs;

        for (int i = 0; i < PQntuples(res); ++i) {
            const std::string database = PQgetvalue(res, i, 0);
            const std::string schema = PQgetvalue(res, i, 1);
            const std::string table = PQgetvalue(res, i, 2);
            const char* meta_txt = PQgetvalue(res, i, 3);
            nlohmann::json meta = nlohmann::json::object();
            if (meta_txt && *meta_txt) {
                meta = nlohmann::json::parse(meta_txt, nullptr, false);
                if (meta.is_discarded()) {
                    meta = nlohmann::json::object();
                }
            }
            const std::string cap = meta.value("capture_instance", "");
            if (!validate_capture_instance(cap)) {
                skipped += 1;
                continue;
            }

            if (!force_t0 && !get_stored_lsn(log_pg, conn_id, database, schema, table).empty()) {
                skipped += 1;
                continue;
            }

            if (!scanned_dbs.count(database)) {
                cdc_scan(mssql, database);
                scanned_dbs.insert(database);
            }
            if (seed_mssql_cdc_lsn_t0_for_table(log_pg, mssql, conn_id, database, schema, table)) {
                seeded += 1;
            } else {
                skipped += 1;
            }
        }
    } catch (const std::exception& ex) {
        errors += 1;
        log_write(log_pg, {
            .level = LogLevel::Error,
            .component = "cdc_catalog_onboard",
            .message = "mssql lsn seed failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .context = {{"error", ex.what()}},
        });
    }

    PQclear(res);

    log_write(log_pg, {
        .level = errors ? LogLevel::Warning : LogLevel::Info,
        .component = "cdc_catalog_onboard",
        .message = errors ? "mssql lsn seed completed with errors" : "mssql lsn seed completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .context = {{"seeded", seeded}, {"skipped", skipped}, {"errors", errors}, {"force_t0", force_t0}},
    });
    return errors;
}

#endif
