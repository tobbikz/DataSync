#include "cdc_reconcile.hpp"

#include "capture_common.hpp"
#include "config.hpp"
#include "kafka_topics.hpp"
#ifdef HAVE_RDKAFKA
#include "kafka_lag.hpp"
#endif
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mongo_lake.hpp"
#include "mssql_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"
#include "pipeline_defaults.hpp"

#ifdef HAVE_FREETDS
#include "mssql_conn.hpp"
#endif
#ifdef HAVE_MONGOC
#include "mongo_conn.hpp"
#endif

#include <openssl/evp.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <iomanip>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_reconcile_shutdown{false};

void on_reconcile_signal(int) {
    g_reconcile_shutdown.store(true);
}

struct ReconcileRuntime {
    int row_abs_tolerance{50};
    double row_pct_tolerance{0.01};
    int row_warn_abs_tolerance{10};
    double row_warn_pct_tolerance{0.005};
    long long large_table_min_rows{100000};
    long long large_table_abs_cap{5000};
    int apply_lag_warn_seconds{600};
    int apply_lag_fail_seconds{1800};
    int pk_sample_size{100};
    int max_tables{0};
    bool enabled{true};
    int interval_hours{4};
    long long kafka_lag_warn{100};
    long long kafka_lag_fail{10000};
    int capture_lag_warn_seconds{300};
    int capture_lag_fail_seconds{900};
    int apply_inactive_seconds{3600};
};

ReconcileRuntime load_reconcile_runtime(RuntimeConfig& runtime, PGconn* pg, const std::string& conn_id) {
    runtime.reload(pg);
    ReconcileRuntime cfg;
    cfg.row_abs_tolerance = pipeline_defaults::kReconcileRowAbsTolerance;
    cfg.row_pct_tolerance = pipeline_defaults::kReconcileRowPctTolerance;
    cfg.row_warn_abs_tolerance = pipeline_defaults::kReconcileRowWarnAbsTolerance;
    cfg.row_warn_pct_tolerance = pipeline_defaults::kReconcileRowWarnPctTolerance;
    cfg.large_table_min_rows = pipeline_defaults::kReconcileLargeTableMinRows;
    cfg.large_table_abs_cap = pipeline_defaults::kReconcileLargeTableAbsCap;
    cfg.apply_lag_warn_seconds = pipeline_defaults::kReconcileApplyLagWarnSeconds;
    cfg.apply_lag_fail_seconds = pipeline_defaults::kReconcileApplyLagFailSeconds;
    cfg.pk_sample_size = pipeline_defaults::kReconcilePkSampleSize;
    cfg.max_tables = pipeline_defaults::kReconcileMaxTables;
    cfg.enabled = pipeline_defaults::kReconcileEnabled;
    cfg.interval_hours = runtime.get_int(
        "reconcile_interval_hours",
        pipeline_defaults::kReconcileIntervalHoursDefault,
        "cdc_kafka_reconcile",
        conn_id);
    cfg.kafka_lag_warn = pipeline_defaults::kReconcileKafkaLagWarn;
    cfg.kafka_lag_fail = pipeline_defaults::kReconcileKafkaLagFail;
    cfg.capture_lag_warn_seconds = pipeline_defaults::kReconcileCaptureLagWarnSeconds;
    cfg.capture_lag_fail_seconds = pipeline_defaults::kReconcileCaptureLagFailSeconds;
    cfg.apply_inactive_seconds = pipeline_defaults::kApplyInactiveSeconds;
    return cfg;
}

struct CatalogReconcileRow {
    long long catalog_id{0};
    std::string conn_id;
    std::string db_engine;
    std::string source_database;
    std::string source_schema;
    std::string source_table;
    std::string lake_schema;
    std::string lake_table;
    std::string pk_columns;
};

std::string mssql_brack(const std::string& name) {
    std::string out = "[";
    for (char c : name) {
        out += (c == ']') ? "]]" : std::string(1, c);
    }
    out += "]";
    return out;
}

std::string md5_hex(const std::string& payload) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return {};
    }
    if (EVP_DigestInit_ex(ctx, EVP_md5(), nullptr) != 1 ||
        EVP_DigestUpdate(ctx, payload.data(), payload.size()) != 1 ||
        EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
        EVP_MD_CTX_free(ctx);
        return {};
    }
    EVP_MD_CTX_free(ctx);
    std::ostringstream oss;
    for (unsigned int i = 0; i < digest_len; ++i) {
        oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    return oss.str();
}

std::string status_rank(const std::string& a, const std::string& b) {
    auto rank = [](const std::string& s) {
        if (s == "fail") {
            return 3;
        }
        if (s == "warn") {
            return 2;
        }
        if (s == "ok") {
            return 1;
        }
        return 0;
    };
    return rank(a) >= rank(b) ? a : b;
}

std::string eval_row_count_status(long long source_rows, long long lake_rows, const ReconcileRuntime& cfg) {
    if (source_rows < 0 || lake_rows < 0) {
        return "skip";
    }
    const long long delta = source_rows - lake_rows;
    const long long abs_delta = std::llabs(delta);
    const double pct =
        source_rows > 0 ? static_cast<double>(abs_delta) / static_cast<double>(source_rows)
                        : (abs_delta > 0 ? 1.0 : 0.0);

    if (source_rows >= cfg.large_table_min_rows) {
        if (pct > cfg.row_pct_tolerance && abs_delta > cfg.large_table_abs_cap) {
            return "fail";
        }
        if (pct > cfg.row_warn_pct_tolerance && abs_delta > cfg.row_warn_abs_tolerance) {
            return "warn";
        }
        return "ok";
    }

    if (abs_delta > cfg.row_abs_tolerance || pct > cfg.row_pct_tolerance) {
        return "fail";
    }
    if (abs_delta > cfg.row_warn_abs_tolerance || pct > cfg.row_warn_pct_tolerance) {
        return "warn";
    }
    return "ok";
}

std::string classify_row_drift(long long source_rows, long long lake_rows) {
    if (source_rows < 0 || lake_rows < 0) {
        return "unknown";
    }
    const long long delta = source_rows - lake_rows;
    if (delta == 0) {
        return "none";
    }
    if (delta > 0) {
        return "source_ahead";
    }
    return "append_zombie";
}

bool reconcile_failure_needs_full_load(const std::string& drift_kind, const std::string& row_status) {
    if (row_status != "fail") {
        return false;
    }
    return drift_kind == "source_ahead" || drift_kind == "append_zombie";
}

std::string reconcile_failure_message(
    const std::string& drift_kind,
    long long source_rows,
    long long lake_rows,
    const std::string& row_status,
    const std::string& overall_status) {
    std::ostringstream oss;
    oss << "reconcile: " << drift_kind << " row_count_" << row_status
        << " (source=" << source_rows << " lake=" << lake_rows << " delta=" << (source_rows - lake_rows)
        << ", overall=" << overall_status << ")";
    return oss.str();
}

std::string eval_apply_lag_status(int apply_lag_seconds, const ReconcileRuntime& cfg) {
    if (apply_lag_seconds < 0) {
        return "skip";
    }
    if (apply_lag_seconds > cfg.apply_lag_fail_seconds) {
        return "fail";
    }
    if (apply_lag_seconds > cfg.apply_lag_warn_seconds) {
        return "warn";
    }
    return "ok";
}

std::string eval_kafka_lag_status(long long kafka_lag, const ReconcileRuntime& cfg) {
    if (kafka_lag < 0) {
        return "skip";
    }
    if (kafka_lag > cfg.kafka_lag_fail) {
        return "fail";
    }
    if (kafka_lag > cfg.kafka_lag_warn) {
        return "warn";
    }
    return "ok";
}

// Bucketed topics share partition watermarks; quiet tables show inflated lag vs their offset.
// Never let kafka alone drive overall=fail/warn when row counts did not fail.
std::string cap_kafka_for_overall(const std::string& row_status, const std::string& kafka_status) {
    if (kafka_status == "skip") {
        return "skip";
    }
    if (row_status == "skip") {
        return "skip";
    }
    if (row_status != "fail") {
        if (kafka_status == "fail" || kafka_status == "warn") {
            return "ok";
        }
    }
    return kafka_status;
}

// PK sample compares first N rows by PK order; mismatch with matching counts is often type/format drift.
// Never let pk_checksum alone drive overall=fail when row counts did not fail.
std::string cap_pk_checksum_for_overall(const std::string& row_status, bool pk_match) {
    if (pk_match) {
        return "ok";
    }
    if (row_status == "skip" || row_status == "ok") {
        return "ok";
    }
    if (row_status == "warn") {
        return "warn";
    }
    return "fail";
}

std::string eval_capture_lag_status(int capture_lag_seconds, const ReconcileRuntime& cfg) {
    if (capture_lag_seconds < 0) {
        return "skip";
    }
    if (capture_lag_seconds > cfg.capture_lag_fail_seconds) {
        return "fail";
    }
    if (capture_lag_seconds > cfg.capture_lag_warn_seconds) {
        return "warn";
    }
    return "ok";
}

std::vector<CatalogReconcileRow> fetch_reconcile_catalog(
    PGconn* pg,
    const std::string& conn_id,
    int max_tables) {
    std::ostringstream sql;
    sql << R"(
        SELECT catalog_id, conn_id, db_engine::text, source_database, source_schema, source_table,
               pk_columns
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND active = true
          AND cdc_enabled = true
          AND needs_full_load = false
          AND has_pk = true
          AND status NOT IN ('skipped', 'disabled')
        ORDER BY source_schema, source_table
    )";
    if (max_tables > 0) {
        sql << " LIMIT " << max_tables;
    }

    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        sql.str().c_str(),
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<CatalogReconcileRow> rows;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("reconcile catalog query failed");
    }

    for (int i = 0; i < PQntuples(res); ++i) {
        CatalogReconcileRow row;
        row.catalog_id = PQgetisnull(res, i, 0) ? 0 : std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1) ? PQgetvalue(res, i, 1) : "";
        row.db_engine = PQgetvalue(res, i, 2) ? PQgetvalue(res, i, 2) : "";
        row.source_database = PQgetvalue(res, i, 3) ? PQgetvalue(res, i, 3) : "";
        row.source_schema = PQgetvalue(res, i, 4) ? PQgetvalue(res, i, 4) : "";
        row.source_table = PQgetvalue(res, i, 5) ? PQgetvalue(res, i, 5) : "";
        row.pk_columns = PQgetvalue(res, i, 6) ? PQgetvalue(res, i, 6) : "";
        if (row.db_engine == "mongodb") {
            row.source_schema = mongo_catalog_source_schema(row.source_database, row.source_schema);
        }
        if (row.db_engine == "mssql") {
            row.lake_schema = mssql_pg_schema_name(row.source_database, row.source_schema);
            row.lake_table = mssql_pg_table_name(row.source_table);
        } else if (row.db_engine == "mongodb") {
            row.lake_schema = mongo_pg_schema_name(row.source_database);
            row.lake_table = mongo_pg_table_name(row.source_table);
        } else {
            row.lake_schema = row.source_schema;
            row.lake_table = row.source_table;
        }
        rows.push_back(std::move(row));
    }
    PQclear(res);
    return rows;
}

long long pg_table_count(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::bigint
        FROM information_schema.tables t
        WHERE t.table_schema = $1 AND t.table_name = $2
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return -1;
    }
    const char* exists_str = PQgetvalue(res, 0, 0);
    const long long exists = exists_str ? std::atoll(exists_str) : -1;
    PQclear(res);
    if (exists <= 0) {
        return 0;
    }

    const std::string fq = pg_ident(schema) + "." + pg_ident(table);
    PGresult* cnt = PQexec(pg, ("SELECT COUNT(*)::bigint FROM " + fq).c_str());
    if (!cnt || PQresultStatus(cnt) != PGRES_TUPLES_OK || PQntuples(cnt) < 1) {
        if (cnt) {
            PQclear(cnt);
        }
        return -1;
    }
    const char* n_str = PQgetvalue(cnt, 0, 0);
    const long long n = n_str ? std::atoll(n_str) : -1;
    PQclear(cnt);
    return n;
}

long long mariadb_table_count(MYSQL* mysql, const std::string& schema, const std::string& table) {
    std::ostringstream sql;
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    sql << "SELECT COUNT(*) FROM `" << esc_id(schema) << "`.`" << esc_id(table) << "`";
    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        return -1;
    }
    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return -1;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    long long count = row && row[0] ? std::atoll(row[0]) : -1;
    mysql_free_result(res);
    return count;
}

#ifdef HAVE_FREETDS
long long mssql_table_count(MssqlConn& mssql, const std::string& database, const std::string& schema, const std::string& table) {
    mssql.use_database(database);
    const auto result = mssql.query(
        "SELECT COUNT_BIG(*) FROM " + mssql_brack(schema) + "." + mssql_brack(table));
    if (result.rows.empty() || result.rows[0].empty()) {
        return -1;
    }
    return result.rows[0][0].text.empty() ? -1 : std::atoll(result.rows[0][0].text.c_str());
}
#endif

#ifdef HAVE_MONGOC
long long mongo_collection_count(MongoConn& mongo, const std::string& database, const std::string& collection) {
    bson_t reply;
    bson_error_t error;
    bson_init(&reply);
    bson_t* cmd_bson = BCON_NEW("count", BCON_UTF8(collection.c_str()));
    if (!cmd_bson) {
        bson_destroy(&reply);
        return -1;
    }
    const bool ok = mongoc_client_command_simple(
        mongo.client, database.c_str(), cmd_bson, nullptr, &reply, &error);
    bson_destroy(cmd_bson);
    if (!ok) {
        bson_destroy(&reply);
        return -1;
    }
    long long count = -1;
    bson_iter_t iter;
    if (bson_iter_init_find(&iter, &reply, "n") && BSON_ITER_HOLDS_INT64(&iter)) {
        count = bson_iter_int64(&iter);
    } else if (bson_iter_init_find(&iter, &reply, "n") && BSON_ITER_HOLDS_INT32(&iter)) {
        count = bson_iter_int32(&iter);
    }
    bson_destroy(&reply);
    return count;
}
#endif

struct ApplyMeta {
    int apply_lag_seconds{-1};
    int seconds_since_last_apply{-1};
    std::string apply_status;
    std::string kafka_topic;
    int kafka_partition{-1};
    long long kafka_offset{-1};
};

ApplyMeta fetch_apply_meta(PGconn* pg, long long catalog_id) {
    ApplyMeta out;
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            apply_lag_seconds,
            status::text,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            extract(epoch FROM (now() - last_applied_at))::integer
        FROM cdc_catalog.apply_position
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    out.apply_lag_seconds = PQgetisnull(res, 0, 0) ? -1 : std::atoi(PQgetvalue(res, 0, 0));
    out.apply_status = PQgetvalue(res, 0, 1) ? PQgetvalue(res, 0, 1) : "";
    if (PQgetvalue(res, 0, 2) && PQgetvalue(res, 0, 2)[0]) {
        out.kafka_topic = PQgetvalue(res, 0, 2);
    }
    if (PQgetvalue(res, 0, 3) && PQgetvalue(res, 0, 3)[0]) {
        out.kafka_partition = std::atoi(PQgetvalue(res, 0, 3));
    }
    if (PQgetvalue(res, 0, 4) && PQgetvalue(res, 0, 4)[0]) {
        out.kafka_offset = std::atoll(PQgetvalue(res, 0, 4));
    }
    if (PQgetvalue(res, 0, 5) && PQgetvalue(res, 0, 5)[0]) {
        out.seconds_since_last_apply = std::atoi(PQgetvalue(res, 0, 5));
    }
    PQclear(res);
    return out;
}

bool is_apply_inactive_table(const ApplyMeta& meta, int inactive_seconds) {
    if (meta.seconds_since_last_apply >= 0 && meta.seconds_since_last_apply > inactive_seconds) {
        return true;
    }
    return false;
}

int fetch_capture_lag_seconds(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const CatalogReconcileRow& row) {
    if (db_engine == "mssql") {
        const char* vals[] = {
            conn_id.c_str(),
            row.source_database.c_str(),
            row.source_schema.c_str(),
            row.source_table.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            SELECT GREATEST(0, extract(epoch FROM (now() - updated_at))::integer)
            FROM cdc_catalog.cdc_mssql_lsn
            WHERE conn_id = $1
              AND database = $2
              AND schema_name = $3
              AND table_name = $4
            )",
            4,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        int lag = -1;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            lag = std::atoi(PQgetvalue(res, 0, 0));
        }
        if (res) {
            PQclear(res);
        }
        return lag;
    }
    if (db_engine == "mongodb") {
        const char* vals[] = {
            conn_id.c_str(),
            row.source_database.c_str(),
            row.source_table.c_str()};
        PGresult* res = PQexecParams(
            pg,
            R"(
            SELECT GREATEST(0, extract(epoch FROM (now() - updated_at))::integer)
            FROM cdc_catalog.cdc_mongo_resume
            WHERE conn_id = $1 AND database = $2 AND collection = $3
            )",
            3,
            nullptr,
            vals,
            nullptr,
            nullptr,
            0);
        int lag = -1;
        if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
            lag = std::atoi(PQgetvalue(res, 0, 0));
        }
        if (res) {
            PQclear(res);
        }
        return lag;
    }
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COALESCE(capture_lag_seconds, 0)
        FROM cdc_catalog.capture_position
        WHERE conn_id = $1
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int lag = -1;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        lag = std::atoi(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return lag;
}

long long insert_reconcile_run(
    PGconn* pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& reconcile_mode,
    const nlohmann::json& context) {
    const std::string context_json = context.dump();
    const char* vals[] = {batch_id.c_str(), conn_id.c_str(), reconcile_mode.c_str(), context_json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation_run (batch_id, conn_id, status, reconcile_mode, context)
        VALUES ($1, $2, 'running', $3, $4::jsonb)
        RETURNING run_id
        )",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        std::string detail = "reconcile run insert failed";
        if (res) {
            const char* err = PQresultErrorMessage(res);
            if (err && err[0]) {
                detail += ": ";
                detail += err;
            }
            PQclear(res);
        } else if (pg) {
            const char* err = PQerrorMessage(pg);
            if (err && err[0]) {
                detail += ": ";
                detail += err;
            }
        }
        throw std::runtime_error(detail);
    }
    const long long run_id = std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return run_id;
}

void finish_reconcile_run(
    PGconn* pg,
    long long run_id,
    const std::string& status,
    int tables_checked,
    int tables_ok,
    int tables_warn,
    int tables_fail,
    int stale_tables,
    const nlohmann::json& context) {
    const std::string run_id_str = std::to_string(run_id);
    const std::string checked = std::to_string(tables_checked);
    const std::string ok = std::to_string(tables_ok);
    const std::string warn = std::to_string(tables_warn);
    const std::string fail = std::to_string(tables_fail);
    const std::string stale = std::to_string(stale_tables);
    const std::string context_json = context.dump();
    const char* vals[] = {
        status.c_str(),
        checked.c_str(),
        ok.c_str(),
        warn.c_str(),
        fail.c_str(),
        stale.c_str(),
        context_json.c_str(),
        run_id_str.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.reconciliation_run
        SET finished_at = now(),
            status = $1,
            tables_checked = $2::int,
            tables_ok = $3::int,
            tables_warn = $4::int,
            tables_fail = $5::int,
            stale_tables = $6::int,
            context = COALESCE(context, '{}'::jsonb) || $7::jsonb
        WHERE run_id = $8::bigint
        )",
        8,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void insert_reconcile_result(
    PGconn* pg,
    long long run_id,
    const CatalogReconcileRow& row,
    long long source_rows,
    long long lake_rows,
    const std::string& row_count_status,
    const ApplyMeta& apply_meta,
    const std::string& overall_status,
    const nlohmann::json& checks) {
    const std::string run_id_str = std::to_string(run_id);
    const std::string catalog_id_str = std::to_string(row.catalog_id);
    const std::string source_str = source_rows >= 0 ? std::to_string(source_rows) : "";
    const std::string lake_str = lake_rows >= 0 ? std::to_string(lake_rows) : "";
    const std::string delta_str =
        (source_rows >= 0 && lake_rows >= 0) ? std::to_string(source_rows - lake_rows) : "";
    const std::string lag_str =
        apply_meta.apply_lag_seconds >= 0 ? std::to_string(apply_meta.apply_lag_seconds) : "";
    const std::string checks_json = checks.dump();
    const char* vals[] = {
        run_id_str.c_str(),
        catalog_id_str.c_str(),
        row.conn_id.c_str(),
        row.source_schema.c_str(),
        row.source_table.c_str(),
        source_str.empty() ? nullptr : source_str.c_str(),
        lake_str.empty() ? nullptr : lake_str.c_str(),
        delta_str.empty() ? nullptr : delta_str.c_str(),
        row_count_status.c_str(),
        lag_str.empty() ? nullptr : lag_str.c_str(),
        apply_meta.apply_status.empty() ? nullptr : apply_meta.apply_status.c_str(),
        overall_status.c_str(),
        checks_json.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation_result (
            run_id, catalog_id, conn_id, source_schema, source_table,
            source_row_count, lake_row_count, row_count_delta, row_count_status,
            apply_lag_seconds, apply_status, status, checks
        ) VALUES (
            $1::bigint, $2::bigint, $3, $4, $5,
            NULLIF($6, '')::bigint, NULLIF($7, '')::bigint, NULLIF($8, '')::bigint, $9,
            NULLIF($10, '')::int, $11, $12, $13::jsonb
        )
        ON CONFLICT (run_id, source_schema, source_table) DO UPDATE SET
            source_row_count = EXCLUDED.source_row_count,
            lake_row_count = EXCLUDED.lake_row_count,
            row_count_delta = EXCLUDED.row_count_delta,
            row_count_status = EXCLUDED.row_count_status,
            apply_lag_seconds = EXCLUDED.apply_lag_seconds,
            apply_status = EXCLUDED.apply_status,
            status = EXCLUDED.status,
            checks = EXCLUDED.checks
        )",
        13,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

int count_stale_tables(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT COUNT(*)::int
        FROM cdc_catalog.v_apply_stale v
        JOIN cdc_catalog.catalog c ON c.catalog_id = v.catalog_id
        WHERE v.conn_id = $1
        )",
        1,
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

std::optional<bool> pk_checksum_match_mariadb(
    MYSQL* mysql,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    const auto pk_cols = split_pk_columns(row.pk_columns);
    if (pk_cols.empty()) {
        return std::nullopt;
    }
    auto esc_id = [](const std::string& id) {
        std::string out;
        for (char c : id) {
            out += (c == '`') ? "``" : std::string(1, c);
        }
        return out;
    };
    std::ostringstream order_by;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            order_by << ", ";
        }
        order_by << "`" << esc_id(pk_cols[i]) << "`";
    }
    std::ostringstream src_sql;
    src_sql << "SELECT " << order_by.str() << " FROM `" << esc_id(row.source_schema) << "`.`" << esc_id(row.source_table)
            << "` ORDER BY " << order_by.str() << " LIMIT " << sample_size;
    if (mysql_query(mysql, src_sql.str().c_str()) != 0) {
        return std::nullopt;
    }
    MYSQL_RES* src_res = mysql_store_result(mysql);
    if (!src_res) {
        return std::nullopt;
    }
    std::ostringstream src_payload;
    MYSQL_ROW src_row;
    while ((src_row = mysql_fetch_row(src_res)) != nullptr) {
        unsigned long* lengths = mysql_fetch_lengths(src_res);
        for (unsigned int i = 0; i < mysql_num_fields(src_res); ++i) {
            if (i > 0) {
                src_payload << '\x1f';
            }
            src_payload << (src_row[i] ? std::string(src_row[i], lengths[i]) : "");
        }
        src_payload << '\n';
    }
    mysql_free_result(src_res);

    std::ostringstream lake_cols;
    std::ostringstream lake_order;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            lake_cols << ", ";
            lake_order << ", ";
        }
        lake_cols << pg_ident(pk_cols[i]);
        lake_order << pg_ident(pk_cols[i]);
    }
    std::ostringstream lake_sql;
    lake_sql << "SELECT " << lake_cols.str() << " FROM " << pg_ident(row.lake_schema) << "."
             << pg_ident(row.lake_table) << " ORDER BY " << lake_order.str() << " LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        for (int j = 0; j < PQnfields(lake_res); ++j) {
            if (j > 0) {
                lake_payload << '\x1f';
            }
            const char* val = PQgetvalue(lake_res, i, j);
            lake_payload << (val ? val : "");
        }
        lake_payload << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}

#ifdef HAVE_FREETDS
std::optional<bool> pk_checksum_match_mssql(
    MssqlConn& mssql,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    const auto pk_cols = split_pk_columns(row.pk_columns);
    if (pk_cols.empty()) {
        return std::nullopt;
    }
    mssql.use_database(row.source_database);
    std::ostringstream order_by;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            order_by << ", ";
        }
        order_by << mssql_brack(pk_cols[i]);
    }
    std::ostringstream src_sql;
    src_sql << "SELECT " << order_by.str() << " FROM " << mssql_brack(row.source_schema) << "."
            << mssql_brack(row.source_table) << " ORDER BY " << order_by.str() << " OFFSET 0 ROWS FETCH NEXT "
            << sample_size << " ROWS ONLY";
    const auto src_result = mssql.query(src_sql.str());
    std::ostringstream src_payload;
    for (const auto& src_row : src_result.rows) {
        for (std::size_t i = 0; i < src_row.size(); ++i) {
            if (i > 0) {
                src_payload << '\x1f';
            }
            src_payload << src_row[i].text;
        }
        src_payload << '\n';
    }

    std::ostringstream lake_cols;
    std::ostringstream lake_order;
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        if (i > 0) {
            lake_cols << ", ";
            lake_order << ", ";
        }
        lake_cols << pg_ident(pk_cols[i]);
        lake_order << pg_ident(pk_cols[i]);
    }
    std::ostringstream lake_sql;
    lake_sql << "SELECT " << lake_cols.str() << " FROM " << pg_ident(row.lake_schema) << "."
             << pg_ident(row.lake_table) << " ORDER BY " << lake_order.str() << " LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        for (int j = 0; j < PQnfields(lake_res); ++j) {
            if (j > 0) {
                lake_payload << '\x1f';
            }
            const char* val = PQgetvalue(lake_res, i, j);
            lake_payload << (val ? val : "");
        }
        lake_payload << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}
#endif

#ifdef HAVE_MONGOC
std::optional<bool> pk_checksum_match_mongo(
    MongoConn& mongo,
    PGconn* lake_pg,
    const CatalogReconcileRow& row,
    int sample_size) {
    if (sample_size <= 0) {
        return std::nullopt;
    }
    mongoc_collection_t* coll = mongo.collection(row.source_database, row.source_table);
    if (!coll) {
        return std::nullopt;
    }
    bson_t query = BSON_INITIALIZER;
    bson_t opts = BSON_INITIALIZER;
    bson_t sort_doc;
    bson_init(&sort_doc);
    BSON_APPEND_INT32(&sort_doc, "_id", 1);
    BSON_APPEND_DOCUMENT(&opts, "sort", &sort_doc);
    BSON_APPEND_INT32(&opts, "limit", sample_size);
    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
    bson_destroy(&sort_doc);
    bson_destroy(&query);
    bson_destroy(&opts);
    mongoc_collection_destroy(coll);

    std::ostringstream src_payload;
    const bson_t* doc = nullptr;
    while (mongoc_cursor_next(cursor, &doc)) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "_id")) {
            if (BSON_ITER_HOLDS_OID(&iter)) {
                char oidstr[25];
                bson_oid_to_string(bson_iter_oid(&iter), oidstr);
                src_payload << oidstr;
            } else if (BSON_ITER_HOLDS_UTF8(&iter)) {
                src_payload << bson_iter_utf8(&iter, nullptr);
            } else if (BSON_ITER_HOLDS_INT32(&iter)) {
                src_payload << bson_iter_int32(&iter);
            } else if (BSON_ITER_HOLDS_INT64(&iter)) {
                src_payload << bson_iter_int64(&iter);
            }
        }
        src_payload << '\n';
    }
    mongoc_cursor_destroy(cursor);

    std::ostringstream lake_sql;
    lake_sql << "SELECT mongo_id FROM " << pg_ident(row.lake_schema) << "." << pg_ident(row.lake_table)
             << " ORDER BY mongo_id LIMIT " << sample_size;
    PGresult* lake_res = PQexec(lake_pg, lake_sql.str().c_str());
    if (!lake_res || PQresultStatus(lake_res) != PGRES_TUPLES_OK) {
        if (lake_res) {
            PQclear(lake_res);
        }
        return std::nullopt;
    }
    std::ostringstream lake_payload;
    for (int i = 0; i < PQntuples(lake_res); ++i) {
        const char* val = PQgetvalue(lake_res, i, 0);
        lake_payload << (val ? val : "") << '\n';
    }
    PQclear(lake_res);
    return md5_hex(src_payload.str()) == md5_hex(lake_payload.str());
}
#endif

}  // namespace

int run_reconcile_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id) {
    const std::string batch_id = make_batch_id();
    const std::string db_engine = conn_engine(cfg, conn_id);

    RuntimeConfig runtime;
    const ReconcileRuntime rcfg = load_reconcile_runtime(runtime, log_pg, conn_id);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"db_engine", db_engine}, {"enabled", rcfg.enabled}},
    });

    if (!rcfg.enabled) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "reconcile",
            .message = "reconcile skipped: disabled in runtime_config",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return 0;
    }

    const int max_tables = rcfg.max_tables > 0 ? rcfg.max_tables : 0;
    const auto tables = fetch_reconcile_catalog(log_pg, conn_id, max_tables);
    const int stale_tables = count_stale_tables(log_pg, conn_id);
    constexpr const char* kReconcileMode = "full";
    const bool pk_sample = rcfg.pk_sample_size > 0;

    const long long run_id = insert_reconcile_run(
        log_pg,
        batch_id,
        conn_id,
        kReconcileMode,
        {{"db_engine", db_engine},
         {"mode", kReconcileMode},
         {"tables_planned", static_cast<int>(tables.size())},
         {"stale_tables", stale_tables}});

    PgConn lake_pg(cfg.datalake.conn_string());

#ifdef HAVE_RDKAFKA
    std::unique_ptr<KafkaLagProbe> kafka_probe;
    const std::string kafka_bootstrap = resolve_kafka_bootstrap().bootstrap;
    try {
        kafka_probe = std::make_unique<KafkaLagProbe>(kafka_bootstrap);
    } catch (...) {
        kafka_probe.reset();
    }
#endif

    std::optional<MariaDbConn> mariadb;
    if (db_engine == "mariadb") {
        const MariaDbSource* source = find_mariadb_source(cfg, conn_id);
        if (!source) {
            throw std::runtime_error("MariaDB source not found: " + conn_id);
        }
        mariadb.emplace(*source);
    }

#ifdef HAVE_FREETDS
    std::optional<MssqlConn> mssql;
    if (db_engine == "mssql") {
        const MssqlSource* source = find_mssql_source(cfg, conn_id);
        if (!source) {
            throw std::runtime_error("MSSQL source not found: " + conn_id);
        }
        mssql.emplace(*source);
    }
#endif

#ifdef HAVE_MONGOC
    std::optional<MongoConn> mongo;
    if (db_engine == "mongodb") {
        const MongoSource* source = find_mongo_source(cfg, conn_id);
        if (!source) {
            throw std::runtime_error("MongoDB source not found: " + conn_id);
        }
        mongo.emplace(*source);
    }
#endif

    int tables_ok = 0;
    int tables_warn = 0;
    int tables_fail = 0;
    int table_errors = 0;

    for (const auto& row : tables) {
        runtime.reload(log_pg);
        try {
            long long source_rows = -1;
            if (db_engine == "mariadb" && mariadb) {
                source_rows = mariadb_table_count(mariadb->handle, row.source_schema, row.source_table);
#ifdef HAVE_FREETDS
            } else if (db_engine == "mssql" && mssql) {
                source_rows =
                    mssql_table_count(*mssql, row.source_database, row.source_schema, row.source_table);
#endif
#ifdef HAVE_MONGOC
            } else if (db_engine == "mongodb" && mongo) {
                source_rows = mongo_collection_count(*mongo, row.source_database, row.source_table);
#endif
            }

            const long long lake_rows = pg_table_count(lake_pg.raw, row.lake_schema, row.lake_table);
            const std::string row_status = eval_row_count_status(source_rows, lake_rows, rcfg);
            const std::string drift_kind = classify_row_drift(source_rows, lake_rows);
            const ApplyMeta apply_meta = fetch_apply_meta(log_pg, row.catalog_id);
            const std::string lag_status = eval_apply_lag_status(apply_meta.apply_lag_seconds, rcfg);
            const int capture_lag_seconds = fetch_capture_lag_seconds(log_pg, conn_id, db_engine, row);
            const std::string capture_status = eval_capture_lag_status(capture_lag_seconds, rcfg);
            std::string overall = status_rank(status_rank(row_status, lag_status), capture_status);

            nlohmann::json checks = nlohmann::json::object();
            checks["reconcile_mode"] = kReconcileMode;
            checks["capture_lag_seconds"] = capture_lag_seconds;
            checks["capture_lag_status"] = capture_status;
            checks["row_count_status"] = row_status;
            checks["apply_lag_status"] = lag_status;
            checks["drift_kind"] = drift_kind;
            if (source_rows >= 0 && lake_rows >= 0) {
                checks["row_count_delta"] = source_rows - lake_rows;
            }

            bool kafka_inactive_table = false;
#ifdef HAVE_RDKAFKA
            long long kafka_lag_probe = -1;
            if (kafka_probe && !apply_meta.kafka_topic.empty() && apply_meta.kafka_offset >= 0) {
                kafka_lag_probe = compute_kafka_consumer_lag(
                    kafka_probe->rk,
                    apply_meta.kafka_topic,
                    apply_meta.kafka_partition,
                    apply_meta.kafka_offset);
            }
            kafka_inactive_table = is_apply_inactive_table(apply_meta, rcfg.apply_inactive_seconds);
            const std::string kafka_status_raw = kafka_inactive_table
                ? "skip"
                : eval_kafka_lag_status(kafka_lag_probe, rcfg);
            const std::string kafka_for_overall =
                cap_kafka_for_overall(row_status, kafka_status_raw);
            checks["kafka_consumer_lag"] = kafka_inactive_table ? 0 : kafka_lag_probe;
            checks["kafka_consumer_lag_probe"] = kafka_lag_probe;
            checks["kafka_inactive_table"] = kafka_inactive_table;
            if (kafka_inactive_table) {
                checks["kafka_lag_skip_reason"] = "inactive_table_bucket_lag";
            }
            checks["kafka_lag_status_raw"] = kafka_status_raw;
            checks["kafka_lag_status"] = kafka_status_raw;
            checks["kafka_lag_status_overall"] = kafka_for_overall;
            checks["kafka_topic"] = apply_meta.kafka_topic;
            checks["kafka_partition"] = apply_meta.kafka_partition;
            checks["kafka_offset"] = apply_meta.kafka_offset;
            checks["seconds_since_last_apply"] = apply_meta.seconds_since_last_apply;
            overall = status_rank(overall, kafka_for_overall);
#else
            checks["kafka_consumer_lag"] = nullptr;
#endif

            {
                const bool pipeline_healthy =
                    lag_status != "fail" && capture_status != "fail";
                const bool static_gap_candidate =
                    pipeline_healthy && row_status == "fail" &&
                    (drift_kind == "source_ahead" || drift_kind == "append_zombie");
                checks["static_gap_detected"] = static_gap_candidate;
                if (static_gap_candidate && kafka_inactive_table) {
                    checks["static_gap_reason"] = "pipeline_healthy_row_drift_inactive_table";
                }
            }

            if (pk_sample && db_engine == "mariadb" && mariadb) {
                const auto pk_match = pk_checksum_match_mariadb(
                    mariadb->handle, lake_pg.raw, row, rcfg.pk_sample_size);
                if (pk_match) {
                    checks["pk_checksum_match"] = *pk_match;
                    const std::string pk_for_overall =
                        cap_pk_checksum_for_overall(row_status, *pk_match);
                    checks["pk_checksum_status_overall"] = pk_for_overall;
                    overall = status_rank(overall, pk_for_overall);
                }
#ifdef HAVE_FREETDS
            } else if (pk_sample && db_engine == "mssql" && mssql) {
                const auto pk_match = pk_checksum_match_mssql(*mssql, lake_pg.raw, row, rcfg.pk_sample_size);
                if (pk_match) {
                    checks["pk_checksum_match"] = *pk_match;
                    const std::string pk_for_overall =
                        cap_pk_checksum_for_overall(row_status, *pk_match);
                    checks["pk_checksum_status_overall"] = pk_for_overall;
                    overall = status_rank(overall, pk_for_overall);
                }
#endif
#ifdef HAVE_MONGOC
            } else if (pk_sample && db_engine == "mongodb" && mongo) {
                const auto pk_match = pk_checksum_match_mongo(*mongo, lake_pg.raw, row, rcfg.pk_sample_size);
                if (pk_match) {
                    checks["pk_checksum_match"] = *pk_match;
                    const std::string pk_for_overall =
                        cap_pk_checksum_for_overall(row_status, *pk_match);
                    checks["pk_checksum_status_overall"] = pk_for_overall;
                    overall = status_rank(overall, pk_for_overall);
                }
#endif
            }

            insert_reconcile_result(
                log_pg, run_id, row, source_rows, lake_rows, row_status, apply_meta, overall, checks);

            if (overall == "fail") {
                const bool needs_fl = reconcile_failure_needs_full_load(drift_kind, row_status);
                mark_catalog_reconcile_failed(
                    log_pg,
                    row.catalog_id,
                    reconcile_failure_message(drift_kind, source_rows, lake_rows, row_status, overall),
                    needs_fl);
            } else if (overall == "ok" && row_status == "ok") {
                mark_catalog_reconcile_healed(log_pg, row.catalog_id);
            }

            if (overall == "fail") {
                tables_fail += 1;
            } else if (overall == "warn") {
                tables_warn += 1;
            } else if (overall == "ok") {
                tables_ok += 1;
            }

            log_write(log_pg, {
                .level = overall == "fail" ? LogLevel::Warning : LogLevel::Info,
                .component = "reconcile",
                .message = "table reconcile completed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = row.source_schema,
                .source_table = row.source_table,
                .context = {
                    {"status", overall},
                    {"source_row_count", source_rows},
                    {"lake_row_count", lake_rows},
                    {"row_count_delta", source_rows >= 0 && lake_rows >= 0 ? source_rows - lake_rows : 0},
                    {"row_count_status", row_status},
                    {"drift_kind", drift_kind},
                    {"static_gap_detected", checks.value("static_gap_detected", false)},
                    {"apply_lag_seconds", apply_meta.apply_lag_seconds},
                    {"apply_lag_status", lag_status},
                    {"capture_lag_seconds", capture_lag_seconds},
                    {"capture_lag_status", capture_status},
                    {"reconcile_mode", kReconcileMode},
                    {"kafka_consumer_lag", checks.value("kafka_consumer_lag", nlohmann::json())},
                    {"kafka_lag_status", checks.value("kafka_lag_status", nlohmann::json("skip"))},
                },
            });
        } catch (const std::exception& ex) {
            table_errors += 1;
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "reconcile",
                .message = "table reconcile failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = row.source_schema,
                .source_table = row.source_table,
                .context = {{"error", ex.what()}},
            });
        }
    }

    std::string run_status = "ok";
    if (tables_fail > 0 || table_errors > 0) {
        run_status = "fail";
    } else if (tables_warn > 0) {
        run_status = "warn";
    }

    finish_reconcile_run(
        log_pg,
        run_id,
        run_status,
        static_cast<int>(tables.size()),
        tables_ok,
        tables_warn,
        tables_fail,
        stale_tables,
        {{"table_errors", table_errors}, {"db_engine", db_engine}, {"mode", kReconcileMode}});

    log_write(log_pg, {
        .level = run_status == "fail" ? LogLevel::Warning : LogLevel::Info,
        .component = "reconcile",
        .message = run_status == "fail" ? "reconcile completed with errors" : "reconcile completed",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"run_id", run_id},
            {"mode", kReconcileMode},
            {"status", run_status},
            {"tables_checked", static_cast<int>(tables.size())},
            {"tables_ok", tables_ok},
            {"tables_warn", tables_warn},
            {"tables_fail", tables_fail},
            {"stale_tables", stale_tables},
            {"table_errors", table_errors},
        },
    });

    return run_status == "fail" ? 1 : 0;
}

int run_reconcile_loop(
    const AppConfig& cfg,
    PGconn* log_pg,
    bool once,
    std::atomic<bool>* external_shutdown) {
    std::atomic<bool>* shutdown = external_shutdown ? external_shutdown : &g_reconcile_shutdown;
    if (!external_shutdown) {
        std::signal(SIGINT, on_reconcile_signal);
        std::signal(SIGTERM, on_reconcile_signal);
    }

    RuntimeConfig runtime;
    runtime.reload(log_pg);
    const int retention_days =
        runtime.get_int("logs_retention_days", pipeline_defaults::kLogsRetentionDaysDefault, "global");
    purge_logs(log_pg, retention_days);

    std::vector<std::string> conn_ids;
    for (const auto& s : cfg.mariadb_sources) {
        conn_ids.push_back(s.conn_id);
    }
    for (const auto& s : cfg.mssql_sources) {
        conn_ids.push_back(s.conn_id);
    }
    for (const auto& s : cfg.mongo_sources) {
        conn_ids.push_back(s.conn_id);
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile-loop started",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {{"conn_ids", static_cast<int>(conn_ids.size())}},
    });

    while (!shutdown->load()) {
        runtime.reload(log_pg);
        for (const auto& conn_id : conn_ids) {
            if (shutdown->load()) {
                break;
            }
            try {
                run_reconcile_cli(cfg, log_pg, conn_id);
            } catch (const std::exception& ex) {
                log_write(log_pg, {
                    .level = LogLevel::Error,
                    .component = "reconcile",
                    .message = "reconcile-loop conn failed",
                    .batch_id = make_batch_id(),
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {{"error", ex.what()}},
                });
            }
        }

        if (once) {
            break;
        }
        const int interval_hours =
            runtime.get_int(
                "reconcile_interval_hours",
                pipeline_defaults::kReconcileIntervalHoursDefault,
                "cdc_kafka_reconcile",
                "");
        const int sleep_sec = std::max(60, interval_hours * 3600);
        for (int i = 0; i < sleep_sec && !shutdown->load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile-loop stopped",
        .batch_id = make_batch_id(),
        .conn_id = std::nullopt,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
    });
    return 0;
}
