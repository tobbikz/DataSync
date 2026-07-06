#include "reconcile_lite.hpp"

#include "full_load_checkpoint.hpp"
#include "full_load_common.hpp"
#include "mariadb_conn.hpp"
#include "mariadb_schema.hpp"
#include "mongo_conn.hpp"
#include "mssql_conn.hpp"
#include "mssql_schema.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "pipeline_defaults.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cmath>
#include <ctime>
#include <map>
#include <optional>
#include <sstream>
#include <vector>

namespace {

using json = nlohmann::json;

struct ReconcileCandidate {
    long long catalog_id{0};
    std::string conn_id;
    std::string db_engine;
    std::string source_database;
    std::string source_schema;
    std::string source_table;
    std::string pk_columns;
    json engine_meta = json::object();
    std::string catalog_status;
    std::string apply_status;
};

using Status = std::string;

constexpr const char* kOk = "ok";
constexpr const char* kWarn = "warn";
constexpr const char* kFail = "fail";
constexpr const char* kSkip = "skip";

Status combine_status(Status a, Status b) {
    auto rank = [](const Status& s) {
        if (s == kFail) {
            return 3;
        }
        if (s == kWarn) {
            return 2;
        }
        if (s == kOk) {
            return 1;
        }
        return 0;
    };
    return rank(a) >= rank(b) ? a : b;
}

std::string lower_ascii(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return value;
}

bool in_sample(long long catalog_id, int sample_pct) {
    if (sample_pct >= 100) {
        return true;
    }
    if (sample_pct <= 0) {
        return false;
    }
    return (catalog_id % 100) < sample_pct;
}

std::string hot_filter_sql(CatalogHotTier tier) {
    if (tier == CatalogHotTier::HotOnly) {
        return " AND c.hot = true";
    }
    if (tier == CatalogHotTier::ColdOnly) {
        return " AND c.hot = false";
    }
    return "";
}

MariaDbRetryOptions mariadb_retry() {
    MariaDbRetryOptions opts;
    opts.max_attempts = pipeline_defaults::kMariadbReconnectMaxAttempts > 0
                            ? pipeline_defaults::kMariadbReconnectMaxAttempts
                            : 3;
    opts.base_ms = pipeline_defaults::kMariadbReconnectBaseMs;
    opts.max_ms = pipeline_defaults::kMariadbReconnectMaxMs;
    return opts;
}

MssqlRetryOptions mssql_retry() {
    MssqlRetryOptions opts;
    opts.max_attempts = pipeline_defaults::kMssqlReconnectMaxAttempts > 0
                              ? pipeline_defaults::kMssqlReconnectMaxAttempts
                              : 3;
    opts.base_ms = pipeline_defaults::kMssqlReconnectBaseMs;
    opts.max_ms = pipeline_defaults::kMssqlReconnectMaxMs;
    return opts;
}

std::optional<std::string> engine_meta_ts_column(const json& meta) {
    if (!meta.is_object() || !meta.contains("reconcile_ts_column") ||
        !meta["reconcile_ts_column"].is_string()) {
        return std::nullopt;
    }
    const std::string col = meta["reconcile_ts_column"].get<std::string>();
    return col.empty() ? std::nullopt : std::optional(col);
}

std::optional<std::string> pick_ts_column_from_names(const std::vector<std::string>& names) {
    static const char* kPreferred[] = {
        "updated_at", "modified_at", "last_modified", "fecha_modificacion"};
    std::map<std::string, std::string> lower_to_orig;
    for (const auto& name : names) {
        lower_to_orig[lower_ascii(name)] = name;
    }
    for (const char* pref : kPreferred) {
        const auto it = lower_to_orig.find(pref);
        if (it != lower_to_orig.end()) {
            return it->second;
        }
    }
    return std::nullopt;
}

bool lake_column_exists(PGconn* pg, const std::string& schema, const std::string& table, const std::string& column) {
    const char* vals[] = {schema.c_str(), table.c_str(), column.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
        LIMIT 1
        )",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool ok = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return ok;
}

std::optional<std::string> lake_scalar_text(PGconn* pg, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1 || PQnfields(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return std::nullopt;
    }
    if (PQgetisnull(res, 0, 0)) {
        PQclear(res);
        return std::nullopt;
    }
    const std::string value = PQgetvalue(res, 0, 0);
    PQclear(res);
    return value;
}

std::string utc_now_iso() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

Status evaluate_row_count(long long source_rows, long long lake_rows) {
    if (source_rows < 0 || lake_rows < 0) {
        return kSkip;
    }
    if (source_rows == lake_rows) {
        return kOk;
    }
    const long long abs_diff = std::llabs(source_rows - lake_rows);
    if (source_rows <= pipeline_defaults::kFullLoadRowCountVerifyLargeTableThreshold) {
        return kFail;
    }
    const double pct =
        source_rows > 0 ? static_cast<double>(abs_diff) / static_cast<double>(source_rows) : 1.0;
    if (pct > pipeline_defaults::kFullLoadRowCountVerifyTolerancePct) {
        return kFail;
    }
    return kWarn;
}

Status apply_row_count_gating(Status evaluated, long long kafka_lag, json& checks) {
    checks["kafka_consumer_lag"] = kafka_lag;
    if (kafka_lag <= 0 || evaluated == kOk || evaluated == kSkip) {
        return evaluated;
    }
    checks["count_gated_reason"] = "kafka_consumer_lag_active";
    checks["row_count_raw_status"] = evaluated;
    return kSkip;
}

struct LakeSnapshotTxn {
    PGconn* pg{nullptr};
    bool active{false};

    explicit LakeSnapshotTxn(PGconn* lake_pg) : pg(lake_pg) {
        pg_exec(pg, "BEGIN ISOLATION LEVEL REPEATABLE READ");
        active = true;
    }

    ~LakeSnapshotTxn() {
        if (active && pg) {
            PGresult* res = PQexec(pg, "ROLLBACK");
            if (res) {
                PQclear(res);
            }
        }
    }

    void commit() {
        if (active && pg) {
            pg_exec(pg, "COMMIT");
            active = false;
        }
    }

    std::string read_at() const {
        return lake_scalar_text(pg,
                                "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', "
                                "'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
            .value_or("");
    }
};

Status evaluate_max_pk(const std::optional<std::string>& source, const std::optional<std::string>& lake) {
    if (!source.has_value() || !lake.has_value()) {
        return kSkip;
    }
    if (source->empty() && lake->empty()) {
        return kOk;
    }
    return *source == *lake ? kOk : kFail;
}

Status evaluate_max_ts_lag(std::optional<int> lag_seconds) {
    if (!lag_seconds.has_value()) {
        return kSkip;
    }
    const int lag = *lag_seconds;
    if (lag <= pipeline_defaults::kReconcileLiteTsLagWarnSeconds) {
        return kOk;
    }
    if (lag <= pipeline_defaults::kReconcileLiteTsLagFailSeconds) {
        return kWarn;
    }
    return kFail;
}

std::optional<int> lag_seconds_on_pg(PGconn* pg, const std::string& source_ts, const std::string& lake_ts) {
    const char* vals[] = {source_ts.c_str(), lake_ts.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT extract(epoch FROM $1::timestamptz - $2::timestamptz)::integer",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1 || PQgetisnull(res, 0, 0)) {
        if (res) {
            PQclear(res);
        }
        return std::nullopt;
    }
    const int lag = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    return lag;
}

bool mariadb_type_numeric_pk(const std::string& mysql_type_raw) {
    const std::string t = lower_ascii(mysql_type_raw);
    if (t.find("int") != std::string::npos) {
        return true;
    }
    if (t.find("decimal") != std::string::npos || t.find("numeric") != std::string::npos) {
        return t.find(",0)") != std::string::npos;
    }
    return false;
}

bool mssql_type_numeric_pk(const std::string& mssql_type) {
    const std::string t = lower_ascii(mssql_type);
    return t.find("int") != std::string::npos;
}

long long fetch_mariadb_count(
    MariaDbConn& conn,
    const std::string& schema,
    const std::string& table,
    const MariaDbRetryOptions& retry) {
    const std::string query = "SELECT COUNT(*) FROM `" + schema + "`.`" + table + "`";
    MYSQL_RES* res = mariadb_mysql_query_store_retry(conn, query, retry);
    if (!res) {
        return -1;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    long long count = -1;
    if (row && row[0]) {
        count = std::atoll(row[0]);
    }
    mysql_free_result(res);
    return count;
}

std::optional<std::string> fetch_mariadb_scalar(
    MariaDbConn& conn,
    const std::string& sql,
    const MariaDbRetryOptions& retry) {
    MYSQL_RES* res = mariadb_mysql_query_store_retry(conn, sql, retry);
    if (!res) {
        return std::nullopt;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    std::optional<std::string> out;
    if (row && row[0]) {
        out = row[0];
    }
    mysql_free_result(res);
    return out;
}

struct MariaDbSnapshotTxn {
    MariaDbConn& conn;
    const MariaDbRetryOptions& retry;
    bool active{false};

    MariaDbSnapshotTxn(MariaDbConn& mariadb, const MariaDbRetryOptions& opts)
        : conn(mariadb), retry(opts) {
        mariadb_mysql_query_retry(conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT", retry);
        active = true;
    }

    ~MariaDbSnapshotTxn() {
        if (active) {
            mariadb_mysql_query_retry(conn, "ROLLBACK", retry);
        }
    }

    void commit() {
        if (active) {
            mariadb_mysql_query_retry(conn, "COMMIT", retry);
            active = false;
        }
    }

    std::string read_at() const {
        return fetch_mariadb_scalar(
                   conn,
                   "SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')",
                   retry)
            .value_or("");
    }
};

#ifdef HAVE_FREETDS
long long fetch_mssql_count(
    MssqlConn& mssql,
    const std::string& schema,
    const std::string& table,
    const MssqlRetryOptions& retry) {
    const std::string sql = "SELECT COUNT(*) FROM [" + schema + "].[" + table + "]";
    const auto result = mssql.query_retry(sql, retry);
    if (result.rows.empty() || result.rows.front().empty()) {
        return -1;
    }
    try {
        return std::stoll(result.rows.front().front().text);
    } catch (...) {
        return -1;
    }
}

std::optional<std::string> fetch_mssql_scalar(
    MssqlConn& mssql,
    const std::string& sql,
    const MssqlRetryOptions& retry) {
    const auto result = mssql.query_retry(sql, retry);
    if (result.rows.empty() || result.rows.front().empty() || result.rows.front().front().is_binary) {
        return std::nullopt;
    }
    return result.rows.front().front().text;
}

struct MssqlSnapshotTxn {
    MssqlConn& conn;
    const MssqlRetryOptions& retry;
    bool active{false};

    MssqlSnapshotTxn(MssqlConn& mssql, const MssqlRetryOptions& opts) : conn(mssql), retry(opts) {
        conn.exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        conn.exec("BEGIN TRANSACTION");
        active = true;
    }

    ~MssqlSnapshotTxn() {
        if (active) {
            try {
                conn.exec("ROLLBACK");
            } catch (...) {
            }
        }
    }

    void commit() {
        if (active) {
            conn.exec("COMMIT");
            active = false;
        }
    }

    std::string read_at() const {
        return fetch_mssql_scalar(conn, "SELECT CONVERT(varchar(33), SYSUTCDATETIME(), 127)", retry)
            .value_or("");
    }
};
#endif

#ifdef HAVE_MONGOC
std::string mongo_bson_value_to_text(const bson_value_t* value) {
    if (!value) {
        return {};
    }
    if (value->value_type == BSON_TYPE_OID) {
        char hex[25];
        bson_oid_to_string(&value->value.v_oid, hex);
        return std::string(hex);
    }
    if (value->value_type == BSON_TYPE_UTF8) {
        return std::string(value->value.v_utf8.str, static_cast<std::size_t>(value->value.v_utf8.len));
    }
    if (value->value_type == BSON_TYPE_INT32) {
        return std::to_string(value->value.v_int32);
    }
    if (value->value_type == BSON_TYPE_INT64) {
        return std::to_string(value->value.v_int64);
    }
    if (value->value_type == BSON_TYPE_DATE_TIME) {
        return std::to_string(value->value.v_datetime);
    }
    if (value->value_type == BSON_TYPE_BOOL) {
        return value->value.v_bool ? "true" : "false";
    }
    return {};
}

long long fetch_mongo_count(mongoc_collection_t* coll) {
    bson_t empty = BSON_INITIALIZER;
    bson_error_t err;
    const int64_t count = mongoc_collection_count_documents(coll, &empty, nullptr, 0, nullptr, &err);
    bson_destroy(&empty);
    if (count < 0) {
        return -1;
    }
    return static_cast<long long>(count);
}

std::optional<std::string> fetch_mongo_max_id(mongoc_collection_t* coll) {
    bson_t query = BSON_INITIALIZER;
    bson_t opts = BSON_INITIALIZER;
    bson_t sort = BSON_INITIALIZER;
    bson_t proj = BSON_INITIALIZER;
    BSON_APPEND_INT32(&sort, "_id", -1);
    bson_append_document(&opts, "sort", -1, &sort);
    bson_append_int32(&opts, "limit", 1, 1);
    BSON_APPEND_INT32(&proj, "_id", 1);
    bson_append_document(&opts, "projection", -1, &proj);

    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
    bson_destroy(&query);
    bson_destroy(&opts);
    bson_destroy(&sort);
    bson_destroy(&proj);

    const bson_t* doc = nullptr;
    std::optional<std::string> out;
    if (mongoc_cursor_next(cursor, &doc) && doc) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, "_id")) {
            out = mongo_bson_value_to_text(bson_iter_value(&iter));
        }
    }
    mongoc_cursor_destroy(cursor);
    return out;
}

std::optional<std::string> fetch_mongo_max_field(mongoc_collection_t* coll, const std::string& field) {
    bson_t query = BSON_INITIALIZER;
    bson_t opts = BSON_INITIALIZER;
    bson_t sort = BSON_INITIALIZER;
    bson_t proj = BSON_INITIALIZER;
    bson_append_int32(&sort, field.c_str(), -1, -1);
    bson_append_document(&opts, "sort", -1, &sort);
    bson_append_int32(&opts, "limit", 1, 1);
    bson_append_int32(&proj, field.c_str(), 1, 1);
    bson_append_document(&opts, "projection", -1, &proj);

    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &query, &opts, nullptr);
    bson_destroy(&query);
    bson_destroy(&opts);
    bson_destroy(&sort);
    bson_destroy(&proj);

    const bson_t* doc = nullptr;
    std::optional<std::string> out;
    if (mongoc_cursor_next(cursor, &doc) && doc) {
        bson_iter_t iter;
        if (bson_iter_init_find(&iter, doc, field.c_str())) {
            out = mongo_bson_value_to_text(bson_iter_value(&iter));
        }
    }
    mongoc_cursor_destroy(cursor);
    return out;
}
#endif

json fetch_pipeline_snapshot(PGconn* pg, long long catalog_id) {
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT apply_health_rag, kafka_consumer_lag, capture_lag_seconds, event_loss_status
        FROM cdc_catalog.v_apply_latest
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    json out = json::object();
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    out["apply_health_rag"] = PQgetisnull(res, 0, 0) ? "" : PQgetvalue(res, 0, 0);
    out["kafka_consumer_lag"] = PQgetisnull(res, 0, 1) ? 0 : std::atoll(PQgetvalue(res, 0, 1));
    out["capture_lag_seconds"] = PQgetisnull(res, 0, 2) ? 0 : std::atoi(PQgetvalue(res, 0, 2));
    out["event_loss_status"] = PQgetisnull(res, 0, 3) ? "ok" : PQgetvalue(res, 0, 3);
    PQclear(res);
    return out;
}

std::vector<ReconcileCandidate> load_candidates(
    PGconn* pg,
    const std::optional<std::string>& conn_id_filter,
    CatalogHotTier hot_tier) {
    std::ostringstream sql;
    sql << R"(
        SELECT c.catalog_id, c.conn_id, c.db_engine::text, COALESCE(c.source_database, ''),
               c.source_schema, c.source_table, COALESCE(c.pk_columns, ''),
               COALESCE(c.engine_meta::text, '{}'), c.status::text,
               COALESCE(ap.status::text, 'healthy')
        FROM cdc_catalog.catalog c
        LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        WHERE c.active = true
          AND c.cdc_enabled = true
          AND NOT c.needs_full_load
          AND c.has_pk = true
          AND c.status NOT IN ('skipped', 'disabled')
    )";
    sql << hot_filter_sql(hot_tier);
    const char* conn_param = "";
    std::string conn_val;
    if (conn_id_filter && !conn_id_filter->empty()) {
        sql << " AND c.conn_id = $1";
        conn_val = *conn_id_filter;
        conn_param = conn_val.c_str();
    }
    sql << " ORDER BY c.conn_id, c.source_schema, c.source_table";

    PGresult* res = conn_id_filter && !conn_id_filter->empty()
                          ? PQexecParams(pg, sql.str().c_str(), 1, nullptr, &conn_param, nullptr, nullptr, 0)
                          : PQexec(pg, sql.str().c_str());
    std::vector<ReconcileCandidate> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        ReconcileCandidate row;
        row.catalog_id = std::atoll(PQgetvalue(res, i, 0));
        row.conn_id = PQgetvalue(res, i, 1);
        row.db_engine = PQgetvalue(res, i, 2);
        row.source_database = PQgetvalue(res, i, 3);
        row.source_schema = PQgetvalue(res, i, 4);
        row.source_table = PQgetvalue(res, i, 5);
        row.pk_columns = PQgetvalue(res, i, 6);
        try {
            row.engine_meta = json::parse(PQgetvalue(res, i, 7));
        } catch (...) {
            row.engine_meta = json::object();
        }
        row.catalog_status = PQgetvalue(res, i, 8);
        row.apply_status = PQgetvalue(res, i, 9);
        out.push_back(std::move(row));
    }
    PQclear(res);
    return out;
}

bool should_skip_table(PGconn* pg, const ReconcileCandidate& row, json& checks) {
    if (row.apply_status == "quarantined") {
        checks["skip_reason"] = "apply_quarantined";
        return true;
    }
    if (row.catalog_status == "full_load_in_progress") {
        checks["skip_reason"] = "full_load_in_progress";
        return true;
    }
    for (const auto& cp : load_full_load_checkpoints(pg, row.catalog_id)) {
        if (cp.phase == FullLoadPhase::Copy) {
            checks["skip_reason"] = "copy_checkpoint_active";
            return true;
        }
    }
    return false;
}

bool insert_reconciliation_row(
    PGconn* pg,
    const std::string& batch_id,
    const ReconcileCandidate& row,
    Status global_status,
    long long source_count,
    long long lake_count,
    Status row_count_status,
    const std::optional<std::string>& max_pk_source,
    const std::optional<std::string>& max_pk_lake,
    Status max_pk_status,
    const std::optional<std::string>& ts_column,
    const std::optional<std::string>& max_ts_source,
    const std::optional<std::string>& max_ts_lake,
    std::optional<int> max_ts_lag,
    Status max_ts_status,
    const json& pipeline_snapshot,
    const json& checks,
    long long duration_ms,
    const std::optional<std::string>& error) {
    const long long delta =
        (source_count >= 0 && lake_count >= 0) ? (source_count - lake_count) : 0;
    const std::string checks_str = checks.dump();
    const std::string pipeline_str = pipeline_snapshot.dump();
    const std::string cid = std::to_string(row.catalog_id);
    const std::string src_cnt = source_count >= 0 ? std::to_string(source_count) : "";
    const std::string lake_cnt = lake_count >= 0 ? std::to_string(lake_count) : "";
    const std::string delta_str = std::to_string(delta);
    const std::string dur = std::to_string(duration_ms);
    const std::string lag_str = max_ts_lag.has_value() ? std::to_string(*max_ts_lag) : "";
    const char* vals[] = {
        batch_id.c_str(),
        row.conn_id.c_str(),
        cid.c_str(),
        row.db_engine.c_str(),
        row.source_schema.c_str(),
        row.source_table.c_str(),
        global_status.c_str(),
        src_cnt.empty() ? nullptr : src_cnt.c_str(),
        lake_cnt.empty() ? nullptr : lake_cnt.c_str(),
        delta_str.c_str(),
        row_count_status.c_str(),
        max_pk_source.has_value() ? max_pk_source->c_str() : nullptr,
        max_pk_lake.has_value() ? max_pk_lake->c_str() : nullptr,
        max_pk_status.c_str(),
        ts_column.has_value() ? ts_column->c_str() : nullptr,
        max_ts_source.has_value() ? max_ts_source->c_str() : nullptr,
        max_ts_lake.has_value() ? max_ts_lake->c_str() : nullptr,
        lag_str.empty() ? nullptr : lag_str.c_str(),
        max_ts_status.c_str(),
        pipeline_str.c_str(),
        checks_str.c_str(),
        dur.c_str(),
        error.has_value() ? error->c_str() : nullptr,
    };
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.reconciliation (
            batch_id, conn_id, catalog_id, db_engine, source_schema, source_table,
            status, row_count_source, row_count_lake, row_count_delta,
            row_count_status, max_pk_source, max_pk_lake, max_pk_status,
            ts_column, max_ts_source, max_ts_lake, max_ts_lag_seconds, max_ts_status,
            pipeline_snapshot, checks, duration_ms, error
        ) VALUES (
            $1, $2, $3::bigint, $4::cdc_catalog.db_engine, $5, $6,
            $7, NULLIF($8,'')::bigint, NULLIF($9,'')::bigint, $10::bigint,
            $11, $12, $13, $14,
            $15, NULLIF($16,'')::timestamptz, NULLIF($17,'')::timestamptz,
            NULLIF($18,'')::integer, $19,
            $20::jsonb, $21::jsonb, $22::bigint, $23
        )
        )",
        23,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool ok = res && PQresultStatus(res) == PGRES_COMMAND_OK;
    if (res) {
        PQclear(res);
    }
    return ok;
}

long long prune_reconciliation(PGconn* pg, int retention_days) {
    const std::string days = std::to_string(retention_days);
    const char* vals[] = {days.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT cdc_catalog.prune_reconciliation($1::integer)",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    long long pruned = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        pruned = std::atoll(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return pruned;
}

std::optional<std::string> resolve_ts_column(
    const json& engine_meta,
    const std::vector<std::string>& column_names,
    PGconn* lake_pg,
    const std::string& lake_schema,
    const std::string& lake_table) {
    if (auto meta_col = engine_meta_ts_column(engine_meta)) {
        if (lake_column_exists(lake_pg, lake_schema, lake_table, *meta_col)) {
            return meta_col;
        }
    }
    if (auto picked = pick_ts_column_from_names(column_names)) {
        if (lake_column_exists(lake_pg, lake_schema, lake_table, *picked)) {
            return picked;
        }
    }
    return std::nullopt;
}

std::optional<std::string> lake_max_pk(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::string& pk_col) {
    if (!lake_column_exists(pg, schema, table, pk_col)) {
        return std::nullopt;
    }
    const std::string sql =
        "SELECT MAX(" + pg_ident(pk_col) + ")::text FROM " + pg_ident(schema) + "." + pg_ident(table);
    return lake_scalar_text(pg, sql);
}

std::optional<std::string> lake_max_ts(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::string& ts_col) {
    if (!lake_column_exists(pg, schema, table, ts_col)) {
        return std::nullopt;
    }
    const std::string sql =
        "SELECT MAX(" + pg_ident(ts_col) + ")::timestamptz::text FROM " + pg_ident(schema) + "." +
        pg_ident(table);
    return lake_scalar_text(pg, sql);
}

struct TableRunStats {
    int checked{0};
    int ok{0};
    int warn{0};
    int fail{0};
    int skip{0};
    int errors{0};
};

void bump_status(TableRunStats& stats, Status status) {
    stats.checked += 1;
    if (status == kOk) {
        stats.ok += 1;
    } else if (status == kWarn) {
        stats.warn += 1;
    } else if (status == kFail) {
        stats.fail += 1;
    } else {
        stats.skip += 1;
    }
}

bool reconcile_table_mariadb(
    PGconn* log_pg,
    PGconn* lake_pg,
    MariaDbConn& conn,
    const std::string& batch_id,
    const ReconcileCandidate& row,
    TableRunStats& stats) {
    const auto start = std::chrono::steady_clock::now();
    json checks = json::object();
    if (should_skip_table(log_pg, row, checks)) {
        insert_reconciliation_row(
            log_pg,
            batch_id,
            row,
            kSkip,
            -1,
            -1,
            kSkip,
            std::nullopt,
            std::nullopt,
            kSkip,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            kSkip,
            fetch_pipeline_snapshot(log_pg, row.catalog_id),
            checks,
            full_load::elapsed_ms(start),
            std::nullopt);
        bump_status(stats, kSkip);
        return true;
    }

    const MariaDbRetryOptions retry = mariadb_retry();
    std::optional<std::string> error;
    try {
        const json pipeline = fetch_pipeline_snapshot(log_pg, row.catalog_id);
        const long long kafka_lag = pipeline.value("kafka_consumer_lag", 0LL);

        const auto pk_cols = split_pk_columns(row.pk_columns);
        const auto cols = fetch_mariadb_columns(conn.handle, row.source_schema, row.source_table);
        std::vector<std::string> col_names;
        col_names.reserve(cols.size());
        for (const auto& col : cols) {
            col_names.push_back(col.name);
        }
        const auto ts_column =
            resolve_ts_column(row.engine_meta, col_names, lake_pg, row.source_schema, row.source_table);
        bool pk_numeric = false;
        if (pk_cols.size() == 1) {
            for (const auto& col : cols) {
                if (col.name == pk_cols[0]) {
                    pk_numeric = mariadb_type_numeric_pk(col.mysql_type);
                    break;
                }
            }
        }

        long long source_count = -1;
        std::optional<std::string> max_pk_source;
        std::optional<std::string> max_ts_source;
        std::string source_read_at;
        {
            MariaDbSnapshotTxn source_txn(conn, retry);
            source_count = fetch_mariadb_count(conn, row.source_schema, row.source_table, retry);
            if (pk_cols.size() == 1 && pk_numeric) {
                const std::string sql = "SELECT MAX(`" + pk_cols[0] + "`) FROM `" + row.source_schema +
                                        "`.`" + row.source_table + "`";
                max_pk_source = fetch_mariadb_scalar(conn, sql, retry);
            }
            if (ts_column.has_value()) {
                const std::string sql = "SELECT MAX(`" + *ts_column + "`) FROM `" + row.source_schema + "`.`" +
                                        row.source_table + "`";
                max_ts_source = fetch_mariadb_scalar(conn, sql, retry);
            }
            source_read_at = source_txn.read_at();
            source_txn.commit();
        }

        long long lake_count = -1;
        std::optional<std::string> max_pk_lake;
        std::optional<std::string> max_ts_lake;
        std::string lake_read_at;
        {
            LakeSnapshotTxn lake_txn(lake_pg);
            lake_count = full_load::lake_table_row_count(lake_pg, row.source_schema, row.source_table);
            if (pk_cols.size() == 1 && pk_numeric) {
                max_pk_lake = lake_max_pk(lake_pg, row.source_schema, row.source_table, pk_cols[0]);
            }
            if (ts_column.has_value()) {
                max_ts_lake = lake_max_ts(lake_pg, row.source_schema, row.source_table, *ts_column);
            }
            lake_read_at = lake_txn.read_at();
            lake_txn.commit();
        }

        const Status raw_row_count_status = evaluate_row_count(source_count, lake_count);
        checks["engine"] = "mariadb";
        checks["source_snapshot"] = "consistent_snapshot";
        checks["lake_snapshot"] = "repeatable_read";
        checks["source_read_at"] = source_read_at;
        checks["lake_read_at"] = lake_read_at;
        const Status row_count_status = apply_row_count_gating(raw_row_count_status, kafka_lag, checks);

        Status max_pk_status = kSkip;
        if (max_pk_source.has_value() || max_pk_lake.has_value()) {
            max_pk_status = evaluate_max_pk(max_pk_source, max_pk_lake);
        }

        std::optional<int> max_ts_lag;
        Status max_ts_status = kSkip;
        if (ts_column.has_value()) {
            if (max_ts_source.has_value() && max_ts_lake.has_value()) {
                max_ts_lag = lag_seconds_on_pg(lake_pg, *max_ts_source, *max_ts_lake);
            }
            max_ts_status = evaluate_max_ts_lag(max_ts_lag);
        }

        Status global = combine_status(row_count_status, combine_status(max_pk_status, max_ts_status));
        if (!insert_reconciliation_row(
                log_pg,
                batch_id,
                row,
                global,
                source_count,
                lake_count,
                row_count_status,
                max_pk_source,
                max_pk_lake,
                max_pk_status,
                ts_column,
                max_ts_source,
                max_ts_lake,
                max_ts_lag,
                max_ts_status,
                pipeline,
                checks,
                full_load::elapsed_ms(start),
                std::nullopt)) {
            stats.errors += 1;
            return false;
        }
        bump_status(stats, global);
        return true;
    } catch (const std::exception& ex) {
        error = ex.what();
    }
    insert_reconciliation_row(
        log_pg,
        batch_id,
        row,
        kFail,
        -1,
        -1,
        kFail,
        std::nullopt,
        std::nullopt,
        kSkip,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        std::nullopt,
        kSkip,
        fetch_pipeline_snapshot(log_pg, row.catalog_id),
        checks,
        full_load::elapsed_ms(start),
        error);
    stats.errors += 1;
    stats.fail += 1;
    return false;
}

#ifdef HAVE_FREETDS
bool reconcile_table_mssql(
    PGconn* log_pg,
    PGconn* lake_pg,
    MssqlConn& conn,
    const std::string& batch_id,
    const ReconcileCandidate& row,
    TableRunStats& stats) {
    const auto start = std::chrono::steady_clock::now();
    json checks = json::object();
    if (should_skip_table(log_pg, row, checks)) {
        insert_reconciliation_row(
            log_pg, batch_id, row, kSkip, -1, -1, kSkip, std::nullopt, std::nullopt, kSkip, std::nullopt,
            std::nullopt, std::nullopt, std::nullopt, kSkip, fetch_pipeline_snapshot(log_pg, row.catalog_id),
            checks, full_load::elapsed_ms(start), std::nullopt);
        bump_status(stats, kSkip);
        return true;
    }

    const MssqlRetryOptions retry = mssql_retry();
    std::optional<std::string> error;
    try {
        if (!row.source_database.empty()) {
            conn.use_database(row.source_database);
        }
        const json pipeline = fetch_pipeline_snapshot(log_pg, row.catalog_id);
        const long long kafka_lag = pipeline.value("kafka_consumer_lag", 0LL);

        const auto pk_cols = split_pk_columns(row.pk_columns);
        const auto cols = fetch_mssql_columns(conn.handle, row.source_schema, row.source_table);
        std::vector<std::string> col_names;
        col_names.reserve(cols.size());
        for (const auto& col : cols) {
            col_names.push_back(col.name);
        }
        const auto ts_column =
            resolve_ts_column(row.engine_meta, col_names, lake_pg, row.source_schema, row.source_table);
        bool pk_numeric = false;
        if (pk_cols.size() == 1) {
            for (const auto& col : cols) {
                if (col.name == pk_cols[0]) {
                    pk_numeric = mssql_type_numeric_pk(col.mssql_type);
                    break;
                }
            }
        }

        long long source_count = -1;
        std::optional<std::string> max_pk_source;
        std::optional<std::string> max_ts_source;
        std::string source_read_at;
        {
            MssqlSnapshotTxn source_txn(conn, retry);
            source_count = fetch_mssql_count(conn, row.source_schema, row.source_table, retry);
            if (pk_cols.size() == 1 && pk_numeric) {
                const std::string sql = "SELECT MAX([" + pk_cols[0] + "]) FROM [" + row.source_schema + "].[" +
                                        row.source_table + "]";
                max_pk_source = fetch_mssql_scalar(conn, sql, retry);
            }
            if (ts_column.has_value()) {
                const std::string sql = "SELECT CONVERT(varchar(33), MAX([" + *ts_column + "]), 127) FROM [" +
                                        row.source_schema + "].[" + row.source_table + "]";
                max_ts_source = fetch_mssql_scalar(conn, sql, retry);
            }
            source_read_at = source_txn.read_at();
            source_txn.commit();
        }

        long long lake_count = -1;
        std::optional<std::string> max_pk_lake;
        std::optional<std::string> max_ts_lake;
        std::string lake_read_at;
        {
            LakeSnapshotTxn lake_txn(lake_pg);
            lake_count = full_load::lake_table_row_count(lake_pg, row.source_schema, row.source_table);
            if (pk_cols.size() == 1 && pk_numeric) {
                max_pk_lake = lake_max_pk(lake_pg, row.source_schema, row.source_table, pk_cols[0]);
            }
            if (ts_column.has_value()) {
                max_ts_lake = lake_max_ts(lake_pg, row.source_schema, row.source_table, *ts_column);
            }
            lake_read_at = lake_txn.read_at();
            lake_txn.commit();
        }

        const Status raw_row_count_status = evaluate_row_count(source_count, lake_count);
        checks["engine"] = "mssql";
        checks["source_snapshot"] = "repeatable_read";
        checks["lake_snapshot"] = "repeatable_read";
        checks["source_read_at"] = source_read_at;
        checks["lake_read_at"] = lake_read_at;
        const Status row_count_status = apply_row_count_gating(raw_row_count_status, kafka_lag, checks);

        Status max_pk_status = kSkip;
        if (max_pk_source.has_value() || max_pk_lake.has_value()) {
            max_pk_status = evaluate_max_pk(max_pk_source, max_pk_lake);
        }

        std::optional<int> max_ts_lag;
        Status max_ts_status = kSkip;
        if (ts_column.has_value()) {
            if (max_ts_source.has_value() && max_ts_lake.has_value()) {
                max_ts_lag = lag_seconds_on_pg(lake_pg, *max_ts_source, *max_ts_lake);
            }
            max_ts_status = evaluate_max_ts_lag(max_ts_lag);
        }

        const Status global = combine_status(row_count_status, combine_status(max_pk_status, max_ts_status));
        if (!insert_reconciliation_row(
                log_pg, batch_id, row, global, source_count, lake_count, row_count_status, max_pk_source,
                max_pk_lake, max_pk_status, ts_column, max_ts_source, max_ts_lake, max_ts_lag, max_ts_status,
                pipeline, checks, full_load::elapsed_ms(start), std::nullopt)) {
            stats.errors += 1;
            return false;
        }
        bump_status(stats, global);
        return true;
    } catch (const std::exception& ex) {
        error = ex.what();
    }
    insert_reconciliation_row(
        log_pg, batch_id, row, kFail, -1, -1, kFail, std::nullopt, std::nullopt, kSkip, std::nullopt,
        std::nullopt, std::nullopt, std::nullopt, kSkip, fetch_pipeline_snapshot(log_pg, row.catalog_id), checks,
        full_load::elapsed_ms(start), error);
    stats.errors += 1;
    stats.fail += 1;
    return false;
}
#endif

#ifdef HAVE_MONGOC
bool reconcile_table_mongo(
    PGconn* log_pg,
    PGconn* lake_pg,
    MongoConn& conn,
    const std::string& batch_id,
    const ReconcileCandidate& row,
    TableRunStats& stats) {
    const auto start = std::chrono::steady_clock::now();
    json checks = json::object();
    if (should_skip_table(log_pg, row, checks)) {
        insert_reconciliation_row(
            log_pg, batch_id, row, kSkip, -1, -1, kSkip, std::nullopt, std::nullopt, kSkip, std::nullopt,
            std::nullopt, std::nullopt, std::nullopt, kSkip, fetch_pipeline_snapshot(log_pg, row.catalog_id),
            checks, full_load::elapsed_ms(start), std::nullopt);
        bump_status(stats, kSkip);
        return true;
    }

    std::optional<std::string> error;
    try {
        const json pipeline = fetch_pipeline_snapshot(log_pg, row.catalog_id);
        const long long kafka_lag = pipeline.value("kafka_consumer_lag", 0LL);

        const std::string db = row.source_database.empty() ? row.source_schema : row.source_database;
        mongoc_collection_t* coll = conn.collection(db, row.source_table);

        std::vector<std::string> col_names;
        {
            bson_t empty = BSON_INITIALIZER;
            bson_t opts = BSON_INITIALIZER;
            bson_append_int32(&opts, "limit", 1, 1);
            mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &empty, &opts, nullptr);
            bson_destroy(&empty);
            bson_destroy(&opts);
            const bson_t* doc = nullptr;
            if (mongoc_cursor_next(cursor, &doc) && doc) {
                bson_iter_t iter;
                if (bson_iter_init(&iter, doc)) {
                    while (bson_iter_next(&iter)) {
                        col_names.push_back(bson_iter_key(&iter));
                    }
                }
            }
            mongoc_cursor_destroy(cursor);
        }
        const auto ts_column =
            resolve_ts_column(row.engine_meta, col_names, lake_pg, row.source_schema, row.source_table);
        const auto pk_cols = split_pk_columns(row.pk_columns);

        const long long source_count = fetch_mongo_count(coll);
        std::optional<std::string> max_pk_source;
        if (pk_cols.size() == 1 && pk_cols[0] == "mongo_id") {
            max_pk_source = fetch_mongo_max_id(coll);
        } else if (pk_cols.size() == 1) {
            max_pk_source = fetch_mongo_max_field(coll, pk_cols[0]);
        }
        std::optional<std::string> max_ts_source;
        if (ts_column.has_value()) {
            max_ts_source = fetch_mongo_max_field(coll, *ts_column);
        }
        const std::string source_read_at = utc_now_iso();
        mongoc_collection_destroy(coll);

        long long lake_count = -1;
        std::optional<std::string> max_pk_lake;
        std::optional<std::string> max_ts_lake;
        std::string lake_read_at;
        {
            LakeSnapshotTxn lake_txn(lake_pg);
            lake_count = full_load::lake_table_row_count(lake_pg, row.source_schema, row.source_table);
            if (pk_cols.size() == 1) {
                max_pk_lake = lake_max_pk(
                    lake_pg,
                    row.source_schema,
                    row.source_table,
                    pk_cols[0] == "mongo_id" ? "mongo_id" : pk_cols[0]);
            }
            if (ts_column.has_value()) {
                max_ts_lake = lake_max_ts(lake_pg, row.source_schema, row.source_table, *ts_column);
            }
            lake_read_at = lake_txn.read_at();
            lake_txn.commit();
        }

        const Status raw_row_count_status = evaluate_row_count(source_count, lake_count);
        checks["engine"] = "mongodb";
        checks["source_snapshot"] = "best_effort";
        checks["lake_snapshot"] = "repeatable_read";
        checks["source_read_at"] = source_read_at;
        checks["lake_read_at"] = lake_read_at;
        const Status row_count_status = apply_row_count_gating(raw_row_count_status, kafka_lag, checks);

        Status max_pk_status = kSkip;
        if (max_pk_source.has_value() || max_pk_lake.has_value()) {
            max_pk_status = evaluate_max_pk(max_pk_source, max_pk_lake);
        }

        std::optional<int> max_ts_lag;
        Status max_ts_status = kSkip;
        if (ts_column.has_value()) {
            if (max_ts_source.has_value() && max_ts_lake.has_value()) {
                max_ts_lag = lag_seconds_on_pg(lake_pg, *max_ts_source, *max_ts_lake);
            }
            max_ts_status = evaluate_max_ts_lag(max_ts_lag);
        }

        const Status global = combine_status(row_count_status, combine_status(max_pk_status, max_ts_status));
        if (!insert_reconciliation_row(
                log_pg, batch_id, row, global, source_count, lake_count, row_count_status, max_pk_source,
                max_pk_lake, max_pk_status, ts_column, max_ts_source, max_ts_lake, max_ts_lag, max_ts_status,
                pipeline, checks, full_load::elapsed_ms(start), std::nullopt)) {
            stats.errors += 1;
            return false;
        }
        bump_status(stats, global);
        return true;
    } catch (const std::exception& ex) {
        error = ex.what();
    }
    insert_reconciliation_row(
        log_pg, batch_id, row, kFail, -1, -1, kFail, std::nullopt, std::nullopt, kSkip, std::nullopt,
        std::nullopt, std::nullopt, std::nullopt, kSkip, fetch_pipeline_snapshot(log_pg, row.catalog_id), checks,
        full_load::elapsed_ms(start), error);
    stats.errors += 1;
    stats.fail += 1;
    return false;
}
#endif

}  // namespace

int run_reconcile_lite(
    const AppConfig& cfg,
    PGconn* log_pg,
    PGconn* lake_pg,
    const std::string& batch_id,
    const std::optional<std::string>& conn_id_filter,
    CatalogHotTier hot_tier,
    int sample_pct) {
    const int pct = std::clamp(sample_pct <= 0 ? 100 : sample_pct, 1, 100);
    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "reconcile",
        .message = "reconcile-lite started",
        .batch_id = batch_id,
        .conn_id = conn_id_filter,
        .context = {{"sample_pct", pct}, {"hot_only", hot_tier == CatalogHotTier::HotOnly},
                    {"cold_only", hot_tier == CatalogHotTier::ColdOnly}},
    });

    const long long pruned = prune_reconciliation(log_pg, pipeline_defaults::kReconcileLiteRetentionDays);
    if (pruned > 0) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "reconcile",
            .message = "reconciliation history pruned",
            .batch_id = batch_id,
            .context = {{"rows_pruned", pruned}},
        });
    }

    auto candidates = load_candidates(log_pg, conn_id_filter, hot_tier);
    candidates.erase(
        std::remove_if(
            candidates.begin(),
            candidates.end(),
            [&](const ReconcileCandidate& row) { return !in_sample(row.catalog_id, pct); }),
        candidates.end());

    TableRunStats stats;
    std::map<std::string, std::vector<ReconcileCandidate>> by_conn;
    for (auto& row : candidates) {
        by_conn[row.conn_id].push_back(std::move(row));
    }

    for (const auto& [conn_id, rows] : by_conn) {
        const std::string engine = conn_engine(cfg, conn_id);
        try {
            if (engine == "mariadb") {
                const MariaDbSource* src = find_mariadb_source(cfg, conn_id);
                if (!src) {
                    stats.errors += static_cast<int>(rows.size());
                    continue;
                }
                MariaDbConn conn(*src);
                for (const auto& row : rows) {
                    reconcile_table_mariadb(log_pg, lake_pg, conn, batch_id, row, stats);
                }
            } else if (engine == "mssql") {
#ifdef HAVE_FREETDS
                const MssqlSource* src = find_mssql_source(cfg, conn_id);
                if (!src) {
                    stats.errors += static_cast<int>(rows.size());
                    continue;
                }
                MssqlConn conn(*src);
                for (const auto& row : rows) {
                    reconcile_table_mssql(log_pg, lake_pg, conn, batch_id, row, stats);
                }
#else
                log_write(log_pg, {
                    .level = LogLevel::Warning,
                    .component = "reconcile",
                    .message = "reconcile-lite skipped conn: built without FreeTDS",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                });
                stats.skip += static_cast<int>(rows.size());
#endif
            } else if (engine == "mongodb") {
#ifdef HAVE_MONGOC
                const MongoSource* src = find_mongo_source(cfg, conn_id);
                if (!src) {
                    stats.errors += static_cast<int>(rows.size());
                    continue;
                }
                MongoConn conn(*src);
                for (const auto& row : rows) {
                    reconcile_table_mongo(log_pg, lake_pg, conn, batch_id, row, stats);
                }
#else
                log_write(log_pg, {
                    .level = LogLevel::Warning,
                    .component = "reconcile",
                    .message = "reconcile-lite skipped conn: built without libmongoc",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                });
                stats.skip += static_cast<int>(rows.size());
#endif
            }
        } catch (const std::exception& ex) {
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "reconcile",
                .message = "reconcile-lite conn failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .context = {{"error", ex.what()}},
            });
            stats.errors += static_cast<int>(rows.size());
        }
    }

    const LogLevel end_level =
        (stats.errors > 0 || stats.fail > 0) ? LogLevel::Warning : LogLevel::Info;
    log_write(log_pg, {
        .level = end_level,
        .component = "reconcile",
        .message = stats.errors > 0 ? "reconcile-lite completed with errors" : "reconcile-lite completed",
        .batch_id = batch_id,
        .conn_id = conn_id_filter,
        .context = {{"tables_checked", stats.checked},
                    {"tables_ok", stats.ok},
                    {"tables_warn", stats.warn},
                    {"tables_fail", stats.fail},
                    {"tables_skip", stats.skip},
                    {"errors", stats.errors}},
    });
    return stats.errors > 0 ? 1 : 0;
}
