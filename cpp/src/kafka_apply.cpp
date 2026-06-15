#include "kafka_apply_detail.hpp"
#include "kafka_apply.hpp"
#include "capture_common.hpp"
#include "lake_columns.hpp"
#include "mariadb_datetime.hpp"
#include "mssql_conn.hpp"
#include "mariadb_schema.hpp"
#include "mssql_lake.hpp"
#include "mongo_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace kafka_apply_detail {

using json = nlohmann::json;
using TableKey = std::pair<std::string, std::string>;

struct TableBatchMeta {
    long long catalog_id{0};
    std::string schema_name;
    std::string table_name;
    std::string catalog_source_schema;
    std::string catalog_source_table;
    std::string pk_columns;
    std::vector<std::string> lake_pk;
    std::vector<std::string> data_cols;
    std::map<std::string, std::string> col_types;
};

struct TableHealthSnapshot {
    bool is_stale{false};
    bool is_quarantined{false};
    int apply_lag_seconds{0};
    int capture_lag_seconds{0};
    std::string apply_status;
    std::string reconciliation_rag{"UNKNOWN"};
    long long reconcile_row_delta{0};
    bool catalog_active{false};
    bool cdc_enabled{false};
    int seconds_since_last_apply{-1};
};

struct BatchStatsMetrics {
    long long kafka_consumer_lag{0};
    int dedup_skipped{0};
    int parse_skipped{0};
    int dropped_unrecoverable{0};
};

std::string table_state_key(const std::string& schema, const std::string& table) {
    return schema + "|" + table;
}

std::string reconciliation_status_to_rag(const std::string& status) {
    if (status == "ok") {
        return "GREEN";
    }
    if (status == "warn") {
        return "AMBER";
    }
    if (status == "fail") {
        return "RED";
    }
    return "UNKNOWN";
}

TableHealthSnapshot fetch_table_health_by_catalog_id(
    PGconn* pg,
    long long catalog_id,
    int staleness_seconds,
    int inactive_seconds) {
    (void)inactive_seconds;
    TableHealthSnapshot out;
    if (catalog_id <= 0) {
        return out;
    }
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT
            ap.status::text,
            ap.apply_lag_seconds,
            extract(epoch FROM (now() - ap.last_applied_at))::integer,
            c.active,
            c.cdc_enabled,
            COALESCE(rag.status, 'skip') AS reconcile_status,
            COALESCE(rag.row_count_delta, 0),
            COALESCE(
                CASE
                    WHEN c.db_engine = 'mssql' THEN mssql_lag.lag_seconds
                    WHEN c.db_engine = 'mongodb' THEN mongo_lag.lag_seconds
                    ELSE cp.capture_lag_seconds
                END,
                0
            )
        FROM cdc_catalog.catalog c
        LEFT JOIN cdc_catalog.apply_position ap ON ap.catalog_id = c.catalog_id
        LEFT JOIN cdc_catalog.capture_position cp ON cp.conn_id = c.conn_id
        LEFT JOIN LATERAL (
            SELECT GREATEST(
                0,
                extract(epoch FROM (now() - l.updated_at))::integer
            ) AS lag_seconds
            FROM cdc_catalog.cdc_mssql_lsn l
            WHERE c.db_engine = 'mssql'
              AND l.conn_id = c.conn_id
              AND l.database = c.source_database
              AND l.schema_name = c.source_schema
              AND l.table_name = c.source_table
        ) mssql_lag ON true
        LEFT JOIN LATERAL (
            SELECT GREATEST(
                0,
                extract(epoch FROM (now() - r.updated_at))::integer
            ) AS lag_seconds
            FROM cdc_catalog.cdc_mongo_resume r
            WHERE c.db_engine = 'mongodb'
              AND r.conn_id = c.conn_id
              AND r.database = c.source_database
              AND r.collection = c.source_table
        ) mongo_lag ON true
        LEFT JOIN LATERAL (
            SELECT rr.status, rr.row_count_delta
            FROM cdc_catalog.reconciliation_result rr
            JOIN cdc_catalog.reconciliation_run rn ON rn.run_id = rr.run_id
            WHERE rr.conn_id = c.conn_id
              AND rr.source_schema = COALESCE(NULLIF(c.source_schema, ''), c.source_database)
              AND rr.source_table = c.source_table
              AND rn.finished_at IS NOT NULL
            ORDER BY rn.finished_at DESC
            LIMIT 1
        ) rag ON true
        WHERE c.catalog_id = $1::bigint
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
    const char* status = PQgetvalue(res, 0, 0);
    out.apply_status = status ? status : "";
    const char* lag_val = PQgetvalue(res, 0, 1);
    out.apply_lag_seconds = lag_val ? std::atoi(lag_val) : 0;
    if (PQgetvalue(res, 0, 2) && PQgetvalue(res, 0, 2)[0]) {
        out.seconds_since_last_apply = std::atoi(PQgetvalue(res, 0, 2));
    }
    const char* active_val = PQgetvalue(res, 0, 3);
    out.catalog_active = active_val && active_val[0] == 't';
    const char* cdc_val = PQgetvalue(res, 0, 4);
    out.cdc_enabled = cdc_val && cdc_val[0] == 't';
    const char* rag_val = PQgetvalue(res, 0, 5);
    out.reconciliation_rag = reconciliation_status_to_rag(rag_val ? rag_val : "");
    if (PQgetvalue(res, 0, 6) && PQgetvalue(res, 0, 6)[0]) {
        out.reconcile_row_delta = std::atoll(PQgetvalue(res, 0, 6));
    }
    const char* capt_val = PQgetvalue(res, 0, 7);
    out.capture_lag_seconds = capt_val ? std::atoi(capt_val) : 0;
    out.is_quarantined = out.apply_status == "quarantined";
    if (out.apply_status == "stale" || out.apply_status == "lagging" || out.apply_status == "gap_detected") {
        out.is_stale = true;
    } else if (out.seconds_since_last_apply >= 0 && out.seconds_since_last_apply > staleness_seconds) {
        out.is_stale = true;
    }
    PQclear(res);
    return out;
}

std::string unique_load_timestamptz(long long ts_ms, int row_index) {
    if (ts_ms > 0) {
        return epoch_ms_to_timestamptz(ts_ms + row_index);
    }
    const auto now = std::chrono::system_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return epoch_ms_to_timestamptz(ms + row_index);
}

std::string csv_escape_local(const std::string& value) {
    bool quote = value.empty();
    for (char c : value) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') {
            quote = true;
            break;
        }
    }
    if (!quote) {
        return value;
    }
    std::string out = "\"";
    for (char c : value) {
        out += (c == '"') ? "\"\"" : std::string(1, c);
    }
    out += "\"";
    return out;
}

namespace {

bool is_integer_pg_type(const std::string& pg_type) {
    return pg_type == "int2" || pg_type == "int4" || pg_type == "int8" || pg_type == "INTEGER" ||
           pg_type == "BIGINT" || pg_type == "SMALLINT";
}

bool is_numeric_pg_type(const std::string& pg_type) {
    return is_integer_pg_type(pg_type) || pg_type == "NUMERIC" || pg_type == "DECIMAL" ||
           pg_type == "numeric" || pg_type == "decimal" || pg_type == "DOUBLE PRECISION" ||
           pg_type == "REAL" || pg_type == "float8" || pg_type == "float4";
}

bool is_bytea_pg_type(const std::string& pg_type) {
    if (pg_type.empty()) {
        return false;
    }
    std::string upper = pg_type;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) { return std::toupper(c); });
    return upper == "BYTEA";
}

/** e.g. "-1 (4294967295)" from unsigned/signed display → "4294967295". */
std::string sanitize_numeric_string_for_pg(const std::string& s) {
    if (s.empty()) {
        return s;
    }
    const auto open = s.find('(');
    if (open != std::string::npos) {
        const auto close = s.find(')', open + 1);
        if (close != std::string::npos) {
            const std::string inner = s.substr(open + 1, close - open - 1);
            bool ok = !inner.empty();
            for (char c : inner) {
                if (c != '-' && c != '.' && !std::isdigit(static_cast<unsigned char>(c))) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return inner;
            }
        }
    }
    std::size_t end = 0;
    while (end < s.size() && std::isspace(static_cast<unsigned char>(s[end]))) {
        ++end;
    }
    if (end < s.size() && (s[end] == '-' || s[end] == '+')) {
        ++end;
    }
    const std::size_t start = end;
    while (end < s.size() &&
           (std::isdigit(static_cast<unsigned char>(s[end])) || s[end] == '.' || s[end] == 'e' ||
            s[end] == 'E' || s[end] == '-' || s[end] == '+')) {
        ++end;
    }
    if (end > start) {
        return s.substr(start, end - start);
    }
    return s;
}

}  // namespace

std::string format_boolean_copy_cell(const json& val) {
    if (val.is_boolean()) {
        return val.get<bool>() ? "t" : "f";
    }
    if (val.is_number_integer() || val.is_number_unsigned()) {
        return val.get<long long>() != 0 ? "t" : "f";
    }
    if (val.is_string()) {
        const std::string s = val.get<std::string>();
        if (s.empty()) {
            return "f";
        }
        if (s == "0" || s == "false" || s == "FALSE" || s == "f" || s == "F" || s == "no" || s == "NO") {
            return "f";
        }
        if (s == "1" || s == "true" || s == "TRUE" || s == "t" || s == "T" || s == "yes" || s == "YES") {
            return "t";
        }
        return "f";
    }
    return "f";
}

std::string json_cell_csv(const json& val_in, const std::string& pg_type, bool mssql_text) {
    json val = val_in;
    if (pg_type == "BOOLEAN" || pg_type == "bool") {
        if (val.is_null()) {
            return "";
        }
        return format_boolean_copy_cell(val);
    }
    if (val.is_string() && is_numeric_pg_type(pg_type)) {
        const std::string s = sanitize_numeric_string_for_pg(val.get<std::string>());
        if (s.empty()) {
            val = nullptr;
        } else if (is_integer_pg_type(pg_type)) {
            try {
                val = std::stoll(s);
            } catch (...) {
                val = s;
            }
        } else {
            val = s;
        }
    }
    if (val.is_null()) {
        return "";
    }
    if (val.is_boolean()) {
        return format_boolean_copy_cell(val);
    }
    if (val.is_number_integer() || val.is_number_unsigned()) {
        const long long n = val.get<long long>();
        if (pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP") {
            std::string t = epoch_to_timestamptz(n);
            if (pg_type == "TIMESTAMP") {
                if (const auto pos = t.rfind('+'); pos != std::string::npos) {
                    t = t.substr(0, pos);
                }
            }
            return csv_escape_local(t);
        }
        if (pg_type == "DATE") {
            return csv_escape_local(epoch_to_date(n));
        }
        return std::to_string(n);
    }
    if (val.is_number_float()) {
        return std::to_string(val.get<double>());
    }
    if (is_bytea_pg_type(pg_type)) {
        if (val.is_string()) {
            return mariadb_bytea_to_copy_csv(val.get<std::string>());
        }
        return "";
    }
    std::string s = val.is_string() ? val.get<std::string>() : val.dump();
    if (pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP" || pg_type == "DATE") {
        const std::string norm = normalize_text_for_pg(s, pg_type);
        if ((pg_type == "DATE" || pg_type == "TIMESTAMPTZ" || pg_type == "TIMESTAMP") && norm.empty()) {
            return "";
        }
        s = norm;
    } else if (mssql_text) {
        s = sanitize_mssql_text_for_pg(s);
    } else {
        s = sanitize_mariadb_text_for_pg(s);
    }
    return csv_escape_local(s);
}

/** Source/business PK from catalog — excludes lake-only _dl_* columns (e.g. _dl_load_timestamp). */
std::vector<std::string> business_pk_cols(const TableBatchMeta& meta) {
    auto cols = split_pk_columns(meta.pk_columns);
    if (!cols.empty()) {
        return cols;
    }
    for (const auto& col : meta.lake_pk) {
        if (col.rfind("_dl_", 0) != 0) {
            cols.push_back(col);
        }
    }
    return cols;
}

std::vector<std::string> fetch_lake_pk(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
        ORDER BY array_position(i.indkey::smallint[], a.attnum)
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<std::string> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        out.emplace_back(PQgetvalue(res, i, 0));
    }
    PQclear(res);
    return out;
}

std::vector<std::string> fetch_lake_data_cols(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
          AND column_name NOT LIKE '_dl\_%' ESCAPE '\'
        ORDER BY ordinal_position
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<std::string> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        out.emplace_back(PQgetvalue(res, i, 0));
    }
    PQclear(res);
    return out;
}

std::map<std::string, std::string> fetch_lake_col_types(
    PGconn* pg,
    const std::string& schema,
    const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
          AND column_name NOT LIKE '_dl\_%' ESCAPE '\'
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::map<std::string, std::string> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        const char* udt = PQgetvalue(res, i, 1);
        std::string pg_type;
        if (udt && std::string(udt) == "timestamptz") {
            pg_type = "TIMESTAMPTZ";
        } else if (udt && std::string(udt) == "timestamp") {
            pg_type = "TIMESTAMP";
        } else if (udt && std::string(udt) == "date") {
            pg_type = "DATE";
        } else if (udt && std::string(udt) == "bytea") {
            pg_type = "BYTEA";
        } else {
            pg_type = udt ? udt : "";
        }
        out[PQgetvalue(res, i, 0)] = pg_type;
    }
    PQclear(res);
    return out;
}

void ensure_partitions(PGconn* pg, const std::string& schema, const std::string& table) {
    static std::unordered_set<std::string> ready;
    const std::string key = schema + "|" + table;
    if (ready.count(key)) {
        return;
    }
    const char* check_vals[] = {schema.c_str(), table.c_str()};
    PGresult* chk = PQexecParams(
        pg,
        R"(SELECT c.relkind = 'p'
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND c.relname = $2)",
        2,
        nullptr,
        check_vals,
        nullptr,
        nullptr,
        0);
    bool partitioned = false;
    if (chk && PQresultStatus(chk) == PGRES_TUPLES_OK && PQntuples(chk) > 0) {
        partitioned = PQgetvalue(chk, 0, 0)[0] == 't';
    }
    if (chk) {
        PQclear(chk);
    }
    if (!partitioned) {
        ready.insert(key);
        return;
    }

    const char* vals[] = {schema.c_str(), table.c_str(), "3"};
    PGresult* res = PQexecParams(
        pg, "SELECT lake.ensure_monthly_partitions($1, $2, $3::integer)", 3, nullptr, vals, nullptr, nullptr, 0);
    if (res) {
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(pg);
            PQclear(res);
            throw std::runtime_error("ensure_monthly_partitions failed: " + err);
        }
        PQclear(res);
    }
    ready.insert(key);
}

void copy_csv_lines(PGconn* pg, const std::string& copy_sql, const std::vector<std::string>& lines) {
    PGresult* res = PQexec(pg, copy_sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_COPY_IN) {
        std::string err = pg ? PQerrorMessage(pg) : "no pg";
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("COPY start failed: " + err);
    }
    PQclear(res);
    bool copy_ok = true;
    for (const auto& line : lines) {
        if (PQputCopyData(pg, line.data(), static_cast<int>(line.size())) != 1 ||
            PQputCopyData(pg, "\n", 1) != 1) {
            copy_ok = false;
            break;
        }
    }
    if (!copy_ok) {
        PQputCopyEnd(pg, "abort");
        while (PGresult* r = PQgetResult(pg)) {
            PQclear(r);
        }
        throw std::runtime_error(std::string("PQputCopyData failed: ") + PQerrorMessage(pg));
    }
    if (PQputCopyEnd(pg, nullptr) != 1) {
        PQputCopyEnd(pg, "abort");
        while (PGresult* r = PQgetResult(pg)) {
            PQclear(r);
        }
        throw std::runtime_error(std::string("PQputCopyEnd failed: ") + PQerrorMessage(pg));
    }
    PGresult* end_res = PQgetResult(pg);
    while (end_res) {
        if (PQresultStatus(end_res) != PGRES_COMMAND_OK) {
            std::string err = PQerrorMessage(pg);
            PQclear(end_res);
            while (PGresult* r = PQgetResult(pg)) {
                PQclear(r);
            }
            throw std::runtime_error("COPY failed: " + err);
        }
        PQclear(end_res);
        end_res = PQgetResult(pg);
    }
}

void record_applied_events(PGconn* pg, const std::string& conn_id, const std::vector<ApplyEvent>& events) {
    if (events.empty()) {
        return;
    }
    pg_exec(
        pg,
        "CREATE TEMP TABLE cdc_audit_staging (LIKE cdc_catalog.cdc_applied_events INCLUDING DEFAULTS) ON COMMIT "
        "DROP");
    std::vector<std::string> lines;
    lines.reserve(events.size());
    for (const auto& e : events) {
        std::ostringstream line;
        line << csv_escape_local(e.event_id) << ',' << csv_escape_local(conn_id) << ','
             << csv_escape_local(e.schema_name) << ',' << csv_escape_local(e.table_name) << ','
             << csv_escape_local(e.op) << ',' << (e.gtid.empty() ? "" : csv_escape_local(e.gtid)) << ','
             << csv_escape_local(e.topic) << ',' << e.partition << ',' << e.offset;
        lines.push_back(line.str());
    }
    copy_csv_lines(
        pg,
        "COPY cdc_audit_staging (event_id, conn_id, source_schema, source_table, op, gtid, kafka_topic, "
        "kafka_partition, kafka_offset) FROM STDIN WITH (FORMAT csv)",
        lines);
    pg_exec(
        pg,
        R"(
        INSERT INTO cdc_catalog.cdc_applied_events
            (event_id, conn_id, source_schema, source_table, op, gtid, kafka_topic, kafka_partition, kafka_offset)
        SELECT event_id, conn_id, source_schema, source_table, op, gtid, kafka_topic, kafka_partition, kafka_offset
        FROM cdc_audit_staging
        ON CONFLICT (event_id) DO NOTHING
        )");
}

struct ApplyOpCounts {
    long long inserts{0};
    long long updates{0};
    long long deletes{0};
};

ApplyOpCounts count_apply_ops(const std::vector<ApplyEvent>& events) {
    ApplyOpCounts counts;
    for (const auto& e : events) {
        if (e.op == "d") {
            counts.deletes += 1;
        } else if (e.op == "u") {
            counts.updates += 1;
        } else {
            counts.inserts += 1;
        }
    }
    return counts;
}

const ApplyEvent* max_kafka_audit_position(const std::vector<ApplyEvent>& events) {
    if (events.empty()) {
        return nullptr;
    }
    const ApplyEvent* best = &events.front();
    for (const auto& e : events) {
        if (e.offset > best->offset ||
            (e.offset == best->offset && e.partition > best->partition)) {
            best = &e;
        }
    }
    return best;
}

void update_apply_position(
    PGconn* pg,
    long long catalog_id,
    const std::string& topic,
    int partition,
    long long offset,
    const std::string& gtid) {
    const std::string part = std::to_string(partition);
    const std::string off = std::to_string(offset);
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {topic.c_str(), part.c_str(), off.c_str(), gtid.c_str(), cid.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        UPDATE cdc_catalog.apply_position
        SET kafka_topic = $1,
            kafka_partition = $2,
            kafka_offset = $3::bigint,
            last_applied_gtid = COALESCE(NULLIF($4, ''), last_applied_gtid),
            last_applied_at = now(),
            apply_lag_seconds = 0,
            status = 'healthy'::cdc_catalog.cdc_health_status,
            last_error = NULL,
            updated_at = now()
        WHERE catalog_id = $5::bigint
        )",
        5,
        vals);
}

long long events_per_minute(long long events_total, long long duration_ms) {
    if (events_total <= 0) {
        return 0;
    }
    const long long ms = duration_ms > 0 ? duration_ms : 1;
    return (events_total * 60'000LL) / ms;
}

json trim_sample_json_value(const json& value, std::size_t max_len) {
    if (value.is_string()) {
        std::string text = sanitize_utf8_for_json(value.get<std::string>());
        if (text.size() > max_len) {
            text.resize(max_len);
            text += "…";
        }
        return text;
    }
    if (value.is_number_integer()) {
        return value.get<long long>();
    }
    if (value.is_number_unsigned()) {
        return value.get<unsigned long long>();
    }
    if (value.is_number_float()) {
        return value.get<double>();
    }
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_null()) {
        return nullptr;
    }
    return json("…");
}

json build_apply_row_sample(
    const std::vector<ApplyEvent>& events,
    int max_rows = 2,
    int max_keys = 4,
    std::size_t max_val_len = 48) {
    json sample = json::array();
    if (events.empty() || max_rows <= 0) {
        return sample;
    }
    const int start = std::max(0, static_cast<int>(events.size()) - max_rows);
    for (int i = start; i < static_cast<int>(events.size()); ++i) {
        const ApplyEvent& event = events[static_cast<std::size_t>(i)];
        json row_snippet = json::object();
        if (event.row.is_object()) {
            int keys = 0;
            for (auto it = event.row.begin(); it != event.row.end() && keys < max_keys; ++it, ++keys) {
                row_snippet[it.key()] = trim_sample_json_value(*it, max_val_len);
            }
            if (event.row.size() > static_cast<std::size_t>(max_keys)) {
                row_snippet["_more_keys"] =
                    static_cast<int>(event.row.size()) - max_keys;
            }
        }
        sample.push_back({{"op", event.op}, {"row", row_snippet}});
    }
    return sample;
}

void insert_apply_batch_stats(
    PGconn* pg,
    const std::string& batch_id,
    const std::string& conn_id,
    long long catalog_id,
    const std::string& source_schema,
    const std::string& source_table,
    const ApplyOpCounts& counts,
    long long duration_ms,
    const std::string& topic,
    int partition,
    long long offset,
    const TableHealthSnapshot& health,
    bool is_inactive,
    int events_seen_in_slice,
    const BatchStatsMetrics& metrics,
    const json& context = json::object()) {
    const long long total = counts.inserts + counts.updates + counts.deletes;
    const long long epm = events_per_minute(total, duration_ms);
    const std::string cid = catalog_id > 0 ? std::to_string(catalog_id) : "";
    const std::string part = std::to_string(partition);
    const std::string off = offset >= 0 ? std::to_string(offset) : "";
    const std::string dur = std::to_string(duration_ms);
    const std::string ins = std::to_string(counts.inserts);
    const std::string upd = std::to_string(counts.updates);
    const std::string del = std::to_string(counts.deletes);
    const std::string tot = std::to_string(total);
    const std::string rate = std::to_string(epm);
    const std::string lag = std::to_string(health.apply_lag_seconds);
    const std::string seen = std::to_string(events_seen_in_slice);
    const std::string stale = health.is_stale ? "true" : "false";
    const std::string inactive = is_inactive ? "true" : "false";
    const std::string quarantined = health.is_quarantined ? "true" : "false";
    const std::string active = health.catalog_active ? "true" : "false";
    const std::string cdc_on = health.cdc_enabled ? "true" : "false";
    const std::string capture_lag = std::to_string(health.capture_lag_seconds);
    const std::string kafka_lag = std::to_string(metrics.kafka_consumer_lag);
    const std::string row_delta = std::to_string(health.reconcile_row_delta);
    const std::string dedup_skip = std::to_string(metrics.dedup_skipped);
    const std::string parse_skip = std::to_string(metrics.parse_skipped);
    const std::string dropped = std::to_string(metrics.dropped_unrecoverable);
    const std::string ctx_json = context.is_null() ? "{}" : context.dump();
    const char* vals[] = {
        batch_id.c_str(),
        conn_id.c_str(),
        cid.empty() ? nullptr : cid.c_str(),
        source_schema.c_str(),
        source_table.c_str(),
        ins.c_str(),
        upd.c_str(),
        del.c_str(),
        tot.c_str(),
        dur.c_str(),
        rate.c_str(),
        topic.empty() ? nullptr : topic.c_str(),
        part.c_str(),
        off.empty() ? nullptr : off.c_str(),
        stale.c_str(),
        inactive.c_str(),
        quarantined.c_str(),
        health.reconciliation_rag.c_str(),
        lag.c_str(),
        health.apply_status.empty() ? nullptr : health.apply_status.c_str(),
        seen.c_str(),
        active.c_str(),
        cdc_on.c_str(),
        capture_lag.c_str(),
        kafka_lag.c_str(),
        row_delta.c_str(),
        dedup_skip.c_str(),
        parse_skip.c_str(),
        dropped.c_str(),
        ctx_json.c_str()};
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.apply_batch_stats (
            batch_id, conn_id, catalog_id, source_schema, source_table,
            events_inserts, events_updates, events_deletes, events_total,
            duration_ms, events_per_minute, kafka_topic, kafka_partition, kafka_offset,
            is_stale, is_inactive, is_quarantined, reconciliation_rag,
            apply_lag_seconds, apply_position_status, events_seen_in_slice,
            catalog_active, cdc_enabled,
            capture_lag_seconds, kafka_consumer_lag, reconcile_row_delta,
            dedup_skipped, parse_skipped, dropped_unrecoverable,
            context
        ) VALUES (
            $1, $2, NULLIF($3, '')::bigint, $4, $5,
            $6::bigint, $7::bigint, $8::bigint, $9::bigint,
            $10::bigint, $11::bigint, $12, NULLIF($13, '')::integer, NULLIF($14, '')::bigint,
            $15::boolean, $16::boolean, $17::boolean, $18,
            $19::integer, $20, $21::integer, $22::boolean, $23::boolean,
            $24::integer, $25::bigint, $26::bigint,
            $27::integer, $28::bigint, $29::bigint,
            $30::jsonb
        )
        )",
        30,
        vals);
}

void merge_from_staging(
    PGconn* pg,
    const std::string& fq,
    const std::string& staging,
    const std::vector<std::string>& all_cols,
    const std::vector<std::string>& lake_pk,
    const std::vector<std::string>& dedup_pk) {
    if (lake_pk.empty()) {
        throw std::runtime_error("merge_from_staging: lake PK required");
    }
    const std::vector<std::string>& distinct_pk = dedup_pk.empty() ? lake_pk : dedup_pk;

    std::ostringstream insert_cols;
    std::ostringstream lake_pk_sql;
    std::ostringstream dedup_pk_sql;
    for (std::size_t i = 0; i < all_cols.size(); ++i) {
        if (i) {
            insert_cols << ", ";
        }
        insert_cols << pg_ident(all_cols[i]);
    }
    for (std::size_t i = 0; i < lake_pk.size(); ++i) {
        if (i) {
            lake_pk_sql << ", ";
        }
        lake_pk_sql << pg_ident(lake_pk[i]);
    }
    for (std::size_t i = 0; i < distinct_pk.size(); ++i) {
        if (i) {
            dedup_pk_sql << ", ";
        }
        dedup_pk_sql << pg_ident(distinct_pk[i]);
    }
    std::ostringstream set_clause;
    for (const auto& col : all_cols) {
        if (std::find(lake_pk.begin(), lake_pk.end(), col) != lake_pk.end()) {
            continue;
        }
        if (!set_clause.str().empty()) {
            set_clause << ", ";
        }
        set_clause << pg_ident(col) << " = EXCLUDED." << pg_ident(col);
    }
    std::string from_clause = staging;
    if (!distinct_pk.empty()) {
        std::ostringstream order_pk;
        for (std::size_t i = 0; i < distinct_pk.size(); ++i) {
            if (i) {
                order_pk << ", ";
            }
            order_pk << pg_ident(distinct_pk[i]);
        }
        from_clause = "(SELECT DISTINCT ON (" + dedup_pk_sql.str() + ") " + insert_cols.str() + " FROM " + staging +
                      " ORDER BY " + order_pk.str() + ", ctid DESC) AS deduped";
    }
    const std::string sql = "INSERT INTO " + fq + " (" + insert_cols.str() + ") SELECT " + insert_cols.str() +
                            " FROM " + from_clause + " ON CONFLICT (" + lake_pk_sql.str() + ") DO UPDATE SET " +
                            set_clause.str();
    pg_exec(pg, sql);
}

void delete_target_by_staging_pk(
    PGconn* pg,
    const std::string& fq,
    const std::string& staging,
    const std::vector<std::string>& delete_pk_cols) {
    if (delete_pk_cols.empty()) {
        return;
    }
    std::ostringstream pk_cols_sql;
    std::ostringstream join_clause;
    for (std::size_t i = 0; i < delete_pk_cols.size(); ++i) {
        if (i) {
            pk_cols_sql << ", ";
            join_clause << " AND ";
        }
        const std::string col = pg_ident(delete_pk_cols[i]);
        pk_cols_sql << col;
        join_clause << "t." << col << " IS NOT DISTINCT FROM s." << col;
    }
    pg_exec(
        pg,
        "DELETE FROM " + fq + " t USING (SELECT DISTINCT " + pk_cols_sql.str() + " FROM " + staging + ") s WHERE " +
            join_clause.str());
}

void copy_upserts_via_staging(
    PGconn* pg,
    const std::string& fq,
    const std::string& col_list,
    const std::vector<std::string>& lines,
    const std::vector<std::string>& all_cols,
    const std::vector<std::string>& lake_pk_cols,
    const std::string& staging_name,
    const std::vector<std::string>& dedup_pk_cols = {},
    const std::vector<std::string>& delete_pk_cols = {}) {
    const std::string staging = pg_ident(staging_name);
    pg_exec(pg, "CREATE TEMP TABLE " + staging + " (LIKE " + fq + " INCLUDING DEFAULTS) ON COMMIT DROP");
    copy_csv_lines(pg, "COPY " + staging + " (" + col_list + ") FROM STDIN WITH (FORMAT csv)", lines);
    if (!delete_pk_cols.empty()) {
        delete_target_by_staging_pk(pg, fq, staging, delete_pk_cols);
    }
    if (!lake_pk_cols.empty()) {
        merge_from_staging(pg, fq, staging, all_cols, lake_pk_cols, dedup_pk_cols);
    } else {
        pg_exec(pg, "INSERT INTO " + fq + " (" + col_list + ") SELECT " + col_list + " FROM " + staging);
    }
}

long long apply_table_batch(
    PGconn* pg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    const TableBatchMeta& meta,
    const std::vector<ApplyEvent>& upserts,
    const std::vector<ApplyEvent>& deletes,
    int staging_id,
    const std::string& source_system = "MariaDB") {
    if (meta.schema_name.empty() || meta.table_name.empty()) {
        throw std::runtime_error("apply_table_batch: empty lake schema or table name");
    }
    const std::string fq = pg_ident(meta.schema_name) + "." + pg_ident(meta.table_name);
    ensure_partitions(pg, meta.schema_name, meta.table_name);
    const auto lake_pk_cols = meta.lake_pk.empty() ? split_pk_columns(meta.pk_columns) : meta.lake_pk;
    const auto business_pk = business_pk_cols(meta);
    auto delete_pk_cols = business_pk;

    int skipped_missing_pk_deletes = 0;
    if (!deletes.empty() && !delete_pk_cols.empty()) {
        const std::string del_stg = "cdc_stg_del_" + std::to_string(staging_id);
        const std::string del_fq_stg = pg_ident(del_stg);

        std::ostringstream del_col_list_ss;
        for (std::size_t i = 0; i < delete_pk_cols.size(); ++i) {
            if (i) del_col_list_ss << ", ";
            del_col_list_ss << pg_ident(delete_pk_cols[i]);
        }
        const std::string del_col_list = del_col_list_ss.str();

        std::ostringstream ct;
        ct << "CREATE TEMP TABLE " << del_fq_stg << " (";
        for (std::size_t i = 0; i < delete_pk_cols.size(); ++i) {
            if (i) ct << ", ";
            ct << pg_ident(delete_pk_cols[i]) << " "
               << (meta.col_types.count(delete_pk_cols[i]) ? meta.col_types.at(delete_pk_cols[i]) : "TEXT");
        }
        ct << ") ON COMMIT DROP";
        pg_exec(pg, ct.str());

        std::vector<std::string> del_lines;
        del_lines.reserve(deletes.size());
        for (const auto& e : deletes) {
            bool missing_pk = false;
            for (const auto& pk_col : delete_pk_cols) {
                const json* pk_val = e.row.contains(pk_col) ? &e.row[pk_col] : nullptr;
                if (!pk_val || pk_val->is_null()) {
                    missing_pk = true;
                    break;
                }
            }
            if (missing_pk) {
                skipped_missing_pk_deletes += 1;
                continue;
            }
            std::ostringstream line;
            for (std::size_t i = 0; i < delete_pk_cols.size(); ++i) {
                if (i) line << ',';
                const json* val = e.row.contains(delete_pk_cols[i]) ? &e.row[delete_pk_cols[i]] : nullptr;
                if (val) {
                    const std::string pg_type = meta.col_types.count(delete_pk_cols[i]) ? meta.col_types.at(delete_pk_cols[i]) : "";
                    line << json_cell_csv(*val, pg_type, false);
                }
            }
            del_lines.push_back(line.str());
        }
        if (!del_lines.empty()) {
            copy_csv_lines(pg, "COPY " + del_fq_stg + " (" + del_col_list + ") FROM STDIN WITH (FORMAT csv)", del_lines);
            delete_target_by_staging_pk(pg, fq, del_fq_stg, delete_pk_cols);
        }
    } else if (!deletes.empty()) {
        skipped_missing_pk_deletes = static_cast<int>(deletes.size());
    }

    const long long applied_deletes = static_cast<long long>(deletes.size()) - skipped_missing_pk_deletes;

    if (upserts.empty()) {
        return applied_deletes;
    }

    std::vector<std::string> data_cols = meta.data_cols;
    if (data_cols.empty()) {
        data_cols = fetch_lake_data_cols(pg, meta.schema_name, meta.table_name);
    }
    if (data_cols.empty()) {
        for (auto it = upserts.front().row.begin(); it != upserts.front().row.end(); ++it) {
            data_cols.push_back(it.key());
        }
    }

    for (const auto& e : upserts) {
        for (auto it = e.row.begin(); it != e.row.end(); ++it) {
            const std::string& col = it.key();
            if (col.rfind("_dl_", 0) == 0) continue;
            if (std::find(data_cols.begin(), data_cols.end(), col) != data_cols.end()) continue;
            std::string pg_type = "TEXT";
            const json& val = it.value();
            if (val.is_boolean())                pg_type = "BOOLEAN";
            else if (val.is_number_integer())    pg_type = "BIGINT";
            else if (val.is_number_float())      pg_type = "DOUBLE PRECISION";
            const std::string fq = pg_ident(meta.schema_name) + "." + pg_ident(meta.table_name);
            try {
                pg_exec(pg, "ALTER TABLE " + fq + " ADD COLUMN IF NOT EXISTS " + pg_ident(col) + " " + pg_type + " NULL");
                data_cols.push_back(col);
            } catch (const std::exception& ex) {
                if (log_pg) {
                    log_write(log_pg, {
                        .level = LogLevel::Warning,
                        .component = "cdc_kafka_apply_cpp",
                        .message = "apply schema drift add column failed",
                        .batch_id = batch_id,
                        .conn_id = conn_id,
                        .source_schema = meta.catalog_source_schema.empty() ? std::nullopt
                                                                            : std::optional(meta.catalog_source_schema),
                        .source_table = meta.catalog_source_table.empty() ? std::nullopt
                                                                          : std::optional(meta.catalog_source_table),
                        .context = {
                            {"lake_schema", meta.schema_name},
                            {"lake_table", meta.table_name},
                            {"column", col},
                            {"pg_type", pg_type},
                            {"error", ex.what()},
                        },
                    });
                }
            }
        }
    }

    std::vector<std::string> all_cols = data_cols;
    all_cols.insert(
        all_cols.end(),
        {lake_columns::kLoadTimestamp, lake_columns::kLoadDate, lake_columns::kSourceSystem, lake_columns::kSnapshotId});

    std::ostringstream col_list;
    for (std::size_t i = 0; i < all_cols.size(); ++i) {
        if (i) {
            col_list << ", ";
        }
        col_list << pg_ident(all_cols[i]);
    }

    const bool mssql_text = source_system == "MSSQL";
    std::vector<std::string> lines;
    lines.reserve(upserts.size());
    int row_index = 0;
    int skipped_missing_pk = 0;
    for (const auto& e : upserts) {
        bool missing_pk = false;
        for (const auto& pk_col : business_pk) {
            const json* pk_val = e.row.contains(pk_col) ? &e.row[pk_col] : nullptr;
            if (!pk_val || pk_val->is_null()) {
                missing_pk = true;
                break;
            }
        }
        if (missing_pk) {
            skipped_missing_pk += 1;
            continue;
        }
        const std::string row_ts = unique_load_timestamptz(e.ts_ms, row_index++);
        const std::string row_date = lake_columns::date_from_timestamptz(row_ts);
        std::ostringstream line;
        for (std::size_t i = 0; i < data_cols.size(); ++i) {
            if (i) {
                line << ',';
            }
            const json* val = e.row.contains(data_cols[i]) ? &e.row[data_cols[i]] : nullptr;
            const std::string pg_type = meta.col_types.count(data_cols[i]) ? meta.col_types.at(data_cols[i]) : "";
            line << (val ? json_cell_csv(*val, pg_type, mssql_text) : "");
        }
        line << ',' << csv_escape_local(row_ts) << ',' << csv_escape_local(row_date) << ','
             << csv_escape_local(source_system) << ',' << csv_escape_local(batch_id);
        lines.push_back(line.str());
    }
    if (lines.empty()) {
        return applied_deletes + static_cast<long long>(upserts.size() - skipped_missing_pk);
    }

    const std::string col_list_str = col_list.str();
    const std::string staging_name = "cdc_stg_" + std::to_string(staging_id);

    if (!lake_pk_cols.empty()) {
        const std::vector<std::string> dedup_cols =
            !delete_pk_cols.empty() ? delete_pk_cols : lake_pk_cols;
        const std::vector<std::string> batch_delete_cols =
            !delete_pk_cols.empty() ? delete_pk_cols : std::vector<std::string>{};
        copy_upserts_via_staging(
            pg,
            fq,
            col_list_str,
            lines,
            all_cols,
            lake_pk_cols,
            staging_name,
            dedup_cols,
            batch_delete_cols);
    } else {
        copy_csv_lines(pg, "COPY " + fq + " (" + col_list_str + ") FROM STDIN WITH (FORMAT csv)", lines);
    }

    return applied_deletes + static_cast<long long>(upserts.size() - skipped_missing_pk);
}

json apply_events_batch(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id,
    const std::string& batch_id,
    const std::vector<ApplyEvent>& events,
    const kafka_apply_detail::ApplyBatchOptions& options) {
    const std::string db_engine = (options.source_system == "MSSQL")
        ? "mssql"
        : (options.source_system == "MongoDB") ? "mongodb" : "mariadb";

    thread_local std::unordered_map<std::string, std::vector<std::string>> lake_pk_cache;
    thread_local std::unordered_map<std::string, std::vector<std::string>> lake_data_cols_cache;
    thread_local std::unordered_map<std::string, std::map<std::string, std::string>> lake_col_types_cache;

    std::vector<ApplyEvent> working(events.begin(), events.end());
    std::map<std::pair<std::string, std::string>, std::vector<ApplyEvent>> by_table;
    int dropped_unrecoverable = 0;
    for (auto& e : working) {
        if (e.catalog_id > 0) {
            resolve_event_lake_key_from_catalog(app_pg, conn_id, e);
        }
        if (e.schema_name.empty() || e.table_name.empty()) {
            fill_table_key_from_event_id(e, db_engine);
        }
        if (e.schema_name.empty() || e.table_name.empty()) {
            dropped_unrecoverable += 1;
            record_dropped_unrecoverable(app_pg, e, options.dropped_unrecoverable_by_table);
            continue;
        }
        by_table[{e.schema_name, e.table_name}].push_back(std::move(e));
    }
    if (dropped_unrecoverable > 0) {
        log_write(app_pg, {
            .level = LogLevel::Warning,
            .component = "cdc_kafka_apply_cpp",
            .message = "apply dropped unrecoverable events",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"events", dropped_unrecoverable}},
        });
    }

    long long applied = 0;
    int table_errors = 0;
    int* table_errors_acc = options.table_errors_out ? options.table_errors_out : &table_errors;
    json offsets = json::object();
    int staging_id = 0;

    // For MSSQL/MongoDB: build a lake-key → catalog map once per batch if any event lacks catalog_id
    std::unordered_map<std::string, std::pair<long long, std::string>> engine_catalog_map;
    if (options.source_system == "MSSQL" || options.source_system == "MongoDB") {
        bool needs_lookup = false;
        for (const auto& [key, items] : by_table) {
            if (!items.empty() && items.front().catalog_id == 0) {
                needs_lookup = true;
                break;
            }
        }
        if (needs_lookup) {
            const char* db_eng = (options.source_system == "MSSQL") ? "mssql" : "mongodb";
            const char* cv[] = {conn_id.c_str(), db_eng};
            PGresult* cr = PQexecParams(
                app_pg,
                "SELECT catalog_id, pk_columns, source_database, source_schema, source_table"
                " FROM cdc_catalog.catalog WHERE conn_id=$1 AND db_engine=$2::cdc_catalog.db_engine",
                2, nullptr, cv, nullptr, nullptr, 0);
            if (cr && PQresultStatus(cr) == PGRES_TUPLES_OK) {
                for (int i = 0; i < PQntuples(cr); ++i) {
                    const std::string db = PQgetvalue(cr, i, 2);
                    const std::string sch = PQgetvalue(cr, i, 3);
                    const std::string tbl = PQgetvalue(cr, i, 4);
                    std::string lk_schema;
                    std::string lk_table;
                    if (options.source_system == "MSSQL") {
                        lk_schema = mssql_pg_schema_name(db, sch);
                        lk_table = mssql_pg_table_name(tbl);
                    } else {
                        lk_schema = mongo_pg_schema_name(db);
                        lk_table = mongo_pg_table_name(tbl);
                    }
                    const char* cid_str = PQgetvalue(cr, i, 0);
                    const char* pk_str = PQgetvalue(cr, i, 1);
                    engine_catalog_map[lk_schema + "|" + lk_table] = {
                        cid_str ? std::atoll(cid_str) : 0, pk_str ? pk_str : ""};
                }
            } else if (cr) {
                log_write(app_pg, {
                    .level = LogLevel::Warning,
                    .component = "cdc_kafka_apply_cpp",
                    .message = "apply engine catalog lookup failed",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = std::nullopt,
                    .source_table = std::nullopt,
                    .context = {
                        {"db_engine", db_eng},
                        {"error", PQresultErrorMessage(cr)},
                    },
                });
            }
            if (cr) {
                PQclear(cr);
            }
        }
    }

    for (auto& [key, items] : by_table) {
        TableBatchMeta meta;
        meta.schema_name = key.first;
        meta.table_name = key.second;
        const std::string lake_key = meta.schema_name + "|" + meta.table_name;
        const auto pk_it = lake_pk_cache.find(lake_key);
        if (pk_it != lake_pk_cache.end()) {
            meta.lake_pk = pk_it->second;
        } else {
            meta.lake_pk = fetch_lake_pk(lake_pg, meta.schema_name, meta.table_name);
            lake_pk_cache[lake_key] = meta.lake_pk;
        }
        const auto cols_it = lake_data_cols_cache.find(lake_key);
        if (cols_it != lake_data_cols_cache.end()) {
            meta.data_cols = cols_it->second;
        } else {
            meta.data_cols = fetch_lake_data_cols(lake_pg, meta.schema_name, meta.table_name);
            lake_data_cols_cache[lake_key] = meta.data_cols;
        }
        const auto types_it = lake_col_types_cache.find(lake_key);
        if (types_it != lake_col_types_cache.end()) {
            meta.col_types = types_it->second;
        } else {
            meta.col_types = fetch_lake_col_types(lake_pg, meta.schema_name, meta.table_name);
            lake_col_types_cache[lake_key] = meta.col_types;
        }

        // Catalog ID / pk_columns lookup
        if (!items.empty() && items.front().catalog_id > 0) {
            meta.catalog_id = items.front().catalog_id;
        }

        if (meta.catalog_id == 0) {
            if (options.source_system == "MSSQL" || options.source_system == "MongoDB") {
                const std::string map_key = meta.schema_name + "|" + meta.table_name;
                auto it = engine_catalog_map.find(map_key);
                if (it != engine_catalog_map.end()) {
                    meta.catalog_id = it->second.first;
                    meta.pk_columns = it->second.second;
                }
            } else {
                const char* vals[] = {conn_id.c_str(), meta.schema_name.c_str(), meta.table_name.c_str()};
                PGresult* res = PQexecParams(
                    app_pg,
                    "SELECT catalog_id, pk_columns FROM cdc_catalog.catalog"
                    " WHERE conn_id=$1 AND db_engine='mariadb' AND source_schema=$2 AND source_table=$3",
                    3, nullptr, vals, nullptr, nullptr, 0);
                if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
                    const char* cid_str = PQgetvalue(res, 0, 0);
                    const char* pk_str = PQgetvalue(res, 0, 1);
                    meta.catalog_id = cid_str ? std::atoll(cid_str) : 0;
                    meta.pk_columns = pk_str ? pk_str : "";
                    meta.catalog_source_schema = meta.schema_name;
                    meta.catalog_source_table = meta.table_name;
                }
                if (res) {
                    PQclear(res);
                }
            }
        }

        if (meta.catalog_id > 0 && meta.catalog_source_schema.empty()) {
            const std::string cid = std::to_string(meta.catalog_id);
            const char* cvals[] = {cid.c_str()};
            PGresult* cres = PQexecParams(
                app_pg,
                "SELECT source_database, source_schema, source_table, db_engine::text"
                " FROM cdc_catalog.catalog WHERE catalog_id = $1::bigint",
                1,
                nullptr,
                cvals,
                nullptr,
                nullptr,
                0);
            if (cres && PQresultStatus(cres) == PGRES_TUPLES_OK && PQntuples(cres) > 0) {
                const std::string src_db = PQgetvalue(cres, 0, 0);
                const std::string src_schema = PQgetvalue(cres, 0, 1);
                const std::string src_table = PQgetvalue(cres, 0, 2);
                const std::string eng = PQgetvalue(cres, 0, 3);
                if (eng == "mongodb") {
                    meta.catalog_source_schema = mongo_catalog_source_schema(src_db, src_schema);
                } else {
                    meta.catalog_source_schema = src_schema;
                }
                meta.catalog_source_table = src_table;
            }
            if (cres) {
                PQclear(cres);
            }
        }
        if (meta.catalog_source_schema.empty()) {
            meta.catalog_source_schema = meta.schema_name;
            meta.catalog_source_table = meta.table_name;
        }
        if (meta.schema_name.empty() && !meta.catalog_source_schema.empty()) {
            meta.schema_name = meta.catalog_source_schema;
        }
        if (meta.table_name.empty() && !meta.catalog_source_table.empty()) {
            meta.table_name = meta.catalog_source_table;
        }
        if (meta.schema_name.empty() || meta.table_name.empty()) {
            (*table_errors_acc) += 1;
            if (options.failed_events_out) {
                for (auto& e : items) {
                    options.failed_events_out->push_back(std::move(e));
                }
            }
            log_write(app_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_apply_cpp",
                .message = "apply skipped: unresolved lake schema or table",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = meta.catalog_source_schema.empty() ? std::nullopt
                                                                    : std::optional(meta.catalog_source_schema),
                .source_table = meta.catalog_source_table.empty() ? std::nullopt
                                                                  : std::optional(meta.catalog_source_table),
                .context = {{"events", static_cast<int>(items.size())}, {"catalog_id", meta.catalog_id}},
            });
            continue;
        }

        const auto business_pk = business_pk_cols(meta);

        std::vector<ApplyEvent> upserts;
        std::vector<ApplyEvent> deletes;
        for (auto& e : items) {
            if (e.op == "d") {
                deletes.push_back(std::move(e));
            } else {
                upserts.push_back(std::move(e));
            }
        }

        int skipped_missing_pk = 0;
        int skipped_missing_pk_deletes = 0;
        if (!business_pk.empty()) {
            std::vector<ApplyEvent> valid_upserts;
            valid_upserts.reserve(upserts.size());
            for (auto& e : upserts) {
                bool missing_pk = false;
                for (const auto& pk_col : business_pk) {
                    const json* pk_val = e.row.contains(pk_col) ? &e.row[pk_col] : nullptr;
                    if (!pk_val || pk_val->is_null()) {
                        missing_pk = true;
                        break;
                    }
                }
                if (missing_pk) {
                    skipped_missing_pk += 1;
                } else {
                    valid_upserts.push_back(std::move(e));
                }
            }
            upserts = std::move(valid_upserts);
            std::vector<ApplyEvent> valid_deletes;
            valid_deletes.reserve(deletes.size());
            for (auto& e : deletes) {
                bool missing_pk = false;
                for (const auto& pk_col : business_pk) {
                    const json* pk_val = e.row.contains(pk_col) ? &e.row[pk_col] : nullptr;
                    if (!pk_val || pk_val->is_null()) {
                        missing_pk = true;
                        break;
                    }
                }
                if (missing_pk) {
                    skipped_missing_pk_deletes += 1;
                } else {
                    valid_deletes.push_back(std::move(e));
                }
            }
            deletes = std::move(valid_deletes);
        }
        const int total_skipped = skipped_missing_pk + skipped_missing_pk_deletes;
        if (total_skipped > 0) {
            log_write(app_pg, {
                .level = LogLevel::Warning,
                .component = "cdc_kafka_apply_cpp",
                .message = "apply skipped events with missing primary key",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = meta.catalog_source_schema,
                .source_table = meta.catalog_source_table,
                .context = {
                    {"skipped_missing_pk", skipped_missing_pk},
                    {"skipped_missing_pk_deletes", skipped_missing_pk_deletes},
                    {"total_upserts", skipped_missing_pk + static_cast<int>(upserts.size())},
                    {"total_deletes", skipped_missing_pk_deletes + static_cast<int>(deletes.size())},
                },
            });
        }

        const auto table_start = std::chrono::steady_clock::now();

        PGresult* lake_begin = PQexec(lake_pg, "BEGIN");
        if (lake_begin) {
            PQclear(lake_begin);
        }
        pg_exec(lake_pg, "SAVEPOINT sp_lake_apply");
        long long table_applied = 0;
        try {
            table_applied = apply_table_batch(
                lake_pg,
                app_pg,
                conn_id,
                batch_id,
                meta,
                upserts,
                deletes,
                staging_id++,
                options.source_system);
        } catch (const std::exception& ex) {
            { PGresult* r = PQexec(lake_pg, "ROLLBACK"); if (r) PQclear(r); }
            (*table_errors_acc) += 1;
            if (options.failed_events_out) {
                for (auto& e : deletes) {
                    options.failed_events_out->push_back(std::move(e));
                }
                for (auto& e : upserts) {
                    options.failed_events_out->push_back(std::move(e));
                }
            }
            log_write(app_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_apply_cpp",
                .message = "lake table apply failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = meta.catalog_source_schema,
                .source_table = meta.catalog_source_table,
                .context = {
                    {"lake_schema", meta.schema_name},
                    {"lake_table", meta.table_name},
                    {"error", ex.what()},
                    {"upserts", static_cast<int>(upserts.size())},
                    {"deletes", static_cast<int>(deletes.size())},
                },
            });
            if (meta.catalog_id > 0) {
                mark_catalog_cdc_failed(app_pg, meta.catalog_id, ex.what());
            }
            continue;
        }

        std::vector<ApplyEvent> audit;
        audit.reserve(deletes.size() + upserts.size());
        audit.insert(audit.end(), deletes.begin(), deletes.end());
        audit.insert(audit.end(), upserts.begin(), upserts.end());
        for (auto& e : audit) {
            if (e.catalog_id > 0) {
                resolve_event_lake_key_from_catalog(app_pg, conn_id, e);
            }
            if (e.schema_name.empty() || e.table_name.empty()) {
                fill_table_key_from_event_id(e, db_engine);
            }
            if (e.schema_name.empty() && !meta.catalog_source_schema.empty()) {
                e.schema_name = meta.catalog_source_schema;
            }
            if (e.table_name.empty() && !meta.catalog_source_table.empty()) {
                e.table_name = meta.catalog_source_table;
            }
            if (e.schema_name.empty() && !meta.schema_name.empty()) {
                e.schema_name = meta.schema_name;
            }
            if (e.table_name.empty() && !meta.table_name.empty()) {
                e.table_name = meta.table_name;
            }
        }
        PGresult* app_begin = PQexec(app_pg, "BEGIN");
        if (app_begin) {
            PQclear(app_begin);
        }
        std::optional<std::pair<std::string, long long>> kafka_offset_out;
        try {
            if (options.audit_enabled && !audit.empty()) {
                record_applied_events(app_pg, conn_id, audit);
            }
            const auto table_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - table_start)
                                               .count();
            const ApplyEvent* max_kafka = max_kafka_audit_position(audit);
            if (max_kafka != nullptr) {
                const ApplyEvent& last = *max_kafka;
                const ApplyOpCounts op_counts = count_apply_ops(audit);
                const std::string state_key =
                    table_state_key(meta.catalog_source_schema, meta.catalog_source_table);
                int events_seen_in_slice = 0;
                BatchStatsMetrics metrics;
                if (const auto it = options.slice_table_state.find(state_key);
                    it != options.slice_table_state.end()) {
                    events_seen_in_slice = it->second.events_seen_in_slice;
                    metrics.kafka_consumer_lag = it->second.kafka_consumer_lag;
                    metrics.dedup_skipped = it->second.dedup_skipped;
                }
                if (options.parse_skipped_by_table) {
                    const TableKey pk{meta.catalog_source_schema, meta.catalog_source_table};
                    const auto ps_it = options.parse_skipped_by_table->find(pk);
                    if (ps_it != options.parse_skipped_by_table->end()) {
                        metrics.parse_skipped = ps_it->second;
                    }
                }
                if (options.dropped_unrecoverable_by_table) {
                    const TableKey pk{meta.catalog_source_schema, meta.catalog_source_table};
                    const auto dr_it = options.dropped_unrecoverable_by_table->find(pk);
                    if (dr_it != options.dropped_unrecoverable_by_table->end()) {
                        metrics.dropped_unrecoverable = dr_it->second;
                    }
                }
                update_apply_position(
                    app_pg,
                    meta.catalog_id,
                    last.topic,
                    last.partition,
                    last.offset,
                    last.gtid);
                const TableHealthSnapshot health = fetch_table_health_by_catalog_id(
                    app_pg,
                    meta.catalog_id,
                    options.apply_staleness_seconds,
                    options.apply_inactive_seconds);
                const json batch_context = {
                    {"sample", build_apply_row_sample(audit)},
                    {"lake_target",
                     {{"schema", meta.schema_name}, {"table", meta.table_name}}},
                };
                insert_apply_batch_stats(
                    app_pg,
                    batch_id,
                    conn_id,
                    meta.catalog_id,
                    meta.catalog_source_schema,
                    meta.catalog_source_table,
                    op_counts,
                    table_duration_ms,
                    last.topic,
                    last.partition,
                    last.offset,
                    health,
                    false,
                    events_seen_in_slice,
                    metrics,
                    batch_context);
                const long long events_applied_total =
                    op_counts.inserts + op_counts.updates + op_counts.deletes;
                log_write(app_pg, {
                    .level = LogLevel::Info,
                    .component = "cdc_kafka_apply_cpp",
                    .message = "lake table written",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = meta.catalog_source_schema,
                    .source_table = meta.catalog_source_table,
                    .context = {
                        {"lake_schema", meta.schema_name},
                        {"lake_table", meta.table_name},
                        {"inserts", op_counts.inserts},
                        {"updates", op_counts.updates},
                        {"deletes", op_counts.deletes},
                        {"rows_applied", events_applied_total},
                        {"duration_ms", table_duration_ms},
                        {"kafka_topic", last.topic},
                        {"kafka_partition", last.partition},
                        {"kafka_offset", last.offset},
                    },
                });
                kafka_offset_out = {last.topic + ":" + std::to_string(last.partition), last.offset};
            }
            pg_exec(lake_pg, "RELEASE SAVEPOINT sp_lake_apply");
            PGresult* lake_commit = PQexec(lake_pg, "COMMIT");
            const bool lake_commit_ok =
                lake_commit && PQresultStatus(lake_commit) == PGRES_COMMAND_OK;
            if (!lake_commit_ok) {
                const std::string lake_err =
                    lake_commit ? PQresultErrorMessage(lake_commit) : PQerrorMessage(lake_pg);
                if (lake_commit) {
                    PQclear(lake_commit);
                }
                { PGresult* r = PQexec(app_pg, "ROLLBACK"); if (r) PQclear(r); }
                { PGresult* r = PQexec(lake_pg, "ROLLBACK"); if (r) PQclear(r); }
                (*table_errors_acc) += 1;
                if (options.failed_events_out) {
                    for (auto& e : audit) {
                        options.failed_events_out->push_back(std::move(e));
                    }
                }
                log_write(app_pg, {
                    .level = LogLevel::Error,
                    .component = "cdc_kafka_apply_cpp",
                    .message = "lake commit failed before audit",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = meta.catalog_source_schema,
                    .source_table = meta.catalog_source_table,
                    .context = {
                        {"lake_schema", meta.schema_name},
                        {"lake_table", meta.table_name},
                        {"error", lake_err},
                        {"audit_events", static_cast<int>(audit.size())},
                    },
                });
                if (meta.catalog_id > 0) {
                    mark_catalog_cdc_failed(app_pg, meta.catalog_id, lake_err);
                }
                continue;
            }
            PQclear(lake_commit);
            bool app_commit_ok = false;
            for (int app_retry = 0; app_retry < 3; ++app_retry) {
                PGresult* app_commit = PQexec(app_pg, "COMMIT");
                app_commit_ok = app_commit && PQresultStatus(app_commit) == PGRES_COMMAND_OK;
                if (app_commit) {
                    PQclear(app_commit);
                }
                if (app_commit_ok) {
                    break;
                }
                if (app_retry < 2) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (1 << app_retry)));
                }
            }
            if (!app_commit_ok) {
                const std::string app_err = PQerrorMessage(app_pg);
                (*table_errors_acc) += 1;
                if (options.failed_events_out) {
                    for (auto& e : audit) {
                        options.failed_events_out->push_back(std::move(e));
                    }
                }
                log_write(app_pg, {
                    .level = LogLevel::Error,
                    .component = "cdc_kafka_apply_cpp",
                    .message = "audit commit failed after lake commit",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = meta.catalog_source_schema,
                    .source_table = meta.catalog_source_table,
                    .context = {
                        {"lake_schema", meta.schema_name},
                        {"lake_table", meta.table_name},
                        {"error", app_err},
                        {"audit_events", static_cast<int>(audit.size())},
                    },
                });
                if (meta.catalog_id > 0) {
                    mark_catalog_cdc_failed(app_pg, meta.catalog_id, app_err);
                }
                continue;
            }
            if (meta.catalog_id > 0) {
                mark_catalog_cdc_success(app_pg, meta.catalog_id);
            }
            applied += table_applied;
            if (kafka_offset_out) {
                offsets[kafka_offset_out->first] = kafka_offset_out->second;
            }
        } catch (const std::exception& ex) {
            { PGresult* r = PQexec(app_pg, "ROLLBACK"); if (r) PQclear(r); }
            { PGresult* r = PQexec(lake_pg, "ROLLBACK TO SAVEPOINT sp_lake_apply"); if (r) PQclear(r); }
            { PGresult* r = PQexec(lake_pg, "RELEASE SAVEPOINT sp_lake_apply"); if (r) PQclear(r); }
            { PGresult* r = PQexec(lake_pg, "ROLLBACK"); if (r) PQclear(r); }
            (*table_errors_acc) += 1;
            if (options.failed_events_out) {
                for (auto& e : audit) {
                    options.failed_events_out->push_back(std::move(e));
                }
            }
            log_write(app_pg, {
                .level = LogLevel::Error,
                .component = "cdc_kafka_apply_cpp",
                .message = "apply audit/position failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = meta.catalog_source_schema,
                .source_table = meta.catalog_source_table,
                .context = {
                    {"lake_schema", meta.schema_name},
                    {"lake_table", meta.table_name},
                    {"error", ex.what()},
                    {"audit_events", static_cast<int>(audit.size())},
                },
            });
            if (meta.catalog_id > 0) {
                mark_catalog_cdc_failed(app_pg, meta.catalog_id, ex.what());
            }
            continue;
        }
    }

    return json{{"applied", applied}, {"offsets", offsets}, {"errors", *table_errors_acc}, {"dropped_unrecoverable", dropped_unrecoverable}};
}

std::unordered_set<std::string> filter_new_event_ids(PGconn* pg, const std::vector<std::string>& event_ids) {
    std::unordered_set<std::string> fresh;
    if (event_ids.empty()) {
        return fresh;
    }
    auto escape_array_elem = [](const std::string& s) {
        std::string out;
        out.reserve(s.size() + 8);
        for (char c : s) {
            if (c == '"' || c == '\\') {
                out += '\\';
            }
            out += c;
        }
        return out;
    };
    std::ostringstream arr;
    arr << '{';
    for (std::size_t i = 0; i < event_ids.size(); ++i) {
        if (i) {
            arr << ',';
        }
        arr << '"' << escape_array_elem(event_ids[i]) << '"';
    }
    arr << '}';
    const std::string literal = arr.str();
    const char* vals[] = {literal.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT event_id FROM cdc_catalog.cdc_applied_events WHERE event_id = ANY($1::text[])",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::unordered_set<std::string> existing;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res); ++i) {
            existing.insert(PQgetvalue(res, i, 0));
        }
    }
    if (res) {
        PQclear(res);
    }
    for (const auto& id : event_ids) {
        if (!existing.count(id)) {
            fresh.insert(id);
        }
    }
    return fresh;
}

}  // namespace kafka_apply_detail

using kafka_apply_detail::ApplyEvent;
using kafka_apply_detail::apply_events_batch;

#ifndef HAVE_RDKAFKA
int run_kafka_apply_native_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    int worker_id,
    int worker_count) {
    (void)cfg;
    (void)log_pg;
    (void)conn_id;
    (void)worker_id;
    (void)worker_count;
    std::cerr << "kafka-apply requires librdkafka (rebuild Docker image: ./install.sh)\n";
    return 2;
}
#endif
