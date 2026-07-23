#include "cdc_envelope.hpp"

#include "mariadb_boolean.hpp"
#include "mariadb_schema.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>

namespace {

std::string trim_copy(std::string s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
        s.erase(s.begin());
    }
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
        s.pop_back();
    }
    return s;
}

std::string to_lower_copy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

}  // namespace

std::string utc_iso_timestamp_now() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S") << "+00:00";
    return oss.str();
}

nlohmann::json parse_sql_literal(const std::string& raw) {
    if (raw == "NULL") {
        return nullptr;
    }
    const std::string trimmed = trim_copy(raw);
    const std::string lower = to_lower_copy(trimmed);
    // MariaDB binlog -v BIT(1): b'0' / b'1' → JSON bool (before binary/hex fallback).
    if (lower == "b'0'" || lower == "b\"0\"") {
        return false;
    }
    if (lower == "b'1'" || lower == "b\"1\"") {
        return true;
    }
    if (lower.rfind("_binary", 0) == 0 || (trimmed.size() >= 2 && trimmed[0] == '0' && (trimmed[1] == 'x' || trimmed[1] == 'X')) ||
        (trimmed.size() >= 2 && (trimmed[0] == 'X' || trimmed[0] == 'x') && trimmed[1] == '\'')) {
        return mariadb_binary_cell_to_json_hex(raw);
    }
    if (raw.size() >= 2 && raw.front() == '\'' && raw.back() == '\'') {
        std::string inner = raw.substr(1, raw.size() - 2);
        std::string unescaped;
        unescaped.reserve(inner.size());
        for (std::size_t i = 0; i < inner.size(); ++i) {
            if (inner[i] == '\'' && i + 1 < inner.size() && inner[i + 1] == '\'') {
                unescaped += '\'';
                ++i;
            } else {
                unescaped += inner[i];
            }
        }
        if (unescaped.size() == 10 && unescaped[4] == '-' && unescaped[7] == '-') {
            return sanitize_utf8_for_json(unescaped);
        }
        return sanitize_utf8_for_json(unescaped);
    }
    static const std::regex int_re(R"(-?\d+)");
    static const std::regex float_re(R"(-?\d+\.\d+)");
    if (std::regex_match(raw, int_re)) {
        try {
            return std::stoll(raw);
        } catch (...) {
            return sanitize_utf8_for_json(raw);
        }
    }
    if (std::regex_match(raw, float_re)) {
        try {
            return std::stod(raw);
        } catch (...) {
            return sanitize_utf8_for_json(raw);
        }
    }
    return sanitize_utf8_for_json(raw);
}

nlohmann::json row_dict_from_columns(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& col_values) {
    nlohmann::json out = nlohmann::json::object();
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i >= col_values.size()) {
            break;
        }
        const std::string col_key = sanitize_utf8_for_json(columns[i]);
        const std::string& raw = col_values[i];
        if (raw.empty() || raw == "NULL") {
            out[col_key] = nullptr;
        } else {
            out[col_key] = json_sanitize_for_kafka(parse_sql_literal(raw));
        }
    }
    return out;
}

nlohmann::json json_sanitize_for_kafka(const nlohmann::json& value) {
    if (value.is_string()) {
        return sanitize_utf8_for_json(value.get<std::string>());
    }
    if (value.is_object()) {
        nlohmann::json out = nlohmann::json::object();
        for (auto it = value.begin(); it != value.end(); ++it) {
            out[sanitize_utf8_for_json(it.key())] = json_sanitize_for_kafka(it.value());
        }
        return out;
    }
    if (value.is_array()) {
        nlohmann::json out = nlohmann::json::array();
        for (const auto& el : value) {
            out.push_back(json_sanitize_for_kafka(el));
        }
        return out;
    }
    return value;
}

std::string json_dump_for_kafka(const nlohmann::json& value) {
    return value.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
}

std::string cdc_event_kafka_payload(const CdcEvent& event) {
    CdcEvent safe = event;
    safe.conn_id = sanitize_utf8_for_json(event.conn_id);
    safe.db_engine = sanitize_utf8_for_json(event.db_engine);
    safe.source_database = sanitize_utf8_for_json(event.source_database);
    safe.schema_name = sanitize_utf8_for_json(event.schema_name);
    safe.table_name = sanitize_utf8_for_json(event.table_name);
    safe.gtid = sanitize_utf8_for_json(event.gtid);
    safe.mssql_seqval = sanitize_utf8_for_json(event.mssql_seqval);
    safe.binlog_file = sanitize_utf8_for_json(event.binlog_file);
    safe.collection = sanitize_utf8_for_json(event.collection);
    safe.ingestion_ts = sanitize_utf8_for_json(
        event.ingestion_ts.empty() ? utc_iso_timestamp_now() : event.ingestion_ts);
    if (!event.before.is_null()) {
        safe.before = json_sanitize_for_kafka(event.before);
    }
    if (!event.after.is_null()) {
        safe.after = json_sanitize_for_kafka(event.after);
    }
    if (!event.resume_token.is_null()) {
        safe.resume_token = json_sanitize_for_kafka(event.resume_token);
    }
    return json_dump_for_kafka(json_sanitize_for_kafka(safe.to_kafka_dict()));
}

std::string op_char_from_mysql(const std::string& mysql_op) {
    if (mysql_op == "INSERT") {
        return "c";
    }
    if (mysql_op == "UPDATE") {
        return "u";
    }
    if (mysql_op == "DELETE") {
        return "d";
    }
    return "";
}

nlohmann::json CdcEvent::to_kafka_dict() const {
    nlohmann::json source = nlohmann::json::object();
    source["gtid"] = gtid.empty() ? nullptr : nlohmann::json(sanitize_utf8_for_json(gtid));
    source["file"] = binlog_file.empty() ? nullptr : nlohmann::json(sanitize_utf8_for_json(binlog_file));
    source["pos"] = binlog_pos.has_value() ? nlohmann::json(*binlog_pos) : nullptr;
    source["ts_ms"] = ts_ms.has_value() ? nlohmann::json(*ts_ms) : nullptr;

    if (db_engine == "mssql") {
        source["lsn"] = gtid.empty() ? nullptr : nlohmann::json(sanitize_utf8_for_json(gtid));
        if (!mssql_seqval.empty()) {
            source["seqval"] = sanitize_utf8_for_json(mssql_seqval);
        }
        source["db_engine"] = "mssql";
    }
    if (db_engine == "mongodb") {
        source["database"] = sanitize_utf8_for_json(source_database);
        source["collection"] = sanitize_utf8_for_json(collection.empty() ? table_name : collection);
        source["gtid"] = gtid.empty() ? nullptr : nlohmann::json(sanitize_utf8_for_json(gtid));
        source["resume_token"] = resume_token.is_null() ? nullptr : json_sanitize_for_kafka(resume_token);
        source["db_engine"] = "mongodb";
    }

    const nlohmann::json before_json = before.is_null() ? nullptr : json_sanitize_for_kafka(before);
    const nlohmann::json after_json = after.is_null() ? nullptr : json_sanitize_for_kafka(after);

    return {
        {"op", sanitize_utf8_for_json(op)},
        {"conn_id", sanitize_utf8_for_json(conn_id)},
        {"db_engine", sanitize_utf8_for_json(db_engine)},
        {"source_database", sanitize_utf8_for_json(source_database)},
        {"source_schema", sanitize_utf8_for_json(schema_name)},
        {"source_table", sanitize_utf8_for_json(table_name)},
        {"before", before_json},
        {"after", after_json},
        {"source", source},
        {"ingestion_ts", sanitize_utf8_for_json(ingestion_ts.empty() ? utc_iso_timestamp_now() : ingestion_ts)},
    };
}
