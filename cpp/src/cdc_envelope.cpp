#include "cdc_envelope.hpp"

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

/** Normalize mariadb-binlog binary cells: _binary '\\x..', X'HEX', 0xHEX. */
std::string normalize_binlog_binary_cell(std::string raw) {
    raw = trim_copy(std::move(raw));
    if (raw == "NULL" || raw.empty()) {
        return raw;
    }
    const std::string lower = to_lower_copy(raw);
    if (lower.rfind("_binary", 0) == 0) {
        raw = trim_copy(raw.substr(7));
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
        return unescaped;
    }
    if (raw.size() >= 3 && (raw[0] == 'X' || raw[0] == 'x') && raw[1] == '\'') {
        const auto close = raw.rfind('\'');
        if (close != std::string::npos && close > 2) {
            return raw.substr(2, close - 2);
        }
    }
    if (raw.size() >= 2 && (raw[0] == '0' && (raw[1] == 'x' || raw[1] == 'X'))) {
        return raw.substr(2);
    }
    return raw;
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
    if (lower.rfind("_binary", 0) == 0 || (trimmed.size() >= 2 && trimmed[0] == '0' && (trimmed[1] == 'x' || trimmed[1] == 'X')) ||
        (trimmed.size() >= 2 && (trimmed[0] == 'X' || trimmed[0] == 'x') && trimmed[1] == '\'')) {
        return normalize_binlog_binary_cell(raw);
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
            return unescaped;
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
        const std::string& raw = col_values[i];
        if (raw.empty() || raw == "NULL") {
            out[columns[i]] = nullptr;
        } else {
            out[columns[i]] = parse_sql_literal(raw);
        }
    }
    return out;
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
    return "u";
}

nlohmann::json CdcEvent::to_kafka_dict() const {
    nlohmann::json source = nlohmann::json::object();
    source["gtid"] = gtid.empty() ? nullptr : nlohmann::json(gtid);
    source["file"] = binlog_file.empty() ? nullptr : nlohmann::json(binlog_file);
    source["pos"] = binlog_pos.has_value() ? nlohmann::json(*binlog_pos) : nullptr;
    source["ts_ms"] = ts_ms.has_value() ? nlohmann::json(*ts_ms) : nullptr;

    if (db_engine == "mssql") {
        source["lsn"] = gtid.empty() ? nullptr : nlohmann::json(gtid);
        if (!mssql_seqval.empty()) {
            source["seqval"] = mssql_seqval;
        }
        source["db_engine"] = "mssql";
    }
    if (db_engine == "mongodb") {
        source["database"] = source_database;
        source["collection"] = collection.empty() ? table_name : collection;
        source["gtid"] = gtid.empty() ? nullptr : nlohmann::json(gtid);
        source["resume_token"] = resume_token.is_null() ? nullptr : resume_token;
        source["db_engine"] = "mongodb";
    }

    return {
        {"op", op},
        {"conn_id", conn_id},
        {"db_engine", db_engine},
        {"source_database", source_database},
        {"source_schema", schema_name},
        {"source_table", table_name},
        {"before", before.is_null() ? nullptr : before},
        {"after", after.is_null() ? nullptr : after},
        {"source", source},
        {"ingestion_ts", ingestion_ts.empty() ? utc_iso_timestamp_now() : ingestion_ts},
    };
}
