#include "kafka_apply_detail.hpp"

#include "mongo_lake.hpp"
#include "mssql_lake.hpp"

#include <cctype>
#include <sstream>
#include <string>
#include <vector>

namespace kafka_apply_detail {

using json = nlohmann::json;

namespace {

std::string op_char(const json& data) {
    std::string op = data.value("op", "u");
    if (op == "INSERT") {
        return "c";
    }
    if (op == "UPDATE") {
        return "u";
    }
    if (op == "DELETE") {
        return "d";
    }
    return op;
}

json row_for_apply_json(const json& data) {
    const std::string op = op_char(data);
    if (op == "d") {
        return data.contains("before") && data["before"].is_object() ? data["before"] : json::object();
    }
    return data.contains("after") && data["after"].is_object() ? data["after"] : json::object();
}

void normalize_apply_row(json& row, const std::vector<std::string>& pk_cols) {
    if (row.contains("mongo_id") && !row["mongo_id"].is_null()) {
        row["mongo_id"] = mongo_object_id_text(row["mongo_id"]);
    }
    for (const auto& col : pk_cols) {
        if (!row.contains(col) || row[col].is_null() || !row[col].is_string()) {
            continue;
        }
        const std::string s = row[col].get<std::string>();
        if (s.empty()) {
            continue;
        }
        bool numeric = true;
        for (char c : s) {
            if (!std::isdigit(static_cast<unsigned char>(c)) && c != '-') {
                numeric = false;
                break;
            }
        }
        if (numeric) {
            try {
                row[col] = std::stoll(s);
            } catch (...) {
            }
        }
    }
}

std::string build_event_id(
    const json& data,
    const std::vector<std::string>& pk_cols,
    const std::string& db_engine = "mariadb") {
    const json row = row_for_apply_json(data);
    std::ostringstream pk_part;
    bool any = false;
    for (const auto& col : pk_cols) {
        if (!row.contains(col)) {
            continue;
        }
        if (any) {
            pk_part << "|";
        }
        any = true;
        pk_part << col << "=" << row[col].dump();
    }
    const json source = data.contains("source") && data["source"].is_object() ? data["source"] : json::object();
    const std::string actual_engine = data.value("db_engine", source.value("db_engine", db_engine));
    std::string pos_part;
    if (actual_engine == "mssql") {
        std::string lsn;
        if (source.contains("lsn") && !source["lsn"].is_null()) {
            lsn = source["lsn"].get<std::string>();
        } else if (source.contains("gtid") && !source["gtid"].is_null()) {
            lsn = source["gtid"].get<std::string>();
        } else if (data.contains("gtid") && !data["gtid"].is_null()) {
            lsn = data["gtid"].get<std::string>();
        }
        const std::string src_db = data.value("source_database", source.value("database", ""));
        pos_part = "mssql:" + src_db + ":" + lsn;
        if (source.contains("seqval") && !source["seqval"].is_null()) {
            pos_part += ":" + source["seqval"].get<std::string>();
        }
    } else if (actual_engine == "mongodb") {
        std::string token;
        if (source.contains("gtid") && !source["gtid"].is_null()) {
            token = source["gtid"].is_string() ? source["gtid"].get<std::string>() : source["gtid"].dump();
        } else if (source.contains("resume_token") && !source["resume_token"].is_null()) {
            token = source["resume_token"].dump();
        } else if (data.contains("gtid") && !data["gtid"].is_null()) {
            token = data["gtid"].is_string() ? data["gtid"].get<std::string>() : data["gtid"].dump();
        }
        const std::string src_db = data.value("source_database", source.value("database", ""));
        pos_part = "mongo:" + src_db + ":" + token;
    } else if (source.contains("gtid") && !source["gtid"].is_null()) {
        pos_part = source["gtid"].get<std::string>();
    } else if (data.contains("gtid") && !data["gtid"].is_null()) {
        pos_part = data["gtid"].get<std::string>();
    } else {
        pos_part = source.value("file", "") + ":" + std::to_string(source.value("pos", 0LL));
    }
    const std::string schema_name = data.value("source_schema", source.value("db", ""));
    const std::string table_name = data.value("source_table", source.value("table", ""));
    if (!any) {
        pk_part << data.dump();
    }
    return pos_part + "|" + schema_name + "." + table_name + "|" + op_char(data) + "|" + pk_part.str();
}

}  // namespace

ApplyEvent parse_apply_event(const json& obj) {
    ApplyEvent e;
    e.event_id = obj.value("event_id", "");
    e.op = obj.value("op", "c");
    e.schema_name = obj.value("schema", obj.value("source_schema", ""));
    e.table_name = obj.value("table", obj.value("source_table", ""));
    e.topic = obj.value("topic", "");
    e.partition = obj.value("partition", 0);
    e.offset = obj.value("offset", 0LL);
    if (obj.contains("gtid") && !obj["gtid"].is_null()) {
        e.gtid = obj["gtid"].get<std::string>();
    }
    e.catalog_id = obj.value("catalog_id", 0LL);
    e.row = obj.contains("row") && obj["row"].is_object() ? obj["row"] : json::object();
    return e;
}

json parse_kafka_message_json(const std::string& payload) {
    for (std::size_t skip = 0; skip <= 32 && skip < payload.size(); ++skip) {
        json data = json::parse(payload.substr(skip), nullptr, false);
        if (!data.is_discarded() && data.is_object() && data.contains("op")) {
            return data;
        }
    }
    return json();
}

bool parse_kafka_payload(
    const json& data,
    ApplyEvent& out,
    const std::string& topic,
    int partition,
    long long offset,
    const std::vector<std::string>& pk_cols,
    const std::string& db_engine) {
    if (!data.is_object()) {
        return false;
    }
    const json source = data.contains("source") && data["source"].is_object() ? data["source"] : json::object();
    const std::string actual_engine = data.value("db_engine", source.value("db_engine", db_engine));

    out.op = op_char(data);
    out.row = row_for_apply_json(data);
    normalize_apply_row(out.row, pk_cols);
    out.topic = topic;
    out.partition = partition;
    out.offset = offset;
    out.catalog_id = data.value("catalog_id", 0LL);
    if (data.contains("ts_ms") && !data["ts_ms"].is_null()) {
        if (data["ts_ms"].is_number_integer()) {
            out.ts_ms = data["ts_ms"].get<long long>();
        } else if (data["ts_ms"].is_number_unsigned()) {
            out.ts_ms = static_cast<long long>(data["ts_ms"].get<unsigned long long>());
        }
    }

    if (actual_engine == "mssql") {
        const std::string src_db = data.value("source_database", source.value("database", ""));
        const std::string src_schema = data.value("source_schema", source.value("db", ""));
        const std::string src_table = data.value("source_table", source.value("table", ""));
        if (src_schema.empty() || src_table.empty()) {
            return false;
        }
        out.schema_name = mssql_pg_schema_name(src_db, src_schema);
        out.table_name = mssql_pg_table_name(src_table);
        if (source.contains("lsn") && !source["lsn"].is_null()) {
            out.gtid = source["lsn"].get<std::string>();
        } else if (source.contains("gtid") && !source["gtid"].is_null()) {
            out.gtid = source["gtid"].get<std::string>();
        }
    } else if (actual_engine == "mongodb") {
        const std::string src_db = data.value("source_database", source.value("database", ""));
        const std::string src_coll = data.value("source_table", source.value("collection", ""));
        if (src_db.empty() || src_coll.empty()) {
            return false;
        }
        out.schema_name = mongo_pg_schema_name(src_db);
        out.table_name = mongo_pg_table_name(src_coll);
        if (source.contains("resume_token") && !source["resume_token"].is_null()) {
            out.gtid = source["resume_token"].dump();
        } else if (source.contains("gtid") && !source["gtid"].is_null()) {
            out.gtid = source["gtid"].get<std::string>();
        }
    } else {
        out.schema_name = data.value("source_schema", data.value("schema", ""));
        if (out.schema_name.empty()) {
            out.schema_name = source.value("db", "");
        }
        out.table_name = data.value("source_table", data.value("table", ""));
        if (out.table_name.empty()) {
            out.table_name = source.value("table", "");
        }
        if (out.schema_name.empty() || out.table_name.empty()) {
            return false;
        }
        if (source.contains("gtid") && !source["gtid"].is_null()) {
            out.gtid = source["gtid"].get<std::string>();
        }
    }

    out.event_id = build_event_id(data, pk_cols, actual_engine);
    return true;
}

bool parse_kafka_payload(
    const char* payload,
    size_t len,
    ApplyEvent& out,
    const std::string& topic,
    int partition,
    long long offset,
    const std::vector<std::string>& pk_cols,
    const std::string& db_engine) {
    const json data = parse_kafka_message_json(std::string(payload, len));
    if (data.is_discarded() || !data.is_object()) {
        return false;
    }
    return parse_kafka_payload(data, out, topic, partition, offset, pk_cols, db_engine);
}

}  // namespace kafka_apply_detail
