#include "apply_dlq.hpp"

#include "pg_conn.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace apply_dlq {
namespace {

bool contains_ci(const std::string& haystack, const std::string& needle) {
    if (needle.empty() || haystack.size() < needle.size()) {
        return false;
    }
    const auto it = std::search(
        haystack.begin(), haystack.end(), needle.begin(), needle.end(),
        [](unsigned char a, unsigned char b) {
            return std::tolower(a) == std::tolower(b);
        });
    return it != haystack.end();
}

/** SQLSTATE class, i.e. the first two characters. */
std::string sqlstate_class(const std::string& sqlstate) {
    return sqlstate.size() >= 2 ? sqlstate.substr(0, 2) : std::string();
}

const char* reason_text(Reason reason) {
    return reason == Reason::UnresolvedTable ? "unresolved_table" : "apply_error";
}

}  // namespace

bool error_is_deterministic(const std::string& sqlstate, const std::string& message) {
    // A lost connection or an exhausted resource says nothing about the row, and the row
    // will apply cleanly once the cause clears. Checked first so it wins over any class
    // match below: a deadlock, for instance, reports as 40P01 but is purely transient.
    if (sqlstate == "40001" || sqlstate == "40P01" ||  // serialization failure, deadlock
        sqlstate == "55P03" || sqlstate == "57014" ||  // lock not available, statement timeout
        sqlstate_class(sqlstate) == "08" ||            // connection exception
        sqlstate_class(sqlstate) == "53" ||            // insufficient resources (disk, memory)
        sqlstate_class(sqlstate) == "57" ||            // operator intervention (shutdown)
        sqlstate_class(sqlstate) == "58") {            // system error
        return false;
    }

    if (!sqlstate.empty()) {
        const std::string cls = sqlstate_class(sqlstate);
        // 22 = data exception (bad numeric input, value too long, invalid encoding).
        // 23 = integrity constraint violation (not null, unique, foreign key, check).
        return cls == "22" || cls == "23";
    }

    // No SQLSTATE reached us, which happens when the failure surfaced through libpq's
    // connection-level message rather than a result. Fall back to the message text, and
    // stay conservative: anything unrecognised is treated as transient, so an unclassified
    // error keeps the old retry-and-quarantine behaviour instead of parking a valid row.
    if (contains_ci(message, "connection") || contains_ci(message, "server closed") ||
        contains_ci(message, "broken pipe") || contains_ci(message, "timeout") ||
        contains_ci(message, "EOF") || contains_ci(message, "reset")) {
        return false;
    }
    return contains_ci(message, "violates") || contains_ci(message, "invalid input syntax") ||
           contains_ci(message, "value too long") || contains_ci(message, "out of range") ||
           contains_ci(message, "cannot be cast");
}

bool park_event(
    PGconn* app_pg,
    const kafka_apply_detail::ApplyEvent& event,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& source_schema,
    const std::string& source_table,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::string& batch_id,
    const std::string& sqlstate,
    const std::string& error_message,
    Reason reason) {
    if (!app_pg) {
        return false;
    }

    const std::string catalog_id = event.catalog_id > 0 ? std::to_string(event.catalog_id) : "";
    const std::string partition = std::to_string(event.partition);
    const std::string offset = std::to_string(event.offset);
    const std::string ts_ms = std::to_string(event.ts_ms);
    const std::string payload = event.row.is_object() ? event.row.dump() : std::string("{}");
    const std::string reason_str = reason_text(reason);

    const char* vals[] = {
        catalog_id.empty() ? nullptr : catalog_id.c_str(),
        conn_id.c_str(),
        db_engine.empty() ? nullptr : db_engine.c_str(),
        source_schema.c_str(),
        source_table.c_str(),
        lake_schema.empty() ? nullptr : lake_schema.c_str(),
        lake_table.empty() ? nullptr : lake_table.c_str(),
        event.op.empty() ? "?" : event.op.c_str(),
        event.event_id.empty() ? nullptr : event.event_id.c_str(),
        event.topic.empty() ? nullptr : event.topic.c_str(),
        partition.c_str(),
        offset.c_str(),
        event.gtid.empty() ? nullptr : event.gtid.c_str(),
        ts_ms.c_str(),
        payload.c_str(),
        sqlstate.empty() ? nullptr : sqlstate.c_str(),
        error_message.c_str(),
        reason_str.c_str(),
        batch_id.empty() ? nullptr : batch_id.c_str(),
    };

    PGresult* res = PQexecParams(
        app_pg,
        "INSERT INTO cdc_catalog.apply_dlq ("
        "  catalog_id, conn_id, db_engine, source_schema, source_table,"
        "  lake_schema, lake_table, op, event_id, kafka_topic, kafka_partition,"
        "  kafka_offset, gtid, event_ts_ms, payload, error_sqlstate, error_message,"
        "  reason, batch_id"
        ") VALUES ("
        "  $1::bigint, $2, $3::cdc_catalog.db_engine, $4, $5,"
        "  $6, $7, $8, $9, $10, $11::integer,"
        "  $12::bigint, $13, $14::bigint, $15::jsonb, $16, $17,"
        "  $18, $19"
        ")",
        19, nullptr, vals, nullptr, nullptr, 0);

    const bool ok = res && PQresultStatus(res) == PGRES_COMMAND_OK;
    if (res) {
        PQclear(res);
    }
    return ok;
}

long long parked_count(PGconn* app_pg, long long catalog_id) {
    if (!app_pg || catalog_id <= 0) {
        return 0;
    }
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(
        app_pg,
        "SELECT COUNT(*)::bigint FROM cdc_catalog.apply_dlq "
        "WHERE catalog_id = $1::bigint AND status = 'parked'",
        1, nullptr, vals, nullptr, nullptr, 0);
    long long count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1) {
        count = std::atoll(PQgetvalue(res, 0, 0));
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

}  // namespace apply_dlq
