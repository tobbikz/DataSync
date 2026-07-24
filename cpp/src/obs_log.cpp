#include "obs_log.hpp"

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace {

const char* level_name(LogLevel level) {
    switch (level) {
        case LogLevel::Debug:
            return "debug";
        case LogLevel::Info:
            return "info";
        case LogLevel::Warning:
            return "warning";
        case LogLevel::Error:
            return "error";
    }
    return "info";
}

const char* opt_cstr(const std::optional<std::string>& v) {
    return v && !v->empty() ? v->c_str() : nullptr;
}

}  // namespace

std::string make_batch_id() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
    return oss.str();
}

bool log_write(PGconn* pg, const LogEvent& event) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return false;
    }

    // Designated initializers without .context can yield json array [{}] — coerce to object.
    const nlohmann::json& ctx =
        event.context.is_object() ? event.context : nlohmann::json::object();
    const std::string context_json = ctx.empty() ? "{}" : ctx.dump();

    static const char* kSql = R"(
        INSERT INTO cdc_catalog.logs (
            level, component, message, context,
            batch_id, conn_id, source_schema, source_table
        ) VALUES (
            $1::cdc_catalog.log_level, $2, $3, $4::jsonb,
            $5, $6, $7, $8
        )
    )";

    const char* values[] = {
        level_name(event.level),
        event.component.c_str(),
        event.message.c_str(),
        context_json.c_str(),
        opt_cstr(event.batch_id),
        opt_cstr(event.conn_id),
        opt_cstr(event.source_schema),
        opt_cstr(event.source_table),
    };

    PGresult* res = PQexecParams(pg, kSql, 8, nullptr, values, nullptr, nullptr, 0);
    if (!res) {
        return false;
    }
    const bool ok = PQresultStatus(res) == PGRES_COMMAND_OK;
    PQclear(res);
    return ok;
}

long long purge_logs(PGconn* pg, int retention_days) {
    if (!pg || PQstatus(pg) != CONNECTION_OK || retention_days < 1) {
        return -1;
    }

    const std::string days = std::to_string(retention_days);
    const char* vals[] = {days.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT cdc_catalog.purge_logs($1::integer)",
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
        return -1;
    }

    const long long deleted = PQgetisnull(res, 0, 0) ? 0 : std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return deleted;
}
