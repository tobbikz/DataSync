#include "snapshot_signal.hpp"

#include "capture_common.hpp"
#include "pg_conn.hpp"

#include <atomic>
#include <chrono>
#include <thread>

namespace snapshot_signal {
namespace {

Signal row_to_signal(PGresult* res, int row) {
    Signal s;
    s.signal_id = std::atoll(PQgetvalue(res, row, 0));
    if (!PQgetisnull(res, row, 1) && PQgetvalue(res, row, 1)[0]) {
        s.conn_id = PQgetvalue(res, row, 1);
    }
    if (!PQgetisnull(res, row, 2) && PQgetvalue(res, row, 2)[0]) {
        s.catalog_id = std::atoll(PQgetvalue(res, row, 2));
    }
    if (auto a = action_from_string(PQgetvalue(res, row, 3))) {
        s.action = *a;
    }
    if (auto st = status_from_string(PQgetvalue(res, row, 4))) {
        s.status = *st;
    }
    if (!PQgetisnull(res, row, 5)) {
        try {
            s.payload = nlohmann::json::parse(PQgetvalue(res, row, 5));
        } catch (...) {
            s.payload = nlohmann::json::object();
        }
    }
    if (!PQgetisnull(res, row, 6)) {
        s.last_error = PQgetvalue(res, row, 6);
    }
    return s;
}

}  // namespace

long long enqueue(
    PGconn* pg,
    Action action,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id,
    const nlohmann::json& payload) {
    const std::string action_s = action_to_string(action);
    const std::string payload_s = payload.dump();
    const std::string conn_s = conn_id.value_or("");
    const std::string cat_s = catalog_id ? std::to_string(*catalog_id) : "";
    const char* vals[] = {
        conn_id ? conn_s.c_str() : nullptr,
        catalog_id ? cat_s.c_str() : nullptr,
        action_s.c_str(),
        payload_s.c_str(),
    };
    PGresult* res = PQexecParams(
        pg,
        "INSERT INTO cdc_catalog.snapshot_signal (conn_id, catalog_id, action, payload)"
        " VALUES ($1, NULLIF($2,'')::bigint, $3, $4::jsonb)"
        " RETURNING signal_id",
        4,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        const std::string err = pg ? PQerrorMessage(pg) : "null";
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("snapshot_signal enqueue failed: " + err);
    }
    const long long id = std::atoll(PQgetvalue(res, 0, 0));
    PQclear(res);
    return id;
}

std::optional<Signal> fetch(PGconn* pg, long long signal_id) {
    const std::string id = std::to_string(signal_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT signal_id, conn_id, catalog_id::text, action, status, payload::text, COALESCE(last_error,'')"
        " FROM cdc_catalog.snapshot_signal WHERE signal_id = $1::bigint",
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
        return std::nullopt;
    }
    if (PQntuples(res) < 1) {
        PQclear(res);
        return std::nullopt;
    }
    Signal s = row_to_signal(res, 0);
    PQclear(res);
    return s;
}

std::optional<Signal> latest_active(
    PGconn* pg,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id) {
    const std::string conn_s = conn_id.value_or("");
    const std::string cat_s = catalog_id ? std::to_string(*catalog_id) : "";
    const char* vals[] = {
        conn_id ? conn_s.c_str() : "",
        catalog_id ? cat_s.c_str() : "",
    };
    PGresult* res = PQexecParams(
        pg,
        "SELECT signal_id, conn_id, catalog_id::text, action, status, payload::text, COALESCE(last_error,'')"
        " FROM cdc_catalog.snapshot_signal"
        " WHERE status IN ('pending','running','paused')"
        "   AND ($1 = '' OR conn_id = $1 OR conn_id IS NULL)"
        "   AND ($2 = '' OR catalog_id = NULLIF($2,'')::bigint OR catalog_id IS NULL)"
        " ORDER BY requested_at DESC LIMIT 1",
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
        return std::nullopt;
    }
    Signal s = row_to_signal(res, 0);
    PQclear(res);
    return s;
}

void set_status(
    PGconn* pg,
    long long signal_id,
    Status status,
    const std::optional<std::string>& last_error) {
    const std::string id = std::to_string(signal_id);
    const std::string st = status_to_string(status);
    const std::string err = last_error.value_or("");
    const char* vals[] = {id.c_str(), st.c_str(), last_error ? err.c_str() : nullptr};
    pg_exec_params_simple(
        pg,
        "UPDATE cdc_catalog.snapshot_signal"
        " SET status = $2, last_error = COALESCE($3, last_error), updated_at = now()"
        " WHERE signal_id = $1::bigint",
        3,
        vals);
}

bool poll_cooperative_stop(PGconn* pg, const std::string& conn_id, long long catalog_id) {
    while (!g_shutdown.load()) {
        auto sig = latest_active(pg, conn_id, catalog_id);
        if (!sig) {
            return false;
        }
        if (sig->action == Action::Stop) {
            set_status(pg, sig->signal_id, Status::Completed);
            return true;
        }
        if (sig->action == Action::Pause || sig->status == Status::Paused) {
            if (sig->status != Status::Paused) {
                set_status(pg, sig->signal_id, Status::Paused);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            // Look for resume/stop
            auto again = latest_active(pg, conn_id, catalog_id);
            if (again && again->action == Action::Resume) {
                set_status(pg, again->signal_id, Status::Completed);
                return false;
            }
            if (again && again->action == Action::Stop) {
                set_status(pg, again->signal_id, Status::Completed);
                return true;
            }
            continue;
        }
        return false;
    }
    return true;
}

bool blocking_active_for_catalog(PGconn* pg, long long catalog_id) {
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM cdc_catalog.snapshot_signal"
        " WHERE catalog_id = $1::bigint"
        "   AND action = 'execute_blocking'"
        "   AND status IN ('pending','running','paused')"
        " LIMIT 1",
        1,
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

}  // namespace snapshot_signal
