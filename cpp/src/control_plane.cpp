#include "control_plane.hpp"

#include "pg_conn.hpp"
#include "snapshot_signal.hpp"

#include <fstream>
#include <sstream>

namespace control_plane {
namespace {

nlohmann::json load_json_file(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        return nlohmann::json::object();
    }
    try {
        nlohmann::json j;
        in >> j;
        return j.is_object() ? j : nlohmann::json::object();
    } catch (...) {
        return nlohmann::json::object();
    }
}

}  // namespace

void notify(
    PGconn* pg,
    const std::string& channel,
    const std::string& severity,
    const std::string& title,
    const std::string& body,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id,
    const nlohmann::json& context) {
    if (!pg) {
        return;
    }
    const std::string conn_s = conn_id.value_or("");
    const std::string cat_s = catalog_id ? std::to_string(*catalog_id) : "";
    const std::string ctx = context.dump();
    const char* vals[] = {
        channel.c_str(),
        severity.c_str(),
        conn_id ? conn_s.c_str() : nullptr,
        catalog_id ? cat_s.c_str() : nullptr,
        title.c_str(),
        body.c_str(),
        ctx.c_str(),
    };
    try {
        pg_exec_params_simple(
            pg,
            "INSERT INTO cdc_catalog.notifications"
            " (channel, severity, conn_id, catalog_id, title, body, context)"
            " VALUES ($1, $2, $3, NULLIF($4,'')::bigint, $5, $6, $7::jsonb)",
            7,
            vals);
    } catch (...) {
        // Notifications must not break the pipeline.
    }
}

bool reload_cdc_tuning(AppConfig& cfg, const std::string& config_path) {
    if (config_path.empty()) {
        return false;
    }
    const auto root = load_json_file(config_path);
    if (!root.contains("cdc") || !root["cdc"].is_object()) {
        return false;
    }
    const auto& j = root["cdc"];
    CdcConfig next = cfg.cdc;
    bool changed = false;
    auto set_int = [&](const char* key, int& dest) {
        if (j.contains(key) && j[key].is_number_integer()) {
            const int v = j[key].get<int>();
            if (v != dest) {
                dest = v;
                changed = true;
            }
        }
    };
    auto set_bool = [&](const char* key, bool& dest) {
        if (j.contains(key) && j[key].is_boolean()) {
            const bool v = j[key].get<bool>();
            if (v != dest) {
                dest = v;
                changed = true;
            }
        }
    };
    set_int("round_idle_seconds", next.round_idle_seconds);
    set_int("slice_max_seconds", next.slice_max_seconds);
    set_int("slice_max_events", next.slice_max_events);
    set_int("capture_worker_count", next.capture_worker_count);
    set_bool("apply_dedup", next.apply_dedup);
    set_bool("apply_audit", next.apply_audit);
    set_bool("mssql_replay_on_idle", next.mssql_replay_on_idle);
    set_bool("kafka_heartbeat", next.kafka_heartbeat);
    set_bool("kafka_tombstones", next.kafka_tombstones);
    set_bool("reselect_on_apply", next.reselect_on_apply);
    set_bool("cloudevents", next.cloudevents);
    set_bool("tracing", next.tracing);
    set_bool("openlineage", next.openlineage);
    set_bool("include_source_query", next.include_source_query);
    set_bool("schema_events", next.schema_events);
    if (j.contains("content_routing") && j["content_routing"].is_array()) {
        if (j["content_routing"] != next.content_routing) {
            next.content_routing = j["content_routing"];
            changed = true;
        }
    }
    if (j.contains("message_filter_exclude_ops") && j["message_filter_exclude_ops"].is_array()) {
        if (j["message_filter_exclude_ops"] != next.message_filter_exclude_ops) {
            next.message_filter_exclude_ops = j["message_filter_exclude_ops"];
            changed = true;
        }
    }
    if (j.contains("binlog_mode") && j["binlog_mode"].is_string()) {
        const std::string mode = j["binlog_mode"].get<std::string>();
        if ((mode == "auto" || mode == "cli" || mode == "native") && mode != next.binlog_mode) {
            next.binlog_mode = mode;
            changed = true;
        }
    }
    if (changed) {
        cfg.cdc = next;
    }
    return changed;
}

int drain_control_signals(PGconn* pg, AppConfig& cfg, const std::string& config_path) {
    if (!pg) {
        return 0;
    }
    PGresult* res = PQexec(
        pg,
        "SELECT signal_id, conn_id, catalog_id::text, action, status, payload::text"
        " FROM cdc_catalog.snapshot_signal"
        " WHERE status = 'pending'"
        "   AND action IN ('reload_config', 'log', 'notify')"
        " ORDER BY requested_at ASC"
        " LIMIT 50");
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return 0;
    }
    int handled = 0;
    for (int i = 0; i < PQntuples(res); ++i) {
        const long long sid = std::atoll(PQgetvalue(res, i, 0));
        const std::string action = PQgetvalue(res, i, 3) ? PQgetvalue(res, i, 3) : "";
        std::optional<std::string> conn_id;
        if (!PQgetisnull(res, i, 1) && PQgetvalue(res, i, 1)[0]) {
            conn_id = PQgetvalue(res, i, 1);
        }
        std::optional<long long> catalog_id;
        if (!PQgetisnull(res, i, 2) && PQgetvalue(res, i, 2)[0]) {
            catalog_id = std::atoll(PQgetvalue(res, i, 2));
        }
        nlohmann::json payload = nlohmann::json::object();
        if (!PQgetisnull(res, i, 5)) {
            try {
                payload = nlohmann::json::parse(PQgetvalue(res, i, 5));
            } catch (...) {
            }
        }
        try {
            if (action == "reload_config") {
                const bool changed = reload_cdc_tuning(cfg, config_path);
                notify(
                    pg,
                    "ops",
                    "info",
                    "config reload",
                    changed ? "CDC tuning reloaded from config.json" : "CDC tuning unchanged",
                    conn_id,
                    catalog_id,
                    {{"changed", changed}});
            } else if (action == "log") {
                const std::string msg = payload.value("message", "signal log");
                notify(pg, "ops", payload.value("severity", "info"), "signal log", msg, conn_id, catalog_id, payload);
            } else if (action == "notify") {
                notify(
                    pg,
                    payload.value("channel", "ops"),
                    payload.value("severity", "info"),
                    payload.value("title", "notify"),
                    payload.value("body", ""),
                    conn_id,
                    catalog_id,
                    payload);
            }
            snapshot_signal::set_status(pg, sid, snapshot_signal::Status::Completed);
            ++handled;
        } catch (const std::exception& ex) {
            snapshot_signal::set_status(pg, sid, snapshot_signal::Status::Failed, ex.what());
        }
    }
    PQclear(res);
    return handled;
}

void emit_lineage(
    PGconn* pg,
    const std::string& event_type,
    const std::optional<std::string>& conn_id,
    const std::optional<long long>& catalog_id,
    const std::string& run_id,
    const nlohmann::json& inputs,
    const nlohmann::json& outputs,
    const nlohmann::json& context) {
    if (!pg) {
        return;
    }
    const std::string conn_s = conn_id.value_or("");
    const std::string cat_s = catalog_id ? std::to_string(*catalog_id) : "";
    const std::string in_s = inputs.dump();
    const std::string out_s = outputs.dump();
    const std::string ctx_s = context.dump();
    const char* vals[] = {
        event_type.c_str(),
        conn_id ? conn_s.c_str() : nullptr,
        catalog_id ? cat_s.c_str() : nullptr,
        "datasync",
        run_id.empty() ? nullptr : run_id.c_str(),
        in_s.c_str(),
        out_s.c_str(),
        ctx_s.c_str(),
    };
    try {
        pg_exec_params_simple(
            pg,
            "INSERT INTO cdc_catalog.lineage_events"
            " (event_type, conn_id, catalog_id, job_name, run_id, inputs, outputs, context)"
            " VALUES ($1, $2, NULLIF($3,'')::bigint, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb)",
            8,
            vals);
    } catch (...) {
    }
}

bool apply_event_already_seen(
    PGconn* pg,
    long long catalog_id,
    const std::string& event_id) {
    if (!pg || catalog_id <= 0 || event_id.empty()) {
        return false;
    }
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str(), event_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM cdc_catalog.apply_dedup WHERE catalog_id = $1::bigint AND event_id = $2 LIMIT 1",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool seen = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return seen;
}

bool claim_apply_event(
    PGconn* pg,
    long long catalog_id,
    const std::string& event_id,
    const std::string& batch_id) {
    if (!pg || catalog_id <= 0 || event_id.empty()) {
        return true;
    }
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str(), event_id.c_str(), batch_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "INSERT INTO cdc_catalog.apply_dedup (catalog_id, event_id, batch_id)"
        " VALUES ($1::bigint, $2, $3)"
        " ON CONFLICT (catalog_id, event_id) DO NOTHING"
        " RETURNING event_id",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool claimed = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return claimed;
}

void audit_apply_event(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& event_id,
    const std::string& op,
    const std::string& schema,
    const std::string& table,
    const std::string& topic,
    int partition,
    long long offset,
    const std::string& batch_id) {
    if (!pg) {
        return;
    }
    const std::string cid = catalog_id > 0 ? std::to_string(catalog_id) : "";
    const std::string part = std::to_string(partition);
    const std::string off = std::to_string(offset);
    const char* vals[] = {
        catalog_id > 0 ? cid.c_str() : nullptr,
        conn_id.c_str(),
        event_id.empty() ? nullptr : event_id.c_str(),
        op.empty() ? nullptr : op.c_str(),
        schema.empty() ? nullptr : schema.c_str(),
        table.empty() ? nullptr : table.c_str(),
        topic.empty() ? nullptr : topic.c_str(),
        part.c_str(),
        off.c_str(),
        batch_id.empty() ? nullptr : batch_id.c_str(),
    };
    try {
        pg_exec_params_simple(
            pg,
            "INSERT INTO cdc_catalog.apply_audit"
            " (catalog_id, conn_id, event_id, op, source_schema, source_table,"
            "  kafka_topic, kafka_partition, kafka_offset, batch_id)"
            " VALUES (NULLIF($1,'')::bigint, $2, $3, $4, $5, $6, $7, $8::int, $9::bigint, $10)",
            10,
            vals);
    } catch (...) {
    }
}

}  // namespace control_plane
