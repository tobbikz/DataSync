#include "cdc_gap.hpp"

#include "capture_common.hpp"
#include "config.hpp"
#include "mariadb_conn.hpp"
#include "obs_log.hpp"

#include <nlohmann/json.hpp>

#include <cstring>

namespace {

using json = nlohmann::json;

std::string truncate_detail(const std::string& detail, std::size_t max_len = 2000) {
    return detail.size() > max_len ? detail.substr(0, max_len) : detail;
}

}  // namespace

long long record_gap_event(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    GapSide side,
    GapKind kind,
    const std::string& detail,
    const json& context,
    const std::string& remediation,
    int tables_flagged,
    const std::string& batch_id,
    std::optional<long long> catalog_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return 0;
    }
    const std::string detail_trim = truncate_detail(detail);
    const std::string ctx_json = context.dump();
    const std::string tables_flagged_str = std::to_string(tables_flagged);
    std::string catalog_str;
    if (catalog_id.has_value()) {
        catalog_str = std::to_string(*catalog_id);
    }
    const char* catalog_val = catalog_id.has_value() ? catalog_str.c_str() : nullptr;
    const char* schema_val =
        source_schema.has_value() && !source_schema->empty() ? source_schema->c_str() : nullptr;
    const char* table_val =
        source_table.has_value() && !source_table->empty() ? source_table->c_str() : nullptr;
    const char* vals[] = {
        conn_id.c_str(),
        db_engine.c_str(),
        gap_side_str(side),
        gap_kind_str(kind),
        detail_trim.c_str(),
        ctx_json.c_str(),
        remediation.c_str(),
        tables_flagged_str.c_str(),
        batch_id.c_str(),
        catalog_val,
        schema_val,
        table_val,
    };
    PGresult* res = PQexecParams(
        pg,
        R"(
        INSERT INTO cdc_catalog.gap_events (
            conn_id, db_engine, gap_side, gap_kind, detail, context,
            remediation, tables_flagged, batch_id, catalog_id,
            source_schema, source_table
        ) VALUES (
            $1::text, $2::cdc_catalog.db_engine, $3::text, $4::text, $5::text,
            $6::jsonb, $7::text, $8::integer, $9::text,
            NULLIF($10::text, '')::bigint, NULLIF($11::text, ''), NULLIF($12::text, '')
        )
        RETURNING gap_id
        )",
        12,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    long long gap_id = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        gap_id = std::atoll(PQgetvalue(res, 0, 0));
    } else if (pg) {
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_gap_playbook",
            .message = "gap_events insert failed",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = source_schema,
            .source_table = source_table,
            .context = {
                {"pg_error", PQerrorMessage(pg)},
                {"result_status", res ? std::to_string(static_cast<int>(PQresultStatus(res))) : "null"},
            },
        });
    }
    if (res) {
        PQclear(res);
    }
    return gap_id;
}

void mark_gap_event_resolved(PGconn* pg, long long gap_id, const std::string& remediation_note) {
    if (!pg || gap_id <= 0) {
        return;
    }
    const std::string note = truncate_detail(remediation_note, 500);
    const std::string gap_id_str = std::to_string(gap_id);
    const char* vals[] = {gap_id_str.c_str(), note.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.gap_events
        SET resolved_at = now(),
            remediation = $2::text
        WHERE gap_id = $1::bigint
          AND resolved_at IS NULL
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

int resolve_stale_gap_events(PGconn* pg) {
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return 0;
    }
    PGresult* res = PQexec(
        pg,
        R"(
        UPDATE cdc_catalog.gap_events g
        SET resolved_at = now(),
            remediation = 'resolved: pipeline healthy'
        WHERE g.resolved_at IS NULL
          AND (
            (
              g.catalog_id IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM cdc_catalog.catalog c
                WHERE c.catalog_id = g.catalog_id
                  AND c.active
                  AND NOT c.needs_full_load
                  AND c.status NOT IN (
                    'pending'::cdc_catalog.replication_status,
                    'needs_full_load'::cdc_catalog.replication_status,
                    'full_load_in_progress'::cdc_catalog.replication_status,
                    'error'::cdc_catalog.replication_status,
                    'quarantined'::cdc_catalog.replication_status
                  )
              )
              AND NOT EXISTS (
                SELECT 1 FROM cdc_catalog.apply_position ap
                WHERE ap.catalog_id = g.catalog_id
                  AND ap.status = 'gap_detected'::cdc_catalog.cdc_health_status
              )
            )
            OR (
              g.gap_side = 'capture'
              AND NOT EXISTS (
                SELECT 1 FROM cdc_catalog.catalog c
                WHERE c.conn_id = g.conn_id
                  AND c.active
                  AND c.needs_full_load
              )
              AND NOT EXISTS (
                SELECT 1 FROM cdc_catalog.capture_position cp
                WHERE cp.conn_id = g.conn_id
                  AND cp.status = 'gap_detected'::cdc_catalog.cdc_health_status
              )
            )
          )
        )");
    int resolved = 0;
    if (res && PQresultStatus(res) == PGRES_COMMAND_OK) {
        resolved = std::atoi(PQcmdTuples(res));
    }
    if (res) {
        PQclear(res);
    }
    return resolved;
}

void clear_capture_gap_status(PGconn* pg, const std::string& conn_id) {
    if (!pg) {
        return;
    }
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.capture_position
        SET status = 'healthy'::cdc_catalog.cdc_health_status,
            last_error = NULL,
            updated_at = now()
        WHERE conn_id = $1
          AND status = 'gap_detected'::cdc_catalog.cdc_health_status
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

void mark_apply_position_gap_detected(PGconn* pg, long long catalog_id, const std::string& reason) {
    if (!pg || catalog_id <= 0) {
        return;
    }
    const std::string err = truncate_detail(reason);
    const std::string catalog_id_str = std::to_string(catalog_id);
    const char* vals[] = {catalog_id_str.c_str(), err.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.apply_position
        SET status = 'gap_detected'::cdc_catalog.cdc_health_status,
            last_error = $2,
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (res) {
        PQclear(res);
    }
}

bool recover_apply_table_gap(
    PGconn* pg,
    long long catalog_id,
    const std::string& conn_id,
    const std::string& db_engine,
    GapKind kind,
    const std::string& detail,
    const std::string& batch_id,
    const json& context) {
    if (!pg || catalog_id <= 0) {
        return false;
    }
    const std::string detail_trim = truncate_detail(detail);
    const std::string catalog_id_str = std::to_string(catalog_id);
    const char* vals[] = {catalog_id_str.c_str(), detail_trim.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = true,
            cdc_enabled = true,
            status = 'pending'::cdc_catalog.replication_status,
            last_error = $2,
            engine_meta = engine_meta - 'stream_kafka_offsets' - 'stream_bookmarked_at',
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND active = true
          AND has_pk = true
          AND status NOT IN ('skipped'::cdc_catalog.replication_status, 'disabled'::cdc_catalog.replication_status)
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const bool flagged = res && PQresultStatus(res) == PGRES_COMMAND_OK && std::atoi(PQcmdTuples(res)) > 0;
    if (res) {
        PQclear(res);
    }
    if (!flagged) {
        return false;
    }

    std::optional<std::string> schema;
    std::optional<std::string> table;
    PGresult* meta = PQexecParams(
        pg,
        R"(
        SELECT source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    if (meta && PQresultStatus(meta) == PGRES_TUPLES_OK && PQntuples(meta) > 0) {
        if (!PQgetisnull(meta, 0, 0)) {
            schema = PQgetvalue(meta, 0, 0);
        }
        if (!PQgetisnull(meta, 0, 1)) {
            table = PQgetvalue(meta, 0, 1);
        }
    }
    if (meta) {
        PQclear(meta);
    }

    const long long gap_row = record_gap_event(
        pg,
        conn_id,
        db_engine,
        GapSide::Apply,
        kind,
        detail_trim,
        context,
        "needs_full_load",
        1,
        batch_id,
        catalog_id,
        schema,
        table);

    const char* clear_vals[] = {catalog_id_str.c_str()};
    PGresult* clear = PQexecParams(
        pg,
        R"(
        UPDATE cdc_catalog.apply_position
        SET status = 'healthy'::cdc_catalog.cdc_health_status,
            last_error = 'gap remediated: awaiting full-load reboot',
            updated_at = now()
        WHERE catalog_id = $1::bigint
        )",
        1,
        nullptr,
        clear_vals,
        nullptr,
        nullptr,
        0);
    if (clear) {
        PQclear(clear);
    }

    log_write(pg, {
        .level = LogLevel::Warning,
        .component = "cdc_gap_playbook",
        .message = "apply gap remediated: table flagged for full-load reboot",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = schema,
        .source_table = table,
        .context = {
            {"gap_kind", gap_kind_str(kind)},
            {"catalog_id", catalog_id},
            {"gap_event_id", gap_row},
        },
    });
    return true;
}

BinlogGapRebootResult recover_mariadb_capture_gap(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& conn_id,
    const std::string& batch_id,
    GapKind kind,
    const std::string& detail,
    const json& context) {
    const auto reboot = reboot_conn_after_mariadb_binlog_gap(pg, mysql, conn_id, batch_id);
    if (reboot.ran) {
        record_gap_event(
            pg,
            conn_id,
            "mariadb",
            GapSide::Capture,
            kind,
            detail,
            context,
            reboot.tables_flagged > 0 ? "needs_full_load" : "t0_reset",
            reboot.tables_flagged,
            batch_id);
        clear_capture_gap_status(pg, conn_id);
        log_write(pg, {
            .level = LogLevel::Warning,
            .component = "cdc_gap_playbook",
            .message = "mariadb capture gap recovered",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"gap_kind", gap_kind_str(kind)},
                {"t0_reset", reboot.t0_reset},
                {"tables_flagged", reboot.tables_flagged},
            },
        });
    }
    return reboot;
}

GapSweepResult run_gap_playbook_sweep(const AppConfig& cfg, PGconn* pg, const std::string& batch_id) {
    GapSweepResult out;
    if (!pg || PQstatus(pg) != CONNECTION_OK) {
        return out;
    }

    PGresult* capture_gaps = PQexec(
        pg,
        R"(
        SELECT conn_id, COALESCE(last_error, '') AS detail
        FROM cdc_catalog.capture_position
        WHERE status = 'gap_detected'::cdc_catalog.cdc_health_status
        )");
    if (capture_gaps && PQresultStatus(capture_gaps) == PGRES_TUPLES_OK) {
        const int n = PQntuples(capture_gaps);
        for (int i = 0; i < n; ++i) {
            const std::string conn_id = PQgetvalue(capture_gaps, i, 0);
            const std::string detail = PQgetvalue(capture_gaps, i, 1);
            const std::string db_engine = conn_engine(cfg, conn_id);
            const GapKind kind = infer_gap_kind_from_detail(detail);
            bool recovered = false;

            if (db_engine == "mariadb") {
                const MariaDbSource* source = find_mariadb_source(cfg, conn_id);
                if (source) {
                    try {
                        MariaDbConn mariadb(*source);
                        const auto reboot =
                            recover_mariadb_capture_gap(pg, mariadb.handle, conn_id, batch_id, kind, detail);
                        recovered = reboot.ran;
                    } catch (const std::exception& ex) {
                        log_write(pg, {
                            .level = LogLevel::Error,
                            .component = "cdc_gap_playbook",
                            .message = "capture gap sweep failed for mariadb conn",
                            .batch_id = batch_id,
                            .conn_id = conn_id,
                            .source_schema = std::nullopt,
                            .source_table = std::nullopt,
                            .context = {{"error", ex.what()}, {"detail", detail}},
                        });
                    }
                }
            } else if (db_engine == "mssql") {
#ifdef HAVE_FREETDS
                const auto reboot = reboot_conn_after_mssql_cdc_gap(cfg, pg, conn_id, batch_id);
                if (reboot.ran) {
                    record_gap_event(
                        pg,
                        conn_id,
                        "mssql",
                        GapSide::Capture,
                        GapKind::MssqlLsnPurged,
                        detail.empty() ? "mssql cdc gap sweep" : detail,
                        json::object(),
                        "t0_reset",
                        reboot.tables_flagged,
                        batch_id);
                    clear_capture_gap_status(pg, conn_id);
                    recovered = true;
                }
#else
                (void)kind;
#endif
            }

            if (recovered) {
                out.capture_recovered += 1;
            } else {
                out.unresolved_capture += 1;
            }
        }
    }
    if (capture_gaps) {
        PQclear(capture_gaps);
    }

    PGresult* apply_gaps = PQexec(
        pg,
        R"(
        SELECT ap.catalog_id, c.conn_id, c.db_engine::text, COALESCE(ap.last_error, '') AS detail,
               c.source_schema, c.source_table
        FROM cdc_catalog.apply_position ap
        JOIN cdc_catalog.catalog c ON c.catalog_id = ap.catalog_id
        WHERE ap.status = 'gap_detected'::cdc_catalog.cdc_health_status
          AND c.active = true
        )");
    if (apply_gaps && PQresultStatus(apply_gaps) == PGRES_TUPLES_OK) {
        const int n = PQntuples(apply_gaps);
        for (int i = 0; i < n; ++i) {
            const long long catalog_id = std::atoll(PQgetvalue(apply_gaps, i, 0));
            const std::string conn_id = PQgetvalue(apply_gaps, i, 1);
            const std::string db_engine = PQgetvalue(apply_gaps, i, 2);
            const std::string detail = PQgetvalue(apply_gaps, i, 3);
            const GapKind kind = infer_gap_kind_from_detail(detail);
            if (recover_apply_table_gap(pg, catalog_id, conn_id, db_engine, kind, detail, batch_id)) {
                out.apply_recovered += 1;
            } else {
                out.unresolved_apply += 1;
            }
        }
    }
    if (apply_gaps) {
        PQclear(apply_gaps);
    }

    if (out.capture_recovered > 0 || out.apply_recovered > 0 || out.unresolved_capture > 0 ||
        out.unresolved_apply > 0) {
        log_write(pg, {
            .level = (out.unresolved_capture > 0 || out.unresolved_apply > 0) ? LogLevel::Warning
                                                                               : LogLevel::Info,
            .component = "cdc_gap_playbook",
            .message = "gap playbook sweep completed",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {
                {"capture_recovered", out.capture_recovered},
                {"apply_recovered", out.apply_recovered},
                {"unresolved_capture", out.unresolved_capture},
                {"unresolved_apply", out.unresolved_apply},
            },
        });
    }

    const int stale_resolved = resolve_stale_gap_events(pg);
    if (stale_resolved > 0) {
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "cdc_gap_playbook",
            .message = "stale gap_events auto-resolved",
            .batch_id = batch_id,
            .conn_id = std::nullopt,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"resolved", stale_resolved}},
        });
    }

    return out;
}
