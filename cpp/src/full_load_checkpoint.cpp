#include "full_load_checkpoint.hpp"

#include "obs_log.hpp"
#include "pg_conn.hpp"

#include <stdexcept>

namespace {

FullLoadCheckpoint row_to_checkpoint(PGresult* res, int row) {
    FullLoadCheckpoint cp;
    cp.catalog_id = std::atoll(PQgetvalue(res, row, 0));
    cp.worker_id = std::atoi(PQgetvalue(res, row, 1));
    cp.batch_id = PQgetvalue(res, row, 2);
    const auto phase = full_load_phase_from_string(PQgetvalue(res, row, 3));
    if (!phase) {
        throw std::runtime_error("invalid full_load_checkpoint phase");
    }
    cp.phase = *phase;
    if (!PQgetisnull(res, row, 4)) {
        cp.last_pk = nlohmann::json::parse(PQgetvalue(res, row, 4));
    }
    cp.rows_loaded = std::atoll(PQgetvalue(res, row, 5));
    if (!PQgetisnull(res, row, 6)) {
        cp.source_rows = std::atoll(PQgetvalue(res, row, 6));
    }
    return cp;
}

}  // namespace

std::string full_load_phase_to_string(FullLoadPhase phase) {
    switch (phase) {
        case FullLoadPhase::Truncate:
            return "truncate";
        case FullLoadPhase::Ddl:
            return "ddl";
        case FullLoadPhase::Copy:
            return "copy";
    }
    return "truncate";
}

std::optional<FullLoadPhase> full_load_phase_from_string(const std::string& value) {
    if (value == "truncate") {
        return FullLoadPhase::Truncate;
    }
    if (value == "ddl") {
        return FullLoadPhase::Ddl;
    }
    if (value == "copy") {
        return FullLoadPhase::Copy;
    }
    return std::nullopt;
}

std::optional<FullLoadCheckpoint> load_full_load_checkpoint(
    PGconn* pg,
    long long catalog_id,
    int worker_id) {
    const std::string cid = std::to_string(catalog_id);
    const std::string wid = std::to_string(worker_id);
    const char* vals[] = {cid.c_str(), wid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT catalog_id, worker_id, batch_id, phase, last_pk, rows_loaded, source_rows
        FROM cdc_catalog.full_load_checkpoint
        WHERE catalog_id = $1::bigint AND worker_id = $2::int
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::optional<FullLoadCheckpoint> out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out = row_to_checkpoint(res, 0);
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

std::vector<FullLoadCheckpoint> load_full_load_checkpoints(PGconn* pg, long long catalog_id) {
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT catalog_id, worker_id, batch_id, phase, last_pk, rows_loaded, source_rows
        FROM cdc_catalog.full_load_checkpoint
        WHERE catalog_id = $1::bigint
        ORDER BY worker_id
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<FullLoadCheckpoint> out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        for (int i = 0; i < PQntuples(res); ++i) {
            out.push_back(row_to_checkpoint(res, i));
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

void save_full_load_checkpoint(PGconn* pg, const FullLoadCheckpoint& checkpoint) {
    const std::string cid = std::to_string(checkpoint.catalog_id);
    const std::string wid = std::to_string(checkpoint.worker_id);
    const std::string phase = full_load_phase_to_string(checkpoint.phase);
    const std::string rows = std::to_string(checkpoint.rows_loaded);
    const std::string last_pk = checkpoint.last_pk.dump();
    const char* vals[] = {
        cid.c_str(),
        wid.c_str(),
        checkpoint.batch_id.c_str(),
        phase.c_str(),
        last_pk.c_str(),
        rows.c_str(),
        checkpoint.source_rows ? std::to_string(*checkpoint.source_rows).c_str() : nullptr,
    };
    pg_exec_params_simple(
        pg,
        R"(
        INSERT INTO cdc_catalog.full_load_checkpoint
            (catalog_id, worker_id, batch_id, phase, last_pk, rows_loaded, source_rows, updated_at)
        VALUES ($1::bigint, $2::int, $3, $4, $5::jsonb, $6::bigint, $7::bigint, now())
        ON CONFLICT (catalog_id, worker_id) DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            phase = EXCLUDED.phase,
            last_pk = EXCLUDED.last_pk,
            rows_loaded = EXCLUDED.rows_loaded,
            source_rows = COALESCE(EXCLUDED.source_rows, cdc_catalog.full_load_checkpoint.source_rows),
            updated_at = now()
        )",
        7,
        vals);
}

void clear_full_load_checkpoints(PGconn* pg, long long catalog_id) {
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    pg_exec_params_simple(
        pg,
        "DELETE FROM cdc_catalog.full_load_checkpoint WHERE catalog_id = $1::bigint",
        1,
        vals);
}

std::vector<std::string> last_pk_from_json(const nlohmann::json& last_pk) {
    std::vector<std::string> out;
    if (!last_pk.is_array()) {
        return out;
    }
    for (const auto& v : last_pk) {
        if (v.is_string()) {
            out.push_back(v.get<std::string>());
        } else if (v.is_number_integer()) {
            out.push_back(std::to_string(v.get<long long>()));
        } else if (v.is_number_unsigned()) {
            out.push_back(std::to_string(v.get<unsigned long long>()));
        } else if (v.is_number_float()) {
            out.push_back(std::to_string(v.get<double>()));
        } else if (!v.is_null()) {
            out.push_back(v.dump());
        }
    }
    return out;
}

nlohmann::json last_pk_to_json(const std::vector<std::string>& last_pk) {
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& v : last_pk) {
        arr.push_back(v);
    }
    return arr;
}

bool conn_has_active_copy_checkpoints(PGconn* pg, const std::string& conn_id) {
    const char* vals[] = {conn_id.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT EXISTS (
            SELECT 1
            FROM cdc_catalog.full_load_checkpoint cp
            JOIN cdc_catalog.catalog c ON c.catalog_id = cp.catalog_id
            WHERE c.conn_id = $1
              AND cp.phase = 'copy'
        )
        )",
        1,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    bool found = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        found = PQgetvalue(res, 0, 0)[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return found;
}

int recover_full_load_for_checkpoint_resume(
    PGconn* pg,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& batch_id) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        WITH recoverable AS (
            SELECT c.catalog_id
            FROM cdc_catalog.catalog c
            WHERE c.conn_id = $1
              AND c.db_engine = $2::cdc_catalog.db_engine
              AND c.status = 'full_load_in_progress'
              AND c.needs_full_load = true
              AND EXISTS (
                  SELECT 1
                  FROM cdc_catalog.full_load_checkpoint cp
                  WHERE cp.catalog_id = c.catalog_id
                    AND cp.phase = 'copy'
              )
        )
        UPDATE cdc_catalog.catalog c
        SET status = 'pending',
            updated_at = now()
        FROM recoverable r
        WHERE c.catalog_id = r.catalog_id
        RETURNING c.catalog_id, c.source_schema, c.source_table
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    int count = 0;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        count = PQntuples(res);
    }
    if (count > 0) {
        nlohmann::json tables = nlohmann::json::array();
        for (int i = 0; i < count && i < 10; ++i) {
            tables.push_back({
                {"catalog_id", std::atoll(PQgetvalue(res, i, 0))},
                {"source_schema", PQgetvalue(res, i, 1)},
                {"source_table", PQgetvalue(res, i, 2)},
            });
        }
        log_write(pg, {
            .level = LogLevel::Info,
            .component = "catalog",
            .message = "full load resume ready",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
            .context = {{"tables_recovered", count}, {"sample", tables}, {"db_engine", db_engine}},
        });
    }
    if (res) {
        PQclear(res);
    }
    return count;
}

std::optional<long long> truncate_baseline_source_rows(
    const std::vector<FullLoadCheckpoint>& checkpoints) {
    for (const auto& cp : checkpoints) {
        if (cp.phase == FullLoadPhase::Truncate && cp.source_rows.has_value()) {
            return cp.source_rows;
        }
    }
    return std::nullopt;
}
