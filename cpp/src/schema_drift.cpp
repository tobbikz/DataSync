#include "schema_drift.hpp"

#include "mariadb_schema.hpp"
#include "pg_conn.hpp"

#include <cstdlib>
#include <set>
#include <sstream>

namespace {

std::string join_names(const std::vector<std::string>& names) {
    std::ostringstream out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i) {
            out << ", ";
        }
        out << names[i];
    }
    return out.str();
}

}  // namespace

std::string SchemaDriftPlan::reason() const {
    if (pk_column_removed) {
        return "schema drift: primary key column dropped in source (" + join_names(removed) +
               "): auto full-load reboot";
    }
    if (ambiguous) {
        return "schema drift: columns added and dropped together, possible rename (added: " +
               join_names(added) + "; dropped: " + join_names(removed) + "): auto full-load reboot";
    }
    return {};
}

SchemaDriftPlan plan_schema_drift(
    const std::vector<std::string>& source_cols,
    const std::vector<std::string>& lake_cols,
    const std::vector<std::string>& pk_cols) {
    SchemaDriftPlan plan;

    // An empty source list is a failed read (permissions, dropped table, transient error),
    // never a table with no columns. Reading it as "every column was dropped" would empty
    // the lake table, so nothing drifts until the source answers again.
    if (source_cols.empty()) {
        plan.source_unavailable = true;
        return plan;
    }

    const std::set<std::string> source_set(source_cols.begin(), source_cols.end());
    const std::set<std::string> lake_set(lake_cols.begin(), lake_cols.end());
    const std::set<std::string> pk_set(pk_cols.begin(), pk_cols.end());

    for (const auto& col : source_cols) {
        if (!lake_set.count(col)) {
            plan.added.push_back(col);
        }
    }
    for (const auto& col : lake_cols) {
        if (!source_set.count(col)) {
            plan.removed.push_back(col);
            if (pk_set.count(col)) {
                plan.pk_column_removed = true;
            }
        }
    }
    plan.ambiguous = !plan.added.empty() && !plan.removed.empty();
    return plan;
}

std::vector<std::string> droppable_columns(
    const SchemaDriftPlan& plan,
    const std::vector<std::string>& pk_cols) {
    const std::set<std::string> pk_set(pk_cols.begin(), pk_cols.end());
    std::vector<std::string> out;
    for (const auto& col : plan.removed) {
        if (!pk_set.count(col)) {
            out.push_back(col);
        }
    }
    return out;
}

std::vector<std::string> fetch_lake_data_columns(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& table) {
    std::vector<std::string> out;
    if (!lake_pg) {
        return out;
    }
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        lake_pg,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "AND column_name NOT LIKE '\\_dl\\_%' "
        "ORDER BY ordinal_position",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
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

int drop_lake_columns(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& columns) {
    int dropped = 0;
    const std::string fq = pg_ident(schema) + "." + pg_ident(table);
    for (const auto& col : columns) {
        // Guard the bookkeeping columns even if a caller hands them over by mistake: the
        // partition key lives among them and dropping it takes the table with it.
        if (col.rfind("_dl_", 0) == 0) {
            continue;
        }
        pg_exec(lake_pg, "ALTER TABLE " + fq + " DROP COLUMN IF EXISTS " + pg_ident(col));
        dropped += 1;
    }
    return dropped;
}

std::string type_migration_drift_reason(
    const std::string& column,
    const std::string& from_type,
    const std::string& to_type) {
    return "schema drift: lake column " + column + " cannot migrate in place (" + from_type + " -> " +
           to_type + "): auto full-load reboot";
}

bool request_full_load_reboot(
    PGconn* catalog_pg,
    long long catalog_id,
    const std::string& reason) {
    if (!catalog_pg || catalog_id <= 0) {
        return false;
    }
    const std::string id = std::to_string(catalog_id);
    const char* vals[] = {id.c_str(), reason.c_str()};
    // capture_during_full_load only means something where CDC can stream into Kafka while the
    // COPY runs; MSSQL cannot, and instead replays from the LSN anchored before the COPY.
    PGresult* res = PQexecParams(
        catalog_pg,
        R"(
        UPDATE cdc_catalog.catalog
        SET needs_full_load = true,
            capture_during_full_load = (db_engine <> 'mssql'),
            cdc_enabled = true,
            status = 'pending',
            last_error = $2,
            last_error_at = now(),
            engine_meta = engine_meta - 'stream_kafka_offsets' - 'stream_bookmarked_at',
            updated_at = now()
        WHERE catalog_id = $1::bigint
          AND active = true
          AND needs_full_load = false
          AND status NOT IN ('skipped', 'disabled')
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    bool flagged = false;
    if (res && PQresultStatus(res) == PGRES_COMMAND_OK) {
        flagged = std::atoi(PQcmdTuples(res)) > 0;
    }
    if (res) {
        PQclear(res);
    }
    return flagged;
}
