#include "lake_history.hpp"

#include "lake_columns.hpp"
#include "mariadb_schema.hpp"
#include "pg_conn.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace lake_history {
namespace {

std::string ident_list(const std::vector<std::string>& cols) {
    std::ostringstream out;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i) {
            out << ", ";
        }
        out << pg_ident(cols[i]);
    }
    return out.str();
}

std::string join_predicate_aliased(
    const std::string& left,
    const std::string& right,
    const std::vector<std::string>& cols) {
    std::ostringstream out;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i) {
            out << " AND ";
        }
        out << left << '.' << pg_ident(cols[i]) << " = " << right << '.' << pg_ident(cols[i]);
    }
    return out.str();
}

/** `h."a" = s."a" AND h."b" = s."b"` */
std::string join_predicate(const std::vector<std::string>& cols) {
    return join_predicate_aliased("h", "s", cols);
}

/** `m."a", m."b"` — every column qualified, since these lists are selected across a join. */
std::string prefixed_ident_list(const std::string& alias, const std::vector<std::string>& cols) {
    std::ostringstream out;
    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (i) {
            out << ", ";
        }
        out << alias << '.' << pg_ident(cols[i]);
    }
    return out.str();
}

/** The columns that carry source data; `_dl_*` is bookkeeping and never signals a change. */
std::vector<std::string> data_columns(const std::vector<std::string>& all_cols) {
    std::vector<std::string> out;
    out.reserve(all_cols.size());
    for (const auto& col : all_cols) {
        if (col.rfind("_dl_", 0) != 0) {
            out.push_back(col);
        }
    }
    return out;
}

long long rows_affected(PGconn* pg, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    const ExecStatusType status = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        const std::string err = res ? PQresultErrorMessage(res) : PQerrorMessage(pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("lake_history: statement failed: " + err);
    }
    const char* tuples = PQcmdTuples(res);
    const long long affected = (tuples && *tuples) ? std::atoll(tuples) : 0;
    PQclear(res);
    return affected;
}

bool relation_exists(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = $1 AND c.relname = $2 LIMIT 1",
        2, nullptr, vals, nullptr, nullptr, 0);
    const bool found = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0;
    if (res) {
        PQclear(res);
    }
    return found;
}

std::vector<std::string> relation_columns(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT a.attname FROM pg_attribute a"
        " JOIN pg_class c ON c.oid = a.attrelid"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped"
        " ORDER BY a.attnum",
        2, nullptr, vals, nullptr, nullptr, 0);
    std::vector<std::string> out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        const int rows = PQntuples(res);
        out.reserve(static_cast<std::size_t>(rows));
        for (int i = 0; i < rows; ++i) {
            out.emplace_back(PQgetvalue(res, i, 0));
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

/**
 * Mirror columns the history table lacks, with the type spelled exactly as the mirror
 * declares it: information_schema.data_type would flatten varchar(32) to "character varying"
 * and quietly widen every column it copies.
 */
std::vector<std::pair<std::string, std::string>> missing_columns(
    PGconn* pg,
    const std::string& schema,
    const std::string& mirror,
    const std::string& hist) {
    const char* vals[] = {schema.c_str(), mirror.c_str(), hist.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "WITH cols AS ("
        "  SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype, a.attnum"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        "  WHERE n.nspname = $1 AND c.relname IN ($2, $3) AND a.attnum > 0 AND NOT a.attisdropped)"
        " SELECT m.attname, m.coltype FROM cols m"
        " WHERE m.relname = $2"
        "   AND NOT EXISTS (SELECT 1 FROM cols h WHERE h.relname = $3 AND h.attname = m.attname)"
        " ORDER BY m.attnum",
        3, nullptr, vals, nullptr, nullptr, 0);
    std::vector<std::pair<std::string, std::string>> out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK) {
        const int rows = PQntuples(res);
        out.reserve(static_cast<std::size_t>(rows));
        for (int i = 0; i < rows; ++i) {
            out.emplace_back(PQgetvalue(res, i, 0), PQgetvalue(res, i, 1));
        }
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

void copy_lines(PGconn* pg, const std::string& copy_sql, const std::vector<std::string>& lines) {
    PGresult* res = PQexec(pg, copy_sql.c_str());
    if (!res || PQresultStatus(res) != PGRES_COPY_IN) {
        const std::string err = PQerrorMessage(pg);
        if (res) {
            PQclear(res);
        }
        throw std::runtime_error("lake_history: COPY start failed: " + err);
    }
    PQclear(res);
    for (const auto& line : lines) {
        if (PQputCopyData(pg, line.data(), static_cast<int>(line.size())) != 1 ||
            PQputCopyData(pg, "\n", 1) != 1) {
            PQputCopyEnd(pg, "abort");
            while (PGresult* r = PQgetResult(pg)) {
                PQclear(r);
            }
            throw std::runtime_error(std::string("lake_history: PQputCopyData failed: ") + PQerrorMessage(pg));
        }
    }
    if (PQputCopyEnd(pg, nullptr) != 1) {
        while (PGresult* r = PQgetResult(pg)) {
            PQclear(r);
        }
        throw std::runtime_error(std::string("lake_history: PQputCopyEnd failed: ") + PQerrorMessage(pg));
    }
    while (PGresult* r = PQgetResult(pg)) {
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            const std::string err = PQresultErrorMessage(r);
            PQclear(r);
            while (PGresult* rest = PQgetResult(pg)) {
                PQclear(rest);
            }
            throw std::runtime_error("lake_history: COPY failed: " + err);
        }
        PQclear(r);
    }
}

/** Deterministic, <= 63 bytes, and distinct per lake table so sessions can reuse it. */
std::string temp_name(const std::string& prefix, const std::string& schema, const std::string& table) {
    std::string out = prefix;
    for (const std::string* part : {&schema, &table}) {
        for (unsigned char c : *part) {
            out.push_back(std::isalnum(c) || c == '_' ? static_cast<char>(std::tolower(c)) : '_');
        }
        out.push_back('_');
    }
    if (out.size() <= 63) {
        return out;
    }
    std::ostringstream os;
    os << prefix << std::hex << (std::hash<std::string>{}(schema + "." + table) & 0xffffffffu);
    return os.str();
}

void ensure_partitions(PGconn* pg, const std::string& schema, const std::string& table, int months_ahead) {
    const std::string months = std::to_string(months_ahead < 1 ? 1 : months_ahead);
    const char* vals[] = {schema.c_str(), table.c_str(), months.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT lake.ensure_monthly_partitions($1, $2, $3::integer)",
        3, nullptr, vals, nullptr, nullptr, 0);
    if (res && PQresultStatus(res) != PGRES_TUPLES_OK) {
        const std::string err = PQresultErrorMessage(res);
        PQclear(res);
        // Same tolerance as the mirror: a racing session or a legacy bound must not
        // quarantine the table forever.
        if (err.find("would overlap") == std::string::npos && err.find("already exists") == std::string::npos) {
            throw std::runtime_error("lake_history: ensure_monthly_partitions failed: " + err);
        }
        return;
    }
    if (res) {
        PQclear(res);
    }
}

}  // namespace

std::string history_table_name(const std::string& mirror_table) {
    const std::string suffix = kSuffix;
    if (mirror_table.size() + suffix.size() <= 63) {
        return mirror_table + suffix;
    }
    return mirror_table.substr(0, 63 - suffix.size()) + suffix;
}

void ensure_history_table(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    int partition_months_ahead) {
    if (!lake_pg || schema.empty() || mirror_table.empty()) {
        return;
    }
    if (pk_cols.empty()) {
        throw std::runtime_error(
            "lake_history: " + schema + "." + mirror_table +
            " has no primary key; SCD2 needs one to tell versions of a row apart");
    }
    const std::string hist = history_table_name(mirror_table);
    const std::string fq_mirror = pg_ident(schema) + "." + pg_ident(mirror_table);
    const std::string fq_hist = pg_ident(schema) + "." + pg_ident(hist);

    if (!relation_exists(lake_pg, schema, hist)) {
        std::vector<std::string> hist_pk = pk_cols;
        hist_pk.emplace_back(lake_columns::kLoadTimestamp);
        std::ostringstream create;
        create << "CREATE TABLE IF NOT EXISTS " << fq_hist << " (\n"
               << "  LIKE " << fq_mirror << " INCLUDING DEFAULTS,\n"
               << "  " << pg_ident(kValidTo) << " timestamptz,\n"
               << "  " << pg_ident(kIsCurrent) << " boolean NOT NULL DEFAULT true,\n"
               << "  " << pg_ident(kIsDeleted) << " boolean NOT NULL DEFAULT false,\n"
               << "  PRIMARY KEY (" << ident_list(hist_pk) << ")\n"
               << ") PARTITION BY RANGE (" << pg_ident(lake_columns::kPartitionColumn) << ")";
        pg_exec(lake_pg, create.str());
        // Every "as of" query filters on the open version, and every close targets it.
        pg_exec(
            lake_pg,
            "CREATE INDEX IF NOT EXISTS " + pg_ident(hist + "_current_idx") + " ON " + fq_hist +
                " (" + ident_list(pk_cols) + ") WHERE " + pg_ident(kIsCurrent));
    }

    // The mirror grows columns at runtime; a history table left behind would drop that field
    // from every version it stores from here on.
    for (const auto& [col, type] : missing_columns(lake_pg, schema, mirror_table, hist)) {
        pg_exec(
            lake_pg,
            "ALTER TABLE " + fq_hist + " ADD COLUMN IF NOT EXISTS " + pg_ident(col) + " " +
                (type.empty() ? "TEXT" : type));
    }

    ensure_partitions(lake_pg, schema, hist, partition_months_ahead);
}

long long record_history_from_staging(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::string& staging,
    const std::vector<std::string>& all_cols,
    const std::vector<std::string>& pk_cols) {
    if (!lake_pg || all_cols.empty() || pk_cols.empty()) {
        return 0;
    }
    const std::string fq_hist = pg_ident(schema) + "." + pg_ident(history_table_name(mirror_table));
    const std::string ts = pg_ident(lake_columns::kLoadTimestamp);
    const std::string pk_idents = ident_list(pk_cols);
    const std::string cols = ident_list(all_cols);
    const std::string data_row = "ROW(" + ident_list(data_columns(all_cols)) + ")";

    // One statement on one snapshot: the close reads the same open versions the change
    // detection did, and the insert does not see the rows the close just modified.
    //
    // A row whose data equals the version already open does not become a new version. That
    // is the SCD2 rule (version on change, not on write), and here it also does the heavy
    // lifting of making history idempotent: the load window replays every row the full load
    // already seeded, and at-least-once delivery replays batches after a retry. Recording
    // those verbatim would fill history with versions that changed nothing.
    return rows_affected(
        lake_pg,
        "WITH staged AS ("
        "  SELECT " + cols + ", lag(" + data_row + ") OVER w AS prev_row, " + data_row +
            " AS this_row"
        "  FROM " + staging + " WINDOW w AS (PARTITION BY " + pk_idents + " ORDER BY " + ts + ")"
        "), open_now AS ("
        "  SELECT " + pk_idents + ", " + data_row + " AS open_row FROM " + fq_hist +
            " WHERE " + pg_ident(kIsCurrent) +
        "), changed AS ("
        "  SELECT " + prefixed_ident_list("s", all_cols) + " FROM staged s LEFT JOIN open_now o ON " +
            join_predicate_aliased("s", "o", pk_cols) +
        "  WHERE CASE WHEN s.prev_row IS NULL"
        "              THEN o.open_row IS NULL OR o.open_row IS DISTINCT FROM s.this_row"
        "              ELSE s.prev_row IS DISTINCT FROM s.this_row END"
        "), versioned AS ("
        "  SELECT " + cols + ", lead(" + ts + ") OVER w2 AS valid_to"
        "  FROM changed WINDOW w2 AS (PARTITION BY " + pk_idents + " ORDER BY " + ts + ")"
        "), closed AS ("
        "  UPDATE " + fq_hist + " h SET " + pg_ident(kValidTo) + " = c.opens_at, " +
            pg_ident(kIsCurrent) + " = false"
        "  FROM (SELECT " + pk_idents + ", min(" + ts + ") AS opens_at FROM changed GROUP BY " +
            pk_idents + ") c"
        "  WHERE " + join_predicate_aliased("c", "h", pk_cols) + " AND h." + pg_ident(kIsCurrent) +
            " AND h." + ts + " < c.opens_at"
        "  RETURNING 1"
        ") INSERT INTO " + fq_hist + " (" + cols + ", " + pg_ident(kValidTo) + ", " +
            pg_ident(kIsCurrent) + ", " + pg_ident(kIsDeleted) + ")"
        " SELECT " + cols + ", valid_to, valid_to IS NULL, false FROM versioned"
        " ON CONFLICT DO NOTHING");
}

long long close_history_for_deletes(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    const std::vector<std::string>& pk_col_types,
    const std::vector<std::string>& pk_ts_lines) {
    if (!lake_pg || pk_cols.empty() || pk_ts_lines.empty()) {
        return 0;
    }
    const std::string fq_hist = pg_ident(schema) + "." + pg_ident(history_table_name(mirror_table));
    const std::string stg_name = temp_name("cdc_hdel_", schema, mirror_table);
    const std::string stg = pg_ident(stg_name);

    std::ostringstream create;
    create << "CREATE TEMP TABLE IF NOT EXISTS " << stg << " (";
    for (std::size_t i = 0; i < pk_cols.size(); ++i) {
        const std::string type = i < pk_col_types.size() && !pk_col_types[i].empty() ? pk_col_types[i] : "TEXT";
        create << pg_ident(pk_cols[i]) << " " << type << " NOT NULL, ";
    }
    create << pg_ident("_scd_ts") << " timestamptz NOT NULL)";
    pg_exec(lake_pg, create.str());
    pg_exec(lake_pg, "TRUNCATE " + stg);

    copy_lines(
        lake_pg,
        "COPY " + stg + " (" + ident_list(pk_cols) + ", " + pg_ident("_scd_ts") +
            ") FROM STDIN WITH (FORMAT csv)",
        pk_ts_lines);

    // A delete closes the open version without opening a new one, and says so: without the
    // flag a reader cannot tell a row that was removed from one that is merely superseded.
    return rows_affected(
        lake_pg,
        "UPDATE " + fq_hist + " h SET " + pg_ident(kValidTo) + " = s." + pg_ident("_scd_ts") + ", " +
            pg_ident(kIsCurrent) + " = false, " + pg_ident(kIsDeleted) + " = true FROM " + stg +
            " s WHERE " + join_predicate(pk_cols) + " AND h." + pg_ident(kIsCurrent) + " AND h." +
            pg_ident(lake_columns::kLoadTimestamp) + " <= s." + pg_ident("_scd_ts"));
}

long long seed_history_from_mirror(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    int partition_months_ahead) {
    if (!lake_pg || pk_cols.empty()) {
        return 0;
    }
    ensure_history_table(lake_pg, schema, mirror_table, pk_cols, partition_months_ahead);

    const std::string fq_mirror = pg_ident(schema) + "." + pg_ident(mirror_table);
    const std::string fq_hist = pg_ident(schema) + "." + pg_ident(history_table_name(mirror_table));
    const std::string ts = pg_ident(lake_columns::kLoadTimestamp);

    const auto mirror_cols = relation_columns(lake_pg, schema, mirror_table);
    if (mirror_cols.empty()) {
        return 0;
    }
    const std::string cols = ident_list(mirror_cols);
    const std::string data_row = "ROW(" + ident_list(data_columns(mirror_cols)) + ")";
    const std::string pk_idents = ident_list(pk_cols);

    // Same rule as the CDC path, and it matters most here: a reload rewrites every row with a
    // fresh timestamp, so without the comparison every reload would double the history of a
    // table nothing changed in.
    return rows_affected(
        lake_pg,
        "WITH open_now AS ("
        "  SELECT " + pk_idents + ", " + data_row + " AS open_row FROM " + fq_hist +
            " WHERE " + pg_ident(kIsCurrent) +
        "), changed AS ("
        "  SELECT " + prefixed_ident_list("m", mirror_cols) + " FROM " + fq_mirror +
            " m LEFT JOIN open_now o ON " + join_predicate_aliased("m", "o", pk_cols) +
        "  WHERE o.open_row IS NULL OR o.open_row IS DISTINCT FROM " +
            "ROW(" + prefixed_ident_list("m", data_columns(mirror_cols)) + ")"
        "), closed AS ("
        "  UPDATE " + fq_hist + " h SET " + pg_ident(kValidTo) + " = c.opens_at, " +
            pg_ident(kIsCurrent) + " = false"
        "  FROM (SELECT " + pk_idents + ", min(" + ts + ") AS opens_at FROM changed GROUP BY " +
            pk_idents + ") c"
        "  WHERE " + join_predicate_aliased("c", "h", pk_cols) + " AND h." + pg_ident(kIsCurrent) +
            " AND h." + ts + " < c.opens_at"
        "  RETURNING 1"
        ") INSERT INTO " + fq_hist + " (" + cols + ", " + pg_ident(kValidTo) + ", " +
            pg_ident(kIsCurrent) + ", " + pg_ident(kIsDeleted) + ")"
        " SELECT " + cols + ", NULL, true, false FROM changed ON CONFLICT DO NOTHING");
}

bool scd2_enabled_for_catalog(PGconn* catalog_pg, long long catalog_id) {
    if (!catalog_pg || catalog_id <= 0) {
        return false;
    }
    const std::string cid = std::to_string(catalog_id);
    const char* vals[] = {cid.c_str()};
    PGresult* res = PQexecParams(
        catalog_pg,
        "SELECT scd2_enabled FROM cdc_catalog.catalog WHERE catalog_id = $1::bigint",
        1, nullptr, vals, nullptr, nullptr, 0);
    bool enabled = false;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        const char* val = PQgetvalue(res, 0, 0);
        enabled = val && val[0] == 't';
    }
    if (res) {
        PQclear(res);
    }
    return enabled;
}

}  // namespace lake_history
