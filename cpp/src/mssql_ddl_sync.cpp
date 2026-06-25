#include "mssql_ddl_sync.hpp"

#include "capture_common.hpp"
#include "lake_columns.hpp"
#include "mariadb_schema.hpp"
#include "mssql_conn.hpp"
#include "mssql_lake.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"

#include <iostream>
#include <functional>
#include <map>
#include <optional>
#include <sstream>

#ifdef HAVE_FREETDS

namespace {

struct MssqlTableTarget {
    std::string source_database;
    std::string source_schema;
    std::string source_table;
};

std::string object_id_expr(const std::string& schema, const std::string& table) {
    auto esc = [](const std::string& id) {
        std::string out = "'";
        for (char c : id) {
            out += (c == '\'') ? "''" : std::string(1, c);
        }
        out += "'";
        return out;
    };
    return esc(schema + "." + table);
}

bool pg_column_exists(PGconn* pg, const std::string& schema, const std::string& table, const std::string& column) {
    const char* vals[] = {schema.c_str(), table.c_str(), column.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 LIMIT 1",
        3,
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

bool pg_index_exists(PGconn* pg, const std::string& schema, const std::string& index_name) {
    const char* vals[] = {schema.c_str(), index_name.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2 LIMIT 1",
        2,
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

bool pg_table_exists(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = $1 AND table_name = $2 LIMIT 1",
        2,
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

bool pg_constraint_exists(PGconn* pg, const std::string& schema, const std::string& constraint_name) {
    const char* vals[] = {schema.c_str(), constraint_name.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE constraint_schema = $1 AND constraint_name = $2 LIMIT 1",
        2,
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

bool pg_table_is_partitioned(PGconn* pg, const std::string& schema, const std::string& table) {
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT c.relkind = 'p'
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        LIMIT 1
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    const char* val = res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0 ? PQgetvalue(res, 0, 0) : nullptr;
    const bool ok = val && val[0] == 't';
    if (res) {
        PQclear(res);
    }
    return ok;
}

std::optional<std::string> pg_try_exec(PGconn* pg, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res) {
        return std::string("PQexec returned null");
    }
    const auto st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        std::string err = PQerrorMessage(pg);
        if (const char* msg = PQresultErrorMessage(res)) {
            if (msg[0]) {
                err += " | ";
                err += msg;
            }
        }
        PQclear(res);
        return err;
    }
    PQclear(res);
    return std::nullopt;
}

std::string mirror_unique_index_name(
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& columns) {
    std::string name = "dl_uq_" + schema + "_" + table;
    for (const auto& col : columns) {
        name += "_" + col;
    }
    if (name.size() > 63) {
        const auto hash = std::hash<std::string>{}(name);
        std::ostringstream suffix;
        suffix << std::hex << (hash & 0xFFFFFFFF);
        const std::string suffix_str = suffix.str();
        name.resize(63 - suffix_str.size() - 1);
        name += "_" + suffix_str;
    }
    return name;
}

bool dedupe_lake_rows_on_columns(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& key_columns) {
    if (key_columns.empty()) {
        return false;
    }
    const std::string fq = pg_ident(schema) + "." + pg_ident(table);
    std::ostringstream partition_by;
    for (std::size_t i = 0; i < key_columns.size(); ++i) {
        if (i) {
            partition_by << ", ";
        }
        partition_by << pg_ident(key_columns[i]);
    }
    const std::string sql =
        "DELETE FROM " + fq + " t USING ("
        "SELECT ctid FROM ("
        "SELECT ctid, ROW_NUMBER() OVER (PARTITION BY " + partition_by.str() + " ORDER BY " +
        pg_ident(lake_columns::kLoadTimestamp) + " DESC NULLS LAST) AS rn "
        "FROM " +
        fq + ") x WHERE x.rn > 1) d WHERE t.ctid = d.ctid";
    return !pg_try_exec(pg, sql).has_value();
}

// Lake PK is (source_pk, _dl_load_timestamp); FKs reference source PK columns only — ensure UNIQUE first.
bool ensure_referenced_unique_index(
    PGconn* pg,
    const std::string& ref_schema,
    const std::string& ref_table,
    const std::vector<std::string>& ref_columns) {
    // Partitioned lake tables (BY _dl_load_timestamp) cannot host UNIQUE on business keys alone.
    if (pg_table_is_partitioned(pg, ref_schema, ref_table)) {
        return false;
    }
    for (const auto& col : ref_columns) {
        if (!pg_column_exists(pg, ref_schema, ref_table, col)) {
            return false;
        }
    }
    const std::string idx_name = mirror_unique_index_name(ref_schema, ref_table, ref_columns);
    if (pg_index_exists(pg, ref_schema, idx_name)) {
        return true;
    }
    std::ostringstream col_list;
    for (std::size_t i = 0; i < ref_columns.size(); ++i) {
        if (i) {
            col_list << ", ";
        }
        col_list << pg_ident(ref_columns[i]);
    }
    const std::string fq = pg_ident(ref_schema) + "." + pg_ident(ref_table);
    const std::string create_sql = "CREATE UNIQUE INDEX IF NOT EXISTS " + pg_ident(idx_name) + " ON " + fq +
                                   " (" + col_list.str() + ")";
    if (!pg_try_exec(pg, create_sql).has_value()) {
        return true;
    }
    if (!dedupe_lake_rows_on_columns(pg, ref_schema, ref_table, ref_columns)) {
        return false;
    }
    return !pg_try_exec(pg, create_sql).has_value();
}

int sync_missing_columns(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<MssqlColumn>& cols) {
    int added = 0;
    for (const auto& col : cols) {
        if (pg_column_exists(pg, pg_schema, pg_table, col.name)) {
            continue;
        }
        pg_exec(
            pg,
            "ALTER TABLE " + pg_ident(pg_schema) + "." + pg_ident(pg_table) + " ADD COLUMN " + pg_ident(col.name) +
            " " + col.pg_type + " NULL");
        added += 1;
    }
    return added;
}

std::string normalize_mssql_pg_data_type(
    const std::string& data_type,
    const std::string& udt_name,
    int char_max_len) {
    if (udt_name == "varchar" || data_type == "character varying") {
        if (char_max_len > 0) {
            return "VARCHAR(" + std::to_string(char_max_len) + ")";
        }
        return "TEXT";
    }
    if (udt_name == "text") {
        return "TEXT";
    }
    if (udt_name == "int4" || data_type == "integer") {
        return "INTEGER";
    }
    if (udt_name == "int8" || data_type == "bigint") {
        return "BIGINT";
    }
    if (udt_name == "bool" || data_type == "boolean") {
        return "BOOLEAN";
    }
    if (udt_name == "numeric") {
        return "NUMERIC";
    }
    if (udt_name == "float8" || data_type == "double precision") {
        return "DOUBLE PRECISION";
    }
    if (udt_name == "float4" || data_type == "real") {
        return "REAL";
    }
    if (udt_name == "timestamp" || data_type == "timestamp without time zone") {
        return "TIMESTAMP";
    }
    if (udt_name == "date") {
        return "DATE";
    }
    if (udt_name == "uuid") {
        return "UUID";
    }
    return "TEXT";
}

int mssql_pg_type_width(const std::string& pg_type) {
    if (pg_type == "TEXT") {
        return 1'000'000;
    }
    if (pg_type.rfind("VARCHAR(", 0) == 0 && pg_type.size() > 8 && pg_type.back() == ')') {
        try {
            return std::stoi(pg_type.substr(8, pg_type.size() - 9));
        } catch (...) {
            return 0;
        }
    }
    if (pg_type == "INTEGER") {
        return 10;
    }
    if (pg_type == "BIGINT") {
        return 20;
    }
    if (pg_type == "BOOLEAN") {
        return 1;
    }
    if (pg_type == "DOUBLE PRECISION" || pg_type == "REAL" || pg_type == "NUMERIC") {
        return 100;
    }
    return 50;
}

std::string mssql_alter_column_using(const std::string& col, const std::string& to_type) {
    const std::string qcol = pg_ident(col);
    if (to_type.rfind("VARCHAR(", 0) == 0 || to_type == "TEXT") {
        return qcol + "::text";
    }
    return qcol + "::" + to_type;
}

int sync_mssql_column_types(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<MssqlColumn>& cols) {
    if (!pg_table_exists(pg, pg_schema, pg_table)) {
        return 0;
    }
    const char* vals[] = {pg_schema.c_str(), pg_table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT column_name, data_type, udt_name, character_maximum_length "
        "FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "AND column_name NOT IN ('_dl_load_timestamp','_dl_load_date','_dl_source_system','_dl_snapshot_id')",
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
        return 0;
    }

    std::map<std::string, std::string> existing_types;
    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string col = PQgetvalue(res, i, 0);
        const std::string data_type = PQgetvalue(res, i, 1);
        const std::string udt_name = PQgetvalue(res, i, 2);
        const int char_max = PQgetisnull(res, i, 3) ? -1 : std::atoi(PQgetvalue(res, i, 3));
        existing_types[col] = normalize_mssql_pg_data_type(data_type, udt_name, char_max);
    }
    PQclear(res);

    int widened = 0;
    const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
    for (const auto& col : cols) {
        const auto it = existing_types.find(col.name);
        if (it == existing_types.end()) {
            continue;
        }
        const int existing_w = mssql_pg_type_width(it->second);
        const int source_w = mssql_pg_type_width(col.pg_type);
        if (source_w <= existing_w) {
            continue;
        }
        const std::string using_expr = mssql_alter_column_using(col.name, col.pg_type);
        pg_exec(
            pg,
            "ALTER TABLE " + fq + " ALTER COLUMN " + pg_ident(col.name) + " TYPE " + col.pg_type + " USING " +
            using_expr);
        widened += 1;
    }
    return widened;
}

struct IndexDef {
    std::string name;
    bool unique{false};
    std::vector<std::string> columns;
};

std::vector<IndexDef> fetch_mssql_indexes(MssqlConn& mssql, const std::string& schema, const std::string& table) {
    std::ostringstream sql;
    sql << "SELECT i.name, i.is_unique, c.name, ic.key_ordinal "
           "FROM sys.indexes i "
           "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
           "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
           "WHERE i.object_id = OBJECT_ID("
        << object_id_expr(schema, table)
        << ") AND i.is_primary_key = 0 AND i.type > 0 "
           "ORDER BY i.name, ic.key_ordinal";

    const MssqlQueryResult result = mssql.query(sql.str());
    std::map<std::string, IndexDef> indexes;
    for (const auto& row : result.rows) {
        if (row.size() < 4 || row[0].text.empty() || row[2].text.empty()) {
            continue;
        }
        auto& idx = indexes[row[0].text];
        idx.name = row[0].text;
        idx.unique = row[1].text == "1";
        idx.columns.push_back(row[2].text);
    }
    std::vector<IndexDef> out;
    for (auto& [_, idx] : indexes) {
        out.push_back(std::move(idx));
    }
    return out;
}

int sync_indexes(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<IndexDef>& indexes) {
    int created = 0;
    for (const auto& idx : indexes) {
        const std::string pg_index_name =
            ("dl_" + pg_schema + "_" + pg_table + "_" + idx.name).substr(0, 63);
        if (pg_index_exists(pg, pg_schema, pg_index_name)) {
            continue;
        }
        std::ostringstream cols;
        for (std::size_t i = 0; i < idx.columns.size(); ++i) {
            if (i) {
                cols << ", ";
            }
            cols << pg_ident(idx.columns[i]);
        }
        // Lake tables are PARTITION BY RANGE (_dl_load_timestamp); PG rejects UNIQUE on
        // business keys alone. Mirror source indexes as non-unique (same as mariadb_ddl_sync).
        (void)idx.unique;
        pg_exec(
            pg,
            "CREATE INDEX IF NOT EXISTS " + pg_ident(pg_index_name) + " ON " + pg_ident(pg_schema) + "." +
            pg_ident(pg_table) + " (" + cols.str() + ")");
        created += 1;
    }
    return created;
}

struct ForeignKeyDef {
    std::string constraint_name;
    std::vector<std::string> columns;
    std::string ref_schema;
    std::string ref_table;
    std::vector<std::string> ref_columns;
};

std::vector<ForeignKeyDef> fetch_mssql_foreign_keys(
    MssqlConn& mssql,
    const std::string& schema,
    const std::string& table) {
    std::ostringstream sql;
    sql << "SELECT fk.name, "
           "COL_NAME(fkc.parent_object_id, fkc.parent_column_id), "
           "OBJECT_SCHEMA_NAME(fk.referenced_object_id), "
           "OBJECT_NAME(fk.referenced_object_id), "
           "COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id), "
           "fkc.constraint_column_id "
           "FROM sys.foreign_keys fk "
           "JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id "
           "WHERE fk.parent_object_id = OBJECT_ID("
        << object_id_expr(schema, table) << ") "
           "ORDER BY fk.name, fkc.constraint_column_id";

    const MssqlQueryResult result = mssql.query(sql.str());
    std::map<std::string, ForeignKeyDef> fks;
    for (const auto& row : result.rows) {
        if (row.size() < 5 || row[0].text.empty() || row[1].text.empty() || row[3].text.empty() ||
            row[4].text.empty()) {
            continue;
        }
        auto& fk = fks[row[0].text];
        fk.constraint_name = row[0].text;
        fk.columns.push_back(row[1].text);
        fk.ref_schema = row[2].text.empty() ? schema : row[2].text;
        fk.ref_table = row[3].text;
        fk.ref_columns.push_back(row[4].text);
    }
    std::vector<ForeignKeyDef> out;
    for (auto& [_, fk] : fks) {
        out.push_back(std::move(fk));
    }
    return out;
}

int sync_foreign_keys(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::string& source_database,
    const std::vector<ForeignKeyDef>& fks) {
    int created = 0;
    for (const auto& fk : fks) {
        const std::string pg_fk_name =
            ("dl_fk_" + pg_schema + "_" + pg_table + "_" + fk.constraint_name).substr(0, 63);
        if (pg_constraint_exists(pg, pg_schema, pg_fk_name)) {
            continue;
        }
        const std::string ref_pg_schema = mssql_pg_schema_name(source_database, fk.ref_schema);
        const std::string ref_pg_table = mssql_pg_table_name(fk.ref_table);
        if (!pg_table_exists(pg, ref_pg_schema, ref_pg_table)) {
            continue;
        }
        if (!ensure_referenced_unique_index(pg, ref_pg_schema, ref_pg_table, fk.ref_columns)) {
            continue;
        }

        std::ostringstream local_cols;
        std::ostringstream ref_cols;
        for (std::size_t i = 0; i < fk.columns.size(); ++i) {
            if (i) {
                local_cols << ", ";
                ref_cols << ", ";
            }
            local_cols << pg_ident(fk.columns[i]);
            ref_cols << pg_ident(fk.ref_columns[i]);
        }

        const std::string alter_sql =
            "ALTER TABLE " + pg_ident(pg_schema) + "." + pg_ident(pg_table) + " ADD CONSTRAINT " +
            pg_ident(pg_fk_name) + " FOREIGN KEY (" + local_cols.str() + ") REFERENCES " + pg_ident(ref_pg_schema) +
            "." + pg_ident(ref_pg_table) + " (" + ref_cols.str() + ")";
        if (pg_try_exec(pg, alter_sql).has_value()) {
            continue;
        }
        created += 1;
    }
    return created;
}

std::vector<MssqlTableTarget> fetch_ddl_targets(
    PGconn* pg,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    std::string sql = R"(
        SELECT source_database, source_schema, source_table
        FROM cdc_catalog.catalog
        WHERE conn_id = $1
          AND db_engine = 'mssql'
          AND active = true
          AND cdc_enabled = true
    )";
    std::vector<std::string> vals = {conn_id};
    if (source_schema && !source_schema->empty()) {
        sql += " AND source_schema = $" + std::to_string(vals.size() + 1);
        vals.push_back(*source_schema);
    }
    if (source_table && !source_table->empty()) {
        sql += " AND source_table = $" + std::to_string(vals.size() + 1);
        vals.push_back(*source_table);
    }
    sql += " ORDER BY source_database, source_schema, source_table";

    std::vector<const char*> ptrs;
    ptrs.reserve(vals.size());
    for (const auto& v : vals) {
        ptrs.push_back(v.c_str());
    }

    PGresult* res = PQexecParams(
        pg,
        sql.c_str(),
        static_cast<int>(ptrs.size()),
        nullptr,
        ptrs.data(),
        nullptr,
        nullptr,
        0);

    std::vector<MssqlTableTarget> out;
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        if (res) {
            PQclear(res);
        }
        return out;
    }
    for (int i = 0; i < PQntuples(res); ++i) {
        out.push_back({PQgetvalue(res, i, 0), PQgetvalue(res, i, 1), PQgetvalue(res, i, 2)});
    }
    PQclear(res);
    return out;
}

}  // namespace

DdlSyncResult sync_mssql_ddl_after_truncate(
    PGconn* pg,
    MssqlConn& mssql,
    const std::string& source_database,
    const std::string& source_schema,
    const std::string& source_table,
    const std::vector<MssqlColumn>& cols,
    const RuntimeConfig& cfg,
    const std::string& conn_id) {
    DdlSyncResult result;
    const std::string pg_schema = mssql_pg_schema_name(source_database, source_schema);
    const std::string pg_table = mssql_pg_table_name(source_table);

    result.columns_added = 0;
    result.columns_widened = 0;
    if (cfg.get_bool("ddl_sync_columns", true, "mssql_load", conn_id)) {
        result.columns_added = sync_missing_columns(pg, pg_schema, pg_table, cols);
        result.columns_widened = sync_mssql_column_types(pg, pg_schema, pg_table, cols);
    }

    if (cfg.get_bool("ddl_sync_indexes", true, "mssql_load", conn_id)) {
        result.indexes_created =
            sync_indexes(pg, pg_schema, pg_table, fetch_mssql_indexes(mssql, source_schema, source_table));
    }
    if (cfg.get_bool("ddl_sync_foreign_keys", true, "mssql_load", conn_id)) {
        result.foreign_keys_created = sync_foreign_keys(
            pg,
            pg_schema,
            pg_table,
            source_database,
            fetch_mssql_foreign_keys(mssql, source_schema, source_table));
    }
    return result;
}

DdlSyncResult sync_mssql_columns_to_lake(
    PGconn* pg,
    MssqlConn& mssql,
    const std::string& source_database,
    const std::string& source_schema,
    const std::string& source_table,
    const RuntimeConfig& cfg,
    const std::string& conn_id) {
    mssql.use_database(source_database);
    const auto cols = fetch_mssql_columns(mssql.handle, source_schema, source_table);
    const std::string pg_schema = mssql_pg_schema_name(source_database, source_schema);
    const std::string pg_table = mssql_pg_table_name(source_table);
    if (!pg_lake_table_exists(pg, pg_schema, pg_table)) {
        std::vector<std::string> pk_cols;
        for (const auto& col : cols) {
            if (col.is_pk) {
                pk_cols.push_back(col.name);
            }
        }
        ensure_mssql_lake_table_base(
            pg,
            pg_schema,
            pg_table,
            cols,
            pk_cols,
            cfg.get_int("lake_partition_months_ahead", 3, "mssql_load", conn_id));
        return {};
    }
    DdlSyncResult result;
    if (cfg.get_bool("ddl_sync_columns", true, "mssql_load", conn_id)) {
        result.columns_added = sync_missing_columns(pg, pg_schema, pg_table, cols);
        result.columns_widened = sync_mssql_column_types(pg, pg_schema, pg_table, cols);
    }
    return result;
}

DdlSyncRunStats run_mssql_ddl_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    DdlSyncRunStats stats;
    RuntimeConfig runtime;
    runtime.reload(log_pg);

    log_write(log_pg, {
        .level = LogLevel::Info,
        .component = "mssql_ddl_sync",
        .message = "ddl sync started",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
    });

    const MssqlSource* source = find_mssql_source(cfg, conn_id);
    if (!source) {
        throw std::runtime_error("unknown conn_id: " + conn_id);
    }

    PgConn app_pg(cfg.datasync.conn_string());
    PgConn lake_pg(cfg.datalake.conn_string());
    const auto targets = fetch_ddl_targets(app_pg.raw, conn_id, source_schema, source_table);
    if (targets.empty()) {
        log_write(log_pg, {
            .level = LogLevel::Info,
            .component = "mssql_ddl_sync",
            .message = "ddl sync skipped: no tables",
            .batch_id = batch_id,
            .conn_id = conn_id,
            .source_schema = std::nullopt,
            .source_table = std::nullopt,
        });
        return stats;
    }

    MssqlConn mssql(*source);
    runtime.reload(app_pg.raw);

    for (const auto& target : targets) {
        stats.tables_processed += 1;
        try {
            runtime.reload(app_pg.raw);
            const auto result = sync_mssql_columns_to_lake(
                lake_pg.raw,
                mssql,
                target.source_database,
                target.source_schema,
                target.source_table,
                runtime,
                conn_id);
            stats.tables_success += 1;
            stats.columns_added += result.columns_added;
            if (result.columns_added > 0 || result.indexes_created > 0 || result.foreign_keys_created > 0) {
                log_write(log_pg, {
                    .level = LogLevel::Info,
                    .component = "mssql_ddl_sync",
                    .message = "ddl sync table updated",
                    .batch_id = batch_id,
                    .conn_id = conn_id,
                    .source_schema = target.source_schema,
                    .source_table = target.source_table,
                    .context = {
                        {"columns_added", result.columns_added},
                        {"indexes_created", result.indexes_created},
                        {"foreign_keys_created", result.foreign_keys_created},
                    },
                });
            }
        } catch (const std::exception& ex) {
            stats.tables_failed += 1;
            log_write(log_pg, {
                .level = LogLevel::Error,
                .component = "mssql_ddl_sync",
                .message = "ddl sync table failed",
                .batch_id = batch_id,
                .conn_id = conn_id,
                .source_schema = target.source_schema,
                .source_table = target.source_table,
                .context = {{"error", ex.what()}},
            });
        }
    }

    const auto level = stats.tables_failed == 0 ? LogLevel::Info : LogLevel::Warning;
    log_write(log_pg, {
        .level = level,
        .component = "mssql_ddl_sync",
        .message = stats.tables_failed == 0 ? "ddl sync completed" : "ddl sync completed with errors",
        .batch_id = batch_id,
        .conn_id = conn_id,
        .source_schema = std::nullopt,
        .source_table = std::nullopt,
        .context = {
            {"tables_processed", stats.tables_processed},
            {"tables_success", stats.tables_success},
            {"tables_failed", stats.tables_failed},
            {"columns_added", stats.columns_added},
        },
    });

    return stats;
}

int run_mssql_ddl_sync_cli(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& conn_id,
    const std::optional<std::string>& source_schema,
    const std::optional<std::string>& source_table) {
    const std::string batch_id = make_batch_id();
    const auto stats =
        run_mssql_ddl_sync(cfg, log_pg, batch_id, conn_id, source_schema, source_table);
    std::cout << "{\"batch_id\":\"" << batch_id << "\",\"tables_processed\":" << stats.tables_processed
              << ",\"tables_success\":" << stats.tables_success << ",\"tables_failed\":" << stats.tables_failed
              << ",\"columns_added\":" << stats.columns_added << "}" << std::endl;
    return stats.tables_failed == 0 ? 0 : 1;
}

#endif
