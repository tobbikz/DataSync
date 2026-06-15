#include "mariadb_ddl_sync.hpp"

#include "lake_columns.hpp"
#include "mariadb_schema.hpp"

#include <functional>
#include <map>
#include <optional>
#include <sstream>

namespace {

std::string mysql_escape_literal(MYSQL* mysql, const std::string& value) {
    std::string out(value.size() * 2 + 3, '\'');
    const unsigned long len = mysql_real_escape_string(mysql, out.data() + 1, value.c_str(), static_cast<unsigned long>(value.size()));
    out.resize(len + 2);
    out[0] = '\'';
    out[len + 1] = '\'';
    return out;
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
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols) {
    int added = 0;
    for (const auto& col : cols) {
        if (pg_column_exists(pg, schema, table, col.name)) {
            continue;
        }
        pg_exec(
            pg,
            "ALTER TABLE " + pg_ident(schema) + "." + pg_ident(table) + " ADD COLUMN " + pg_ident(col.name) + " " +
            col.pg_type + " NULL");
        added += 1;
    }
    return added;
}

std::string pg_column_data_type(PGconn* pg, const std::string& schema, const std::string& table, const std::string& column) {
    const char* vals[] = {schema.c_str(), table.c_str(), column.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 LIMIT 1",
        3,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::string out;
    if (res && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0) {
        out = PQgetvalue(res, 0, 0);
    }
    if (res) {
        PQclear(res);
    }
    return out;
}

bool pg_type_is_text_like(const std::string& data_type) {
    return data_type == "text" || data_type == "character varying" || data_type == "character";
}

bool catalog_pg_type_is_text_like(const std::string& pg_type) {
    return pg_type == "TEXT" || pg_type == "JSONB";
}

int sync_binary_column_types(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols) {
    int changed = 0;
    for (const auto& col : cols) {
        if (!pg_column_exists(pg, schema, table, col.name)) {
            continue;
        }
        const std::string current = pg_column_data_type(pg, schema, table, col.name);
        const std::string fq = pg_ident(schema) + "." + pg_ident(table);
        const std::string ident = pg_ident(col.name);
        if (col.pg_type == "BYTEA") {
            if (current == "bytea") {
                continue;
            }
            if (!pg_type_is_text_like(current)) {
                continue;
            }
            pg_exec(
                pg,
                "ALTER TABLE " + fq + " ALTER COLUMN " + ident + " TYPE bytea USING (CASE WHEN " + ident +
                " IS NULL THEN NULL::bytea ELSE decode(encode(convert_to(" + ident +
                ", 'SQL_ASCII'), 'hex'), 'hex') END)");
            changed += 1;
            continue;
        }
        if (current != "bytea") {
            continue;
        }
        if (!catalog_pg_type_is_text_like(col.pg_type)) {
            continue;
        }
        pg_exec(
            pg,
            "ALTER TABLE " + fq + " ALTER COLUMN " + ident + " TYPE text USING encode(" + ident + ", 'escape')");
        changed += 1;
    }
    return changed;
}

struct IndexDef {
    std::string name;
    bool unique{false};
    std::vector<std::string> columns;
};

std::vector<IndexDef> fetch_mariadb_indexes(MYSQL* mysql, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT index_name, non_unique, column_name, seq_in_index FROM information_schema.statistics "
        "WHERE table_schema=" +
        mysql_escape_literal(mysql, schema) + " AND table_name=" + mysql_escape_literal(mysql, table) +
        " ORDER BY index_name, seq_in_index";

    if (mysql_query(mysql, sql.c_str()) != 0) {
        throw std::runtime_error(std::string("index scan failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return {};
    }

    std::map<std::string, IndexDef> indexes;
    MYSQL_ROW row;
    try {
        while ((row = mysql_fetch_row(res)) != nullptr) {
            if (!row[0] || !row[2]) {
                continue;
            }
            const std::string name = row[0];
            if (name == "PRIMARY") {
                continue;
            }
            auto& idx = indexes[name];
            idx.name = name;
            idx.unique = row[1] && std::string(row[1]) == "0";
            idx.columns.push_back(row[2]);
        }
    } catch (...) {
        mysql_free_result(res);
        throw;
    }
    mysql_free_result(res);

    std::vector<IndexDef> out;
    for (auto& [_, idx] : indexes) {
        out.push_back(std::move(idx));
    }
    return out;
}

int sync_indexes(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<IndexDef>& indexes) {
    const bool partitioned = pg_table_is_partitioned(pg, schema, table);
    int created = 0;
    for (const auto& idx : indexes) {
        const std::string pg_index_name = ("dl_" + schema + "_" + table + "_" + idx.name).substr(0, 63);
        if (pg_index_exists(pg, schema, pg_index_name)) {
            continue;
        }
        std::ostringstream cols;
        for (std::size_t i = 0; i < idx.columns.size(); ++i) {
            if (i) {
                cols << ", ";
            }
            cols << pg_ident(idx.columns[i]);
        }
        // PG partitioned tables require UNIQUE indexes to include the partition key;
        // lake is PARTITION BY RANGE (_dl_load_timestamp) — mirror on business keys uses non-unique indexes.
        const bool use_unique = idx.unique && !partitioned;
        const std::string unique = use_unique ? "UNIQUE " : "";
        const std::string sql = "CREATE " + unique + "INDEX IF NOT EXISTS " + pg_ident(pg_index_name) + " ON " +
                                pg_ident(schema) + "." + pg_ident(table) + " (" + cols.str() + ")";
        if (pg_try_exec(pg, sql).has_value()) {
            continue;
        }
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

std::vector<ForeignKeyDef> fetch_mariadb_foreign_keys_once(MYSQL* mysql, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT constraint_name, column_name, referenced_table_schema, referenced_table_name, "
        "referenced_column_name, ordinal_position "
        "FROM information_schema.key_column_usage "
        "WHERE table_schema=" +
        mysql_escape_literal(mysql, schema) + " AND table_name=" + mysql_escape_literal(mysql, table) +
        " AND referenced_table_name IS NOT NULL "
        "ORDER BY constraint_name, ordinal_position";

    if (mysql_query(mysql, sql.c_str()) != 0) {
        throw std::runtime_error(std::string("fk scan failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return {};
    }

    std::map<std::string, ForeignKeyDef> fks;
    MYSQL_ROW row;
    try {
        while ((row = mysql_fetch_row(res)) != nullptr) {
            if (!row[0] || !row[1] || !row[3] || !row[4]) {
                continue;
            }
            auto& fk = fks[row[0]];
            fk.constraint_name = row[0];
            fk.columns.push_back(row[1]);
            fk.ref_schema = row[2] ? row[2] : schema;
            fk.ref_table = row[3];
            fk.ref_columns.push_back(row[4]);
        }
    } catch (...) {
        mysql_free_result(res);
        throw;
    }
    mysql_free_result(res);

    std::vector<ForeignKeyDef> out;
    for (auto& [_, fk] : fks) {
        out.push_back(std::move(fk));
    }
    return out;
}

int sync_foreign_keys(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<ForeignKeyDef>& fks) {
    int created = 0;
    for (const auto& fk : fks) {
        const std::string pg_fk_name = ("dl_fk_" + schema + "_" + table + "_" + fk.constraint_name).substr(0, 63);
        if (pg_constraint_exists(pg, schema, pg_fk_name)) {
            continue;
        }
        if (!pg_table_exists(pg, fk.ref_schema, fk.ref_table)) {
            continue;
        }
        if (!ensure_referenced_unique_index(pg, fk.ref_schema, fk.ref_table, fk.ref_columns)) {
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
            "ALTER TABLE " + pg_ident(schema) + "." + pg_ident(table) + " ADD CONSTRAINT " + pg_ident(pg_fk_name) +
            " FOREIGN KEY (" + local_cols.str() + ") REFERENCES " + pg_ident(fk.ref_schema) + "." +
            pg_ident(fk.ref_table) + " (" + ref_cols.str() + ")";
        if (pg_try_exec(pg, alter_sql).has_value()) {
            continue;
        }
        created += 1;
    }
    return created;
}

}  // namespace

DdlSyncResult sync_mariadb_ddl_after_truncate(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    const RuntimeConfig& cfg,
    const std::string& conn_id) {
    DdlSyncResult result;
    result.columns_added = sync_missing_columns(pg, schema, table, cols);
    result.columns_widened = sync_binary_column_types(pg, schema, table, cols);

    if (cfg.get_bool("ddl_sync_indexes", true, "mariadb_load", conn_id)) {
        result.indexes_created = sync_indexes(pg, schema, table, fetch_mariadb_indexes(mysql, schema, table));
    }
    if (cfg.get_bool("ddl_sync_foreign_keys", true, "mariadb_load", conn_id)) {
        result.foreign_keys_created =
            sync_foreign_keys(pg, schema, table, fetch_mariadb_foreign_keys_once(mysql, schema, table));
    }
    return result;
}

DdlSyncResult sync_mariadb_columns_to_lake(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const RuntimeConfig& /*cfg*/,
    const std::string& /*conn_id*/) {
    const auto cols = fetch_mariadb_columns(mysql, schema, table);
    if (!pg_lake_table_exists(pg, schema, table)) {
        ensure_lake_table_base(pg, schema, table, cols);
        return {};
    }
    DdlSyncResult result;
    result.columns_added = sync_missing_columns(pg, schema, table, cols);
    result.columns_widened = sync_binary_column_types(pg, schema, table, cols);
    return result;
}
