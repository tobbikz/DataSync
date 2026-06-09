#include "mariadb_ddl_sync.hpp"

#include "mariadb_schema.hpp"

#include <map>
#include <queue>
#include <set>
#include <sstream>
#include <unordered_map>

namespace {

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

int sync_binary_column_types(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols) {
    int changed = 0;
    for (const auto& col : cols) {
        if (col.pg_type != "BYTEA") {
            continue;
        }
        if (!pg_column_exists(pg, schema, table, col.name)) {
            continue;
        }
        const std::string current = pg_column_data_type(pg, schema, table, col.name);
        if (current == "bytea") {
            continue;
        }
        if (!pg_type_is_text_like(current)) {
            continue;
        }
        const std::string fq = pg_ident(schema) + "." + pg_ident(table);
        const std::string ident = pg_ident(col.name);
        pg_exec(
            pg,
            "ALTER TABLE " + fq + " ALTER COLUMN " + ident + " TYPE bytea USING (CASE WHEN " + ident +
            " IS NULL THEN NULL::bytea ELSE decode(encode(convert_to(" + ident +
            ", 'SQL_ASCII'), 'hex'), 'hex') END)");
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
        "WHERE table_schema='" +
        schema + "' AND table_name='" + table + "' ORDER BY index_name, seq_in_index";

    if (mysql_query(mysql, sql.c_str()) != 0) {
        throw std::runtime_error(std::string("index scan failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        return {};
    }

    std::map<std::string, IndexDef> indexes;
    MYSQL_ROW row;
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
        const std::string unique = idx.unique ? "UNIQUE " : "";
        pg_exec(
            pg,
            "CREATE " + unique + "INDEX IF NOT EXISTS " + pg_ident(pg_index_name) + " ON " + pg_ident(schema) + "." +
            pg_ident(table) + " (" + cols.str() + ")");
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

std::vector<ForeignKeyDef> fetch_mariadb_foreign_keys(MYSQL* mysql, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT constraint_name, column_name, referenced_table_schema, referenced_table_name, "
        "referenced_column_name, ordinal_position "
        "FROM information_schema.key_column_usage "
        "WHERE table_schema='" +
        schema + "' AND table_name='" + table + "' AND referenced_table_name IS NOT NULL "
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

        pg_exec(
            pg,
            "ALTER TABLE " + pg_ident(schema) + "." + pg_ident(table) + " ADD CONSTRAINT " + pg_ident(pg_fk_name) +
            " FOREIGN KEY (" + local_cols.str() + ") REFERENCES " + pg_ident(fk.ref_schema) + "." +
            pg_ident(fk.ref_table) + " (" + ref_cols.str() + ")");
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
            sync_foreign_keys(pg, schema, table, fetch_mariadb_foreign_keys(mysql, schema, table));
    }
    return result;
}

DdlSyncResult sync_mariadb_columns_to_lake(
    PGconn* pg,
    MYSQL* mysql,
    const std::string& schema,
    const std::string& table,
    const RuntimeConfig& cfg,
    const std::string& conn_id) {
    const auto cols = fetch_mariadb_columns(mysql, schema, table);
    if (!pg_lake_table_exists(pg, schema, table)) {
        ensure_lake_table_base(pg, schema, table, cols);
        return {};
    }
    return sync_mariadb_ddl_after_truncate(pg, mysql, schema, table, cols, cfg, conn_id);
}

std::vector<std::string> sort_tables_by_fk_order(
    MYSQL* mysql,
    const std::string& schema,
    const std::vector<std::string>& tables) {
    std::set<std::string> table_set(tables.begin(), tables.end());
    if (table_set.size() <= 1) {
        return tables;
    }

    std::unordered_map<std::string, int> in_degree;
    std::unordered_map<std::string, std::vector<std::string>> dependents;
    for (const auto& t : tables) {
        in_degree[t] = 0;
    }

    for (const auto& child : tables) {
        for (const auto& fk : fetch_mariadb_foreign_keys(mysql, schema, child)) {
            if (fk.ref_schema != schema) {
                continue;
            }
            if (table_set.find(fk.ref_table) == table_set.end()) {
                continue;
            }
            if (fk.ref_table == child) {
                continue;
            }
            in_degree[child] += 1;
            dependents[fk.ref_table].push_back(child);
        }
    }

    std::queue<std::string> ready;
    for (const auto& t : tables) {
        if (in_degree[t] == 0) {
            ready.push(t);
        }
    }

    std::vector<std::string> ordered;
    while (!ready.empty()) {
        const std::string current = ready.front();
        ready.pop();
        ordered.push_back(current);
        for (const auto& child : dependents[current]) {
            in_degree[child] -= 1;
            if (in_degree[child] == 0) {
                ready.push(child);
            }
        }
    }

    if (ordered.size() != tables.size()) {
        return tables;
    }
    return ordered;
}

std::vector<std::vector<std::string>> group_tables_by_fk_level(
    MYSQL* mysql,
    const std::string& schema,
    const std::vector<std::string>& tables) {
    std::set<std::string> table_set(tables.begin(), tables.end());
    if (table_set.size() <= 1) {
        return {tables};
    }

    std::unordered_map<std::string, int> in_degree;
    std::unordered_map<std::string, std::vector<std::string>> dependents;
    for (const auto& t : tables) {
        in_degree[t] = 0;
    }

    for (const auto& child : tables) {
        for (const auto& fk : fetch_mariadb_foreign_keys(mysql, schema, child)) {
            if (fk.ref_schema != schema) {
                continue;
            }
            if (table_set.find(fk.ref_table) == table_set.end()) {
                continue;
            }
            if (fk.ref_table == child) {
                continue;
            }
            in_degree[child] += 1;
            dependents[fk.ref_table].push_back(child);
        }
    }

    std::queue<std::string> ready;
    for (const auto& t : tables) {
        if (in_degree[t] == 0) {
            ready.push(t);
        }
    }

    std::vector<std::vector<std::string>> levels;
    std::size_t placed = 0;
    while (!ready.empty()) {
        const std::size_t wave = ready.size();
        std::vector<std::string> level;
        level.reserve(wave);
        for (std::size_t i = 0; i < wave; ++i) {
            const std::string current = ready.front();
            ready.pop();
            level.push_back(current);
            placed += 1;
            for (const auto& child : dependents[current]) {
                in_degree[child] -= 1;
                if (in_degree[child] == 0) {
                    ready.push(child);
                }
            }
        }
        levels.push_back(std::move(level));
    }

    if (placed != tables.size()) {
        return {tables};
    }
    return levels;
}
