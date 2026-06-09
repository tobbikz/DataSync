#include "mariadb_schema.hpp"

#include "lake_columns.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace {

std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

}  // namespace

std::string pg_ident(const std::string& name) {
    std::string out = "\"";
    for (char c : name) {
        out += (c == '"') ? "\"\"" : std::string(1, c);
    }
    out += "\"";
    return out;
}

std::string mariadb_to_pg_type(const std::string& mysql_type_raw) {
    const std::string t = to_lower(mysql_type_raw);
    if (t.find("bigint") != std::string::npos) {
        return "BIGINT";
    }
    if (t.find("int") != std::string::npos) {
        if (t.find("tinyint") != std::string::npos || t.find("smallint") != std::string::npos) {
            return "SMALLINT";
        }
        if (t.find("mediumint") != std::string::npos) {
            return "INTEGER";
        }
        return "NUMERIC";
    }
    if (t.find("decimal") != std::string::npos || t.find("numeric") != std::string::npos) {
        return "DECIMAL";
    }
    if (t.find("double") != std::string::npos) {
        return "DOUBLE PRECISION";
    }
    if (t.find("float") != std::string::npos) {
        return "REAL";
    }
    if (t.find("datetime") != std::string::npos || t.find("timestamp") != std::string::npos) {
        return "TIMESTAMPTZ";  // lake stores instants in UTC
    }
    if (t.find("date") != std::string::npos && t.find("datetime") == std::string::npos) {
        return "DATE";
    }
    if (t.find("time") != std::string::npos && t.find("timestamp") == std::string::npos) {
        return "TIME";
    }
    if (t.find("blob") != std::string::npos || t.find("binary") != std::string::npos) {
        return "BYTEA";
    }
    if (t.find("json") != std::string::npos) {
        return "JSONB";
    }
    return "TEXT";
}

void pg_exec(PGconn* pg, const std::string& sql) {
    PGresult* res = PQexec(pg, sql.c_str());
    if (!res) {
        throw std::runtime_error("PQexec returned null");
    }
    const auto st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error(err + " | " + sql);
    }
    PQclear(res);
}

void pg_exec_params_simple(PGconn* pg, const char* sql, int n, const char* const* vals) {
    PGresult* res = PQexecParams(pg, sql, n, nullptr, vals, nullptr, nullptr, 0);
    if (!res) {
        throw std::runtime_error(std::string("SQL failed: ") + PQerrorMessage(pg) + " | " + sql);
    }
    const auto st = PQresultStatus(res);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        const std::string err = PQerrorMessage(pg);
        PQclear(res);
        throw std::runtime_error(std::string("SQL failed: ") + err + " | " + sql);
    }
    PQclear(res);
}

std::vector<MariaDbColumn> fetch_mariadb_columns(MYSQL* mysql, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT column_name, column_type, column_key FROM information_schema.columns "
        "WHERE table_schema='" +
        schema + "' AND table_name='" + table + "' ORDER BY ordinal_position";

    if (mysql_query(mysql, sql.c_str()) != 0) {
        throw std::runtime_error(std::string("schema read failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        throw std::runtime_error(std::string("schema store_result failed: ") + mysql_error(mysql));
    }

    std::vector<MariaDbColumn> cols;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (!row[0] || !row[1]) {
            continue;
        }
        MariaDbColumn col;
        col.name = row[0];
        col.mysql_type = row[1];
        col.is_pk = row[2] && std::string(row[2]) == "PRI";
        col.pg_type = mariadb_to_pg_type(col.mysql_type);
        cols.push_back(std::move(col));
    }
    mysql_free_result(res);
    if (cols.empty()) {
        throw std::runtime_error("no columns found in source table");
    }
    return cols;
}

void ensure_lake_table_base(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    int partition_months_ahead) {
    pg_exec(pg, "CREATE SCHEMA IF NOT EXISTS " + pg_ident(schema));

    std::vector<std::string> col_defs;
    std::vector<std::string> pk_cols;
    for (const auto& col : cols) {
        col_defs.push_back(pg_ident(col.name) + " " + col.pg_type + (col.is_pk ? " NOT NULL" : " NULL"));
        if (col.is_pk) {
            pk_cols.push_back(pg_ident(col.name));
        }
    }
    col_defs.push_back(pg_ident(lake_columns::kLoadTimestamp) + " TIMESTAMPTZ NOT NULL DEFAULT NOW()");
    col_defs.push_back(pg_ident(lake_columns::kLoadDate) + " DATE DEFAULT CURRENT_DATE");
    col_defs.push_back(pg_ident(lake_columns::kSourceSystem) + " VARCHAR(50) DEFAULT 'MariaDB'");
    col_defs.push_back(pg_ident(lake_columns::kSnapshotId) + " TEXT");

    std::string create = "CREATE TABLE IF NOT EXISTS " + pg_ident(schema) + "." + pg_ident(table) + " (\n  ";
    for (std::size_t i = 0; i < col_defs.size(); ++i) {
        if (i) {
            create += ",\n  ";
        }
        create += col_defs[i];
    }
    if (!pk_cols.empty()) {
        create += ",\n  PRIMARY KEY (";
        for (std::size_t i = 0; i < pk_cols.size(); ++i) {
            if (i) {
                create += ", ";
            }
            create += pk_cols[i];
        }
        create += ", " + pg_ident(lake_columns::kLoadTimestamp);
        create += ")";
    }
    create += "\n) PARTITION BY RANGE (" + pg_ident(lake_columns::kPartitionColumn) + ")";
    pg_exec(pg, create);

    const std::string months = std::to_string(std::max(1, partition_months_ahead));
    const char* part_vals[] = {schema.c_str(), table.c_str(), months.c_str()};
    pg_exec_params_simple(
        pg,
        "SELECT lake.ensure_monthly_partitions($1::text, $2::text, $3::integer)",
        3,
        part_vals);
}

void truncate_lake_table(PGconn* pg, const std::string& schema, const std::string& table) {
    pg_exec(pg, "TRUNCATE TABLE " + pg_ident(schema) + "." + pg_ident(table) + " RESTART IDENTITY CASCADE");
}

bool pg_lake_table_exists(PGconn* pg, const std::string& schema, const std::string& table) {
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
