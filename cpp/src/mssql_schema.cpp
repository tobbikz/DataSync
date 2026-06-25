#include "mssql_schema.hpp"

#include "capture_common.hpp"
#include "lake_columns.hpp"
#include "mssql_lake.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace {

std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

}  // namespace

std::string mssql_to_pg_type(const std::string& data_type, int max_length, int precision, int scale) {
    const std::string base = to_lower(data_type);
    if ((base == "decimal" || base == "numeric") && precision > 0) {
        return "NUMERIC(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
    }
    if (base == "nvarchar" || base == "varchar" || base == "nchar" || base == "char") {
        if (max_length == -1) {
            return "TEXT";
        }
        int char_len = max_length;
        if (base == "nvarchar" || base == "nchar") {
            char_len = max_length / 2;
        }
        if (char_len > 0 && char_len < 8000) {
            return "VARCHAR(" + std::to_string(char_len) + ")";
        }
        if (char_len >= 8000) {
            return "TEXT";
        }
    }
    if (base == "int") {
        return "INTEGER";
    }
    if (base == "bigint") {
        return "BIGINT";
    }
    if (base == "smallint" || base == "tinyint") {
        return "SMALLINT";
    }
    if (base == "bit") {
        return "BOOLEAN";
    }
    if (base == "decimal" || base == "numeric") {
        return "NUMERIC";
    }
    if (base == "money") {
        return "NUMERIC(19,4)";
    }
    if (base == "smallmoney") {
        return "NUMERIC(10,4)";
    }
    if (base == "float") {
        return "DOUBLE PRECISION";
    }
    if (base == "real") {
        return "REAL";
    }
    if (base == "datetime" || base == "datetime2" || base == "smalldatetime") {
        return "TIMESTAMPTZ";
    }
    if (base == "date") {
        return "DATE";
    }
    if (base == "time" || base.rfind("time(", 0) == 0) {
        return "TIME";
    }
    if (base == "uniqueidentifier") {
        return "UUID";
    }
    if (base == "varbinary" || base == "binary" || base == "image") {
        return "BYTEA";
    }
    return "TEXT";
}

#ifdef HAVE_FREETDS

namespace {

std::string sql_escape_literal(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        out += (c == '\'') ? "''" : std::string(1, c);
    }
    out += "'";
    return out;
}

std::string format_dberror(DBPROCESS* db) {
    if (!db) {
        return "null DBPROCESS";
    }
    char err_buf[1024];
    err_buf[0] = '\0';
    dbstrcpy(db, 1, static_cast<int>(sizeof(err_buf) - 1), err_buf);
    if (err_buf[0]) {
        return err_buf;
    }
    return "unknown DB-Library error";
}

}  // namespace

std::vector<MssqlColumn> fetch_mssql_columns(
    DBPROCESS* db,
    const std::string& schema,
    const std::string& table) {
    std::ostringstream sql;
    sql << "SELECT c.name, t.name, c.max_length, c.precision, c.scale, "
           "CASE WHEN pk.column_id IS NOT NULL THEN 'PRI' ELSE '' END "
           "FROM sys.columns c "
           "JOIN sys.types t ON c.user_type_id = t.user_type_id "
           "JOIN sys.tables tb ON c.object_id = tb.object_id "
           "JOIN sys.schemas s ON tb.schema_id = s.schema_id "
           "LEFT JOIN ("
           "  SELECT ic.object_id, ic.column_id "
           "  FROM sys.index_columns ic "
           "  JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
           "  WHERE i.is_primary_key = 1"
           ") pk ON pk.object_id = c.object_id AND pk.column_id = c.column_id "
           "WHERE s.name = " << sql_escape_literal(schema) << " AND tb.name = " << sql_escape_literal(table)
        << " ORDER BY c.column_id";

    run_dbsql(db, sql.str());

    std::vector<MssqlColumn> cols;
    if (dbresults(db) == FAIL) {
        throw std::runtime_error("MSSQL column query dbresults failed: " + format_dberror(db));
    }

    while (true) {
        const int rc = dbnextrow(db);
        if (rc == NO_MORE_ROWS) {
            break;
        }
        if (rc == FAIL) {
            throw std::runtime_error("MSSQL column query dbnextrow failed: " + format_dberror(db));
        }
        MssqlColumn col;
        auto read_text = [&](int colnum) -> std::string {
            if (!dbdata(db, colnum)) {
                return {};
            }
            char buf[512];
            mssql_cell_to_char(db, colnum, buf, static_cast<DBINT>(sizeof(buf) - 1));
            buf[sizeof(buf) - 1] = '\0';
            return trim_mssql_text(buf);
        };
        col.name = read_text(1);
        col.mssql_type = read_text(2);
        if (col.mssql_type.empty()) {
            col.mssql_type = "text";
        }
        auto read_int = [&](int colnum) -> int {
            if (!dbdata(db, colnum)) {
                return 0;
            }
            char buf[64];
            mssql_cell_to_char(db, colnum, buf, static_cast<DBINT>(sizeof(buf) - 1));
            buf[sizeof(buf) - 1] = '\0';
            return std::atoi(buf);
        };
        const int max_len = read_int(3);
        const int prec = read_int(4);
        const int scale = read_int(5);
        col.is_pk = read_text(6) == "PRI";
        col.pg_type = mssql_to_pg_type(col.mssql_type, max_len, prec, scale);
        if (!col.name.empty()) {
            cols.push_back(std::move(col));
        }
    }

    while (true) {
        const int r = dbresults(db);
        if (r == NO_MORE_RESULTS) break;
        if (r == FAIL) throw std::runtime_error("MSSQL schema drain failed");
    }

    if (cols.empty()) {
        throw std::runtime_error("no columns found in MSSQL source table");
    }
    return cols;
}

std::vector<std::string> fetch_lake_primary_key_columns(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table) {
    const char* vals[] = {pg_schema.c_str(), pg_table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        R"(
        SELECT a.attname
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
        ORDER BY array_position(i.indkey::smallint[], a.attnum)
        )",
        2,
        nullptr,
        vals,
        nullptr,
        nullptr,
        0);
    std::vector<std::string> out;
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

std::vector<std::string> expected_mssql_lake_pk(const std::vector<std::string>& source_pk_cols) {
    return lake_columns::expected_lake_pk(source_pk_cols);
}

bool lake_pk_matches(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<std::string>& expected) {
    const auto actual = fetch_lake_primary_key_columns(pg, pg_schema, pg_table);
    return actual == expected;
}

void ensure_mssql_lake_table_base(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::vector<MssqlColumn>& cols,
    const std::vector<std::string>& source_pk_cols,
    int partition_months_ahead) {
    pg_exec(pg, "CREATE SCHEMA IF NOT EXISTS " + pg_ident(pg_schema));

    const auto expected_pk = expected_mssql_lake_pk(source_pk_cols);
    if (pg_lake_table_exists(pg, pg_schema, pg_table) && !lake_pk_matches(pg, pg_schema, pg_table, expected_pk)) {
        throw std::runtime_error(
            "lake primary key mismatch for " + pg_schema + "." + pg_table +
            " — manual intervention required (DROP not allowed)");
    }
    drop_lake_table_if_not_partitioned(pg, pg_schema, pg_table);

    std::vector<std::string> pk_set(source_pk_cols.begin(), source_pk_cols.end());
    std::vector<std::string> col_defs;
    std::vector<std::string> pk_cols;
    for (const auto& col : cols) {
        const bool is_pk =
            std::find(pk_set.begin(), pk_set.end(), col.name) != pk_set.end();
        col_defs.push_back(pg_ident(col.name) + " " + col.pg_type + (is_pk ? " NOT NULL" : " NULL"));
        if (is_pk) {
            pk_cols.push_back(pg_ident(col.name));
        }
    }
    col_defs.push_back(pg_ident(lake_columns::kLoadTimestamp) + " TIMESTAMPTZ NOT NULL DEFAULT NOW()");
    col_defs.push_back(pg_ident(lake_columns::kLoadDate) + " DATE NOT NULL DEFAULT CURRENT_DATE");
    col_defs.push_back(pg_ident(lake_columns::kSourceSystem) + " VARCHAR(50) DEFAULT 'MSSQL'");
    col_defs.push_back(pg_ident(lake_columns::kSnapshotId) + " TEXT");

    std::string create = "CREATE TABLE IF NOT EXISTS " + pg_ident(pg_schema) + "." + pg_ident(pg_table) + " (\n  ";
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
        create += ", " + pg_ident(lake_columns::kLoadTimestamp) + ")";
    }
    create += "\n) PARTITION BY RANGE (" + pg_ident(lake_columns::kPartitionColumn) + ")";
    pg_exec(pg, create);

    const std::string months = std::to_string(std::max(1, partition_months_ahead));
    const char* part_vals[] = {pg_schema.c_str(), pg_table.c_str(), months.c_str()};
    pg_exec_params_simple(
        pg,
        "SELECT lake.ensure_monthly_partitions($1::text, $2::text, $3::integer)",
        3,
        part_vals);
}

#endif
