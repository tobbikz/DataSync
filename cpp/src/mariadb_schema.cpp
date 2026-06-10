#include "mariadb_schema.hpp"

#include "lake_columns.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

bool is_hex_digit_char(char c) {
    return std::isxdigit(static_cast<unsigned char>(c)) != 0;
}

std::vector<std::uint8_t> bytea_repeated_x_escapes(const std::string& value) {
    std::vector<std::uint8_t> out;
    std::size_t i = 0;
    while (i + 3 < value.size() && value[i] == '\\' && (value[i + 1] == 'x' || value[i + 1] == 'X') &&
           is_hex_digit_char(value[i + 2]) && is_hex_digit_char(value[i + 3])) {
        char hex_pair[3] = {value[i + 2], value[i + 3], '\0'};
        out.push_back(static_cast<std::uint8_t>(std::strtoul(hex_pair, nullptr, 16)));
        i += 4;
    }
    if (!out.empty()) {
        return out;
    }
    return {};
}

std::vector<std::uint8_t> bytea_payload_to_bytes(const std::string& value) {
    if (value.empty()) {
        return {};
    }
    if (auto esc = bytea_repeated_x_escapes(value); !esc.empty()) {
        return esc;
    }
    if (value.size() >= 2 && value[0] == '\\' && (value[1] == 'x' || value[1] == 'X')) {
        const std::string tail = value.substr(2);
        if (!tail.empty() && tail.size() % 2 == 0) {
            bool all_hex = true;
            for (char c : tail) {
                if (!is_hex_digit_char(c)) {
                    all_hex = false;
                    break;
                }
            }
            if (all_hex) {
                std::vector<std::uint8_t> out;
                out.reserve(tail.size() / 2);
                for (std::size_t i = 0; i + 1 < tail.size(); i += 2) {
                    char hex_pair[3] = {tail[i], tail[i + 1], '\0'};
                    out.push_back(static_cast<std::uint8_t>(std::strtoul(hex_pair, nullptr, 16)));
                }
                return out;
            }
        }
    }
    if (value.size() >= 2 && value.size() % 2 == 0) {
        bool all_hex = true;
        for (char c : value) {
            if (!is_hex_digit_char(c)) {
                all_hex = false;
                break;
            }
        }
        if (all_hex) {
            std::vector<std::uint8_t> out;
            out.reserve(value.size() / 2);
            for (std::size_t i = 0; i + 1 < value.size(); i += 2) {
                char hex_pair[3] = {value[i], value[i + 1], '\0'};
                out.push_back(static_cast<std::uint8_t>(std::strtoul(hex_pair, nullptr, 16)));
            }
            return out;
        }
    }
    return std::vector<std::uint8_t>(value.begin(), value.end());
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

bool mariadb_is_text_like_mysql_type(const std::string& t) {
    return t.find("char") != std::string::npos || t.find("text") != std::string::npos ||
           t.find("enum") != std::string::npos || t.find("set") != std::string::npos;
}

bool ends_with_ci(const std::string& haystack, const std::string& suffix) {
    if (haystack.size() < suffix.size()) {
        return false;
    }
    const std::string tail = to_lower(haystack.substr(haystack.size() - suffix.size()));
    return tail == to_lower(suffix);
}

bool starts_with_ci(const std::string& haystack, const std::string& prefix) {
    if (haystack.size() < prefix.size()) {
        return false;
    }
    const std::string head = to_lower(haystack.substr(0, prefix.size()));
    return head == to_lower(prefix);
}

bool mariadb_column_prefers_bytea(const std::string& column_name, const std::string& mysql_type_raw) {
    const std::string t = to_lower(mysql_type_raw);
    if (t.find("blob") != std::string::npos || t.find("binary") != std::string::npos) {
        return false;
    }
    if (!mariadb_is_text_like_mysql_type(t)) {
        return false;
    }
    const std::string n = to_lower(column_name);
    static const char* const kExact[] = {
        "pin",
        "password",
        "passwd",
        "pwd",
        "hash",
        "salt",
        "token",
        "secret",
        "checksum",
        "digest",
        "ciphertext",
        "nonce",
        "iv",
        "signature",
        "api_key",
        "private_key",
    };
    for (const char* exact : kExact) {
        if (n == exact) {
            return true;
        }
    }
    static const char* const kSuffix[] = {
        "_hash",
        "_pin",
        "_token",
        "_salt",
        "_secret",
        "_checksum",
        "_digest",
        "_signature",
    };
    for (const char* suffix : kSuffix) {
        if (ends_with_ci(n, suffix)) {
            return true;
        }
    }
    return starts_with_ci(n, "bin_") || starts_with_ci(n, "binary_");
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

std::string mariadb_lake_pg_type(const std::string& column_name, const std::string& mysql_type_raw) {
    if (mariadb_column_prefers_bytea(column_name, mysql_type_raw)) {
        return "BYTEA";
    }
    return mariadb_to_pg_type(mysql_type_raw);
}

std::string sanitize_mariadb_text_for_pg(const std::string& in) {
    if (in.find('\0') == std::string::npos) {
        return in;
    }
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        if (c != '\0') {
            out.push_back(c);
        }
    }
    return out;
}

std::string mariadb_bytea_to_copy_csv(const char* data, std::size_t len) {
    static const char hex[] = "0123456789abcdef";
    std::string hex_payload;
    hex_payload.reserve(2 + len * 2);
    hex_payload += "\\x";
    for (std::size_t i = 0; i < len; ++i) {
        const unsigned char b = static_cast<unsigned char>(data[i]);
        hex_payload.push_back(hex[b >> 4]);
        hex_payload.push_back(hex[b & 0x0f]);
    }
    bool quote = true;
    std::string out = "\"";
    for (char c : hex_payload) {
        out += (c == '"') ? "\"\"" : std::string(1, c);
    }
    out += "\"";
    return out;
}

std::string mariadb_bytea_to_copy_csv(const std::string& value) {
    const auto bytes = bytea_payload_to_bytes(value);
    return mariadb_bytea_to_copy_csv(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

std::string mariadb_bytea_to_sql_literal(const std::string& value) {
    const auto bytes = bytea_payload_to_bytes(value);
    static const char hex[] = "0123456789abcdef";
    std::string out = "'\\x";
    for (std::uint8_t b : bytes) {
        out.push_back(hex[b >> 4]);
        out.push_back(hex[b & 0x0f]);
    }
    out += "'::bytea";
    return out;
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
        "SELECT column_name, column_type, column_key, is_nullable FROM information_schema.columns "
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
        col.is_nullable = !(row[3] && std::string(row[3]) == "NO");
        col.pg_type = mariadb_lake_pg_type(col.name, col.mysql_type);
        cols.push_back(std::move(col));
    }
    mysql_free_result(res);
    if (cols.empty()) {
        throw std::runtime_error("no columns found in source table");
    }
    return cols;
}

void merge_lake_column_nullability(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    std::vector<MariaDbColumn>& cols) {
    if (!pg_lake_table_exists(pg, schema, table)) {
        return;
    }
    const char* vals[] = {schema.c_str(), table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT column_name, is_nullable FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2",
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
        return;
    }
    std::unordered_map<std::string, bool> lake_nullable;
    for (int i = 0; i < PQntuples(res); ++i) {
        const std::string name = PQgetvalue(res, i, 0);
        const bool nullable = PQgetvalue(res, i, 1) && std::string(PQgetvalue(res, i, 1)) == "YES";
        lake_nullable[name] = nullable;
    }
    PQclear(res);
    for (auto& col : cols) {
        const auto it = lake_nullable.find(col.name);
        if (it != lake_nullable.end()) {
            col.is_nullable = col.is_nullable && it->second;
        }
    }
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
