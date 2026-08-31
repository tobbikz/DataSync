#pragma once

#include "pg_conn.hpp"

#include <libpq-fe.h>
#include <mysql/mysql.h>

#include <string>
#include <vector>

struct MariaDbColumn {
    std::string name;
    std::string mysql_type;
    bool is_pk{false};
    bool is_nullable{true};
    std::string pg_type;
};

std::string pg_ident(const std::string& name);
std::string mariadb_to_pg_type(const std::string& mysql_type_raw);

/** Lake PG type: BLOB/BINARY → BYTEA; everything else from mariadb_to_pg_type. */
std::string mariadb_lake_pg_type(const std::string& column_name, const std::string& mysql_type_raw);

/** Strip NUL bytes and other bytes PostgreSQL TEXT rejects (mirrors MSSQL sanitizer). */
std::string sanitize_mariadb_text_for_pg(const std::string& value);

/** Strip/replace invalid UTF-8 so nlohmann::json can serialize capture payloads. */
std::string sanitize_utf8_for_json(const std::string& value);

/** Binlog BLOB/BINARY cell → \\xHEX JSON string (valid UTF-8 for Kafka). */
std::string mariadb_binary_cell_to_json_hex(const std::string& binlog_cell);

/** Raw cell value when MariaDB sends NULL but the lake column is NOT NULL (schema drift). */
std::string mariadb_not_null_copy_default(const MariaDbColumn& col);

/** COPY CSV cell for BYTEA (hex \\x… form). */
std::string mariadb_bytea_to_copy_csv(const char* data, std::size_t len);
std::string mariadb_bytea_to_copy_csv(const std::string& value);

/** SQL literal for BYTEA upserts (CDC apply). */
std::string mariadb_bytea_to_sql_literal(const std::string& value);

std::vector<MariaDbColumn> fetch_mariadb_columns(MYSQL* mysql, const std::string& schema, const std::string& table);

/** Tighten is_nullable when the lake column is NOT NULL (schema drift vs MariaDB). */
void merge_lake_column_nullability(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    std::vector<MariaDbColumn>& cols);

void ensure_lake_table_base(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols,
    int partition_months_ahead = 3);

void truncate_lake_table(PGconn* pg, const std::string& schema, const std::string& table);

bool pg_lake_table_exists(PGconn* pg, const std::string& schema, const std::string& table);

std::string lake_column_data_type(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::string& column);

/** True when schema.table exists as a partitioned parent (pg_class.relkind = p). */
bool pg_lake_table_is_partitioned(PGconn* pg, const std::string& schema, const std::string& table);

/** Drop legacy non-partitioned lake shells so PARTITION BY RANGE create can succeed. */
void drop_lake_table_if_not_partitioned(PGconn* pg, const std::string& schema, const std::string& table);

struct LakeTypeMigration {
    int columns_migrated{0};
    /** First column the lake refused to migrate in place; empty when everything went through. */
    std::string failed_column;
    std::string from_type;
    std::string to_type;
    std::string error;

    bool failed() const { return !failed_column.empty(); }
};

/**
 * Align lake column types with the source (e.g. int unsigned → bigint, varchar shrink).
 * Best effort: a column whose data does not fit the new type is left alone and reported, so
 * the caller decides — the full load retries it on the truncated table, CDC reloads instead.
 */
LakeTypeMigration migrate_lake_table_schema(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<MariaDbColumn>& cols);

/** Widen integer/smallint lake columns to bigint (CDC unsigned bit patterns in signed MariaDB ints). */
void widen_lake_integer_column_to_bigint(
    PGconn* pg,
    const std::string& schema,
    const std::string& table,
    const std::string& column);
