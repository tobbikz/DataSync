#pragma once

#include "mariadb_schema.hpp"

#include <libpq-fe.h>
#include <map>
#include <nlohmann/json.hpp>
#include <set>
#include <string>
#include <vector>

std::string mongo_pg_schema_name(const std::string& database_name);
std::string mongo_pg_table_name(const std::string& collection_name);

/** Normalize Mongo _id / documentKey JSON to plain hex string for lake PK matching. */
std::string mongo_object_id_text(const nlohmann::json& id);

/** Catalog/stats key: Mongo uses source_database; normalizes legacy empty source_schema. */
std::string mongo_catalog_source_schema(
    const std::string& source_database,
    const std::string& source_schema);

struct MongoLakeColumn {
    std::string name;
    std::string pg_type;
};

/** Flatten nested document; _id → mongo_id (string). Mirrors cdc_kafka/mongo_lake.py */
std::map<std::string, nlohmann::json> flatten_mongo_document(
    const nlohmann::json& doc,
    const std::string& parent_key = "",
    char sep = '_');

std::string infer_pg_type_from_json(const nlohmann::json& value);

/** Resolve one PG type when a column has multiple observed BSON types (schema-flexible). */
std::string resolve_pg_type_from_type_set(const std::set<std::string>& type_set);

/** Merge inferred types from sample flattened rows. */
std::map<std::string, std::string> infer_schema_from_flat_rows(
    const std::vector<std::map<std::string, nlohmann::json>>& rows,
    std::size_t sample_size = 1000);

void ensure_mongo_lake_table_base(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& cols,
    int partition_months_ahead);

int sync_missing_mongo_columns(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& source_cols);

/** Widen existing lake columns when inferred type is broader (e.g. BOOLEAN → TEXT). */
int sync_mongo_column_types(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& source_cols);

#ifdef HAVE_MONGOC
#include <mongoc/mongoc.h>

struct MongoLakeDdlSyncResult {
    int columns_added{0};
    int columns_widened{0};
};

/** Sample collection docs, flatten, infer PG types (pre-apply / full-load DDL). */
std::vector<std::map<std::string, nlohmann::json>> sample_flattened_mongo_docs(
    mongoc_collection_t* coll,
    std::size_t limit);

/** Add missing columns and widen types on an existing lake table from Mongo sample. */
MongoLakeDdlSyncResult sync_mongo_lake_columns_from_collection(
    PGconn* lake_pg,
    mongoc_collection_t* coll,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::size_t sample_limit = 1000);
#endif
