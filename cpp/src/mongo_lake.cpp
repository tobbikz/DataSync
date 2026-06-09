#include "mongo_lake.hpp"

#include "lake_columns.hpp"
#include "mssql_lake.hpp"

#include <algorithm>
#include <cctype>
#include <set>

std::string mongo_pg_schema_name(const std::string& database_name) {
    return sanitize_pg_identifier_part(database_name);
}

std::string mongo_pg_table_name(const std::string& collection_name) {
    return sanitize_pg_identifier_part(collection_name);
}

std::string mongo_object_id_text(const nlohmann::json& id) {
    if (id.is_null()) {
        return {};
    }
    if (id.is_string()) {
        return id.get<std::string>();
    }
    if (id.is_object()) {
        if (id.contains("$oid") && id["$oid"].is_string()) {
            return id["$oid"].get<std::string>();
        }
        if (id.contains("oid") && id["oid"].is_string()) {
            return id["oid"].get<std::string>();
        }
    }
    std::string raw = id.dump();
    if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') {
        return raw.substr(1, raw.size() - 2);
    }
    return raw;
}

std::string mongo_catalog_source_schema(
    const std::string& source_database,
    const std::string& source_schema) {
    return source_schema.empty() ? source_database : source_schema;
}

namespace {

std::string sanitize_col_name(const std::string& name) {
    return sanitize_pg_identifier_part(name);
}

}  // namespace

std::map<std::string, nlohmann::json> flatten_mongo_document(
    const nlohmann::json& doc,
    const std::string& parent_key,
    char sep) {
    std::map<std::string, nlohmann::json> out;
    if (!doc.is_object()) {
        return out;
    }
    for (auto it = doc.begin(); it != doc.end(); ++it) {
        const std::string key = it.key();
        const nlohmann::json& v = it.value();
        std::string new_key;
        if (key == "_id") {
            new_key = "mongo_id";
            if (v.is_object() && v.contains("$oid")) {
                out[new_key] = v["$oid"].get<std::string>();
            } else if (v.is_string()) {
                out[new_key] = v.get<std::string>();
            } else {
                out[new_key] = v.dump();
            }
            continue;
        }
        new_key = parent_key.empty() ? key : parent_key + sep + key;
        new_key = sanitize_col_name(new_key);
        if (v.is_object()) {
            auto nested = flatten_mongo_document(v, new_key, sep);
            out.insert(nested.begin(), nested.end());
        } else if (v.is_array()) {
            out[new_key] = v;
        } else {
            out[new_key] = v;
        }
    }
    return out;
}

std::string infer_pg_type_from_json(const nlohmann::json& value) {
    if (value.is_null()) {
        return "TEXT";
    }
    if (value.is_boolean()) {
        return "BOOLEAN";
    }
    if (value.is_number_integer()) {
        const auto n = value.get<long long>();
        if (n >= -2147483648LL && n <= 2147483647LL) {
            return "INTEGER";
        }
        return "BIGINT";
    }
    if (value.is_number_float()) {
        return "DOUBLE PRECISION";
    }
    if (value.is_object() || value.is_array()) {
        return "JSONB";
    }
    return "TEXT";
}

std::string resolve_pg_type_from_type_set(const std::set<std::string>& type_set) {
    if (type_set.empty()) {
        return "TEXT";
    }
    if (type_set.size() == 1) {
        return *type_set.begin();
    }
    if (type_set.count("JSONB")) {
        return "JSONB";
    }
    const bool numeric_only = std::all_of(type_set.begin(), type_set.end(), [](const std::string& t) {
        return t == "INTEGER" || t == "BIGINT" || t == "DOUBLE PRECISION";
    });
    if (numeric_only) {
        if (type_set.count("DOUBLE PRECISION")) {
            return "DOUBLE PRECISION";
        }
        if (type_set.count("BIGINT")) {
            return "BIGINT";
        }
        return "INTEGER";
    }
    return "TEXT";
}

namespace {

int mongo_type_rank(const std::string& pg_type) {
    if (pg_type == "BOOLEAN") {
        return 1;
    }
    if (pg_type == "INTEGER") {
        return 2;
    }
    if (pg_type == "BIGINT") {
        return 3;
    }
    if (pg_type == "DOUBLE PRECISION") {
        return 4;
    }
    if (pg_type == "JSONB") {
        return 5;
    }
    return 6;
}

std::string normalize_pg_data_type(const std::string& data_type, const std::string& udt_name) {
    if (udt_name == "jsonb" || data_type == "jsonb") {
        return "JSONB";
    }
    if (udt_name == "bool" || data_type == "boolean") {
        return "BOOLEAN";
    }
    if (udt_name == "int4" || data_type == "integer") {
        return "INTEGER";
    }
    if (udt_name == "int8" || data_type == "bigint") {
        return "BIGINT";
    }
    if (udt_name == "float8" || data_type == "double precision") {
        return "DOUBLE PRECISION";
    }
    return "TEXT";
}

std::string alter_column_using_expr(
    const std::string& col,
    const std::string& from_type,
    const std::string& to_type) {
    const std::string qcol = pg_ident(col);
    if (from_type == "BOOLEAN" && to_type == "TEXT") {
        return "CASE WHEN " + qcol + " IS NULL THEN NULL WHEN " + qcol + " THEN 'true' ELSE 'false' END";
    }
    if (to_type == "TEXT") {
        return qcol + "::text";
    }
    if (to_type == "BIGINT") {
        return qcol + "::bigint";
    }
    if (to_type == "DOUBLE PRECISION") {
        return qcol + "::double precision";
    }
    if (to_type == "JSONB") {
        return "to_jsonb(" + qcol + ")";
    }
    return qcol + "::text";
}

}  // namespace

std::map<std::string, std::string> infer_schema_from_flat_rows(
    const std::vector<std::map<std::string, nlohmann::json>>& rows,
    std::size_t sample_size) {
    std::map<std::string, std::set<std::string>> types;
    const std::size_t limit = std::min(sample_size, rows.size());
    for (std::size_t i = 0; i < limit; ++i) {
        for (const auto& [key, value] : rows[i]) {
            if (key == "mongo_id") {
                continue;
            }
            types[key].insert(infer_pg_type_from_json(value));
        }
    }
    std::map<std::string, std::string> final_cols;
    for (const auto& [key, type_set] : types) {
        final_cols[key] = resolve_pg_type_from_type_set(type_set);
    }
    return final_cols;
}

void ensure_mongo_lake_table_base(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& cols,
    int partition_months_ahead) {
    pg_exec(pg, "CREATE SCHEMA IF NOT EXISTS " + pg_ident(pg_schema));

    std::vector<std::string> col_defs;
    col_defs.push_back(pg_ident("mongo_id") + " TEXT NOT NULL");
    for (const auto& [name, pg_type] : cols) {
        if (name == "mongo_id") {
            continue;
        }
        col_defs.push_back(pg_ident(name) + " " + pg_type + " NULL");
    }
    col_defs.push_back(pg_ident(lake_columns::kLoadTimestamp) + " TIMESTAMPTZ NOT NULL DEFAULT NOW()");
    col_defs.push_back(pg_ident(lake_columns::kLoadDate) + " DATE DEFAULT CURRENT_DATE");
    col_defs.push_back(pg_ident(lake_columns::kSourceSystem) + " VARCHAR(50) DEFAULT 'MongoDB'");
    col_defs.push_back(pg_ident(lake_columns::kSnapshotId) + " TEXT");

    std::string create = "CREATE TABLE IF NOT EXISTS " + pg_ident(pg_schema) + "." + pg_ident(pg_table) + " (\n  ";
    for (std::size_t i = 0; i < col_defs.size(); ++i) {
        if (i) {
            create += ",\n  ";
        }
        create += col_defs[i];
    }
    create += ",\n  PRIMARY KEY (" + pg_ident("mongo_id") + ", " + pg_ident(lake_columns::kLoadTimestamp) + ")";
    create += "\n) PARTITION BY RANGE (" + pg_ident(lake_columns::kPartitionColumn) + ")";
    pg_exec(pg, create);

    const std::string months = std::to_string(std::max(1, partition_months_ahead));
    const char* part_vals[] = {pg_schema.c_str(), pg_table.c_str(), months.c_str()};
    PGresult* part_res = PQexecParams(
        pg,
        "SELECT lake.ensure_monthly_partitions($1::text, $2::text, $3::integer)",
        3,
        nullptr,
        part_vals,
        nullptr,
        nullptr,
        0);
    if (!part_res || (PQresultStatus(part_res) != PGRES_TUPLES_OK && PQresultStatus(part_res) != PGRES_COMMAND_OK)) {
        const std::string err = PQerrorMessage(pg);
        if (part_res) {
            PQclear(part_res);
        }
        throw std::runtime_error("partition ensure failed: " + err);
    }
    PQclear(part_res);
}

int sync_missing_mongo_columns(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& source_cols) {
    if (!pg_lake_table_exists(pg, pg_schema, pg_table)) {
        return 0;
    }
    const char* vals[] = {pg_schema.c_str(), pg_table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT column_name FROM information_schema.columns "
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
    std::set<std::string> pg_cols;
    for (int i = 0; i < PQntuples(res); ++i) {
        pg_cols.insert(PQgetvalue(res, i, 0));
    }
    PQclear(res);

    int added = 0;
    const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
    for (const auto& [name, pg_type] : source_cols) {
        if (name == "mongo_id" || pg_cols.count(name)) {
            continue;
        }
        pg_exec(pg, "ALTER TABLE " + fq + " ADD COLUMN " + pg_ident(name) + " " + pg_type + " NULL");
        added += 1;
    }
    return added;
}

int sync_mongo_column_types(
    PGconn* pg,
    const std::string& pg_schema,
    const std::string& pg_table,
    const std::map<std::string, std::string>& source_cols) {
    if (!pg_lake_table_exists(pg, pg_schema, pg_table)) {
        return 0;
    }
    const char* vals[] = {pg_schema.c_str(), pg_table.c_str()};
    PGresult* res = PQexecParams(
        pg,
        "SELECT column_name, data_type, udt_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "AND column_name NOT IN ('mongo_id','_dl_load_timestamp','_dl_load_date','_dl_source_system','_dl_snapshot_id')",
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
        existing_types[col] = normalize_pg_data_type(data_type, udt_name);
    }
    PQclear(res);

    int widened = 0;
    const std::string fq = pg_ident(pg_schema) + "." + pg_ident(pg_table);
    for (const auto& [name, inferred_type] : source_cols) {
        if (name == "mongo_id") {
            continue;
        }
        const auto it = existing_types.find(name);
        if (it == existing_types.end()) {
            continue;
        }
        const std::string& existing_type = it->second;
        if (mongo_type_rank(inferred_type) <= mongo_type_rank(existing_type)) {
            continue;
        }
        if (existing_type == "JSONB") {
            continue;
        }
        const std::string using_expr = alter_column_using_expr(name, existing_type, inferred_type);
        pg_exec(
            pg,
            "ALTER TABLE " + fq + " ALTER COLUMN " + pg_ident(name) + " TYPE " + inferred_type +
                " USING " + using_expr);
        widened += 1;
    }
    return widened;
}

#ifdef HAVE_MONGOC

#include <bson/bson.h>
#include <mongoc/mongoc.h>

namespace {

nlohmann::json bson_to_json_relaxed(const bson_t* doc) {
    char* json_str = bson_as_relaxed_extended_json(doc, nullptr);
    if (!json_str) {
        return nlohmann::json::object();
    }
    nlohmann::json out = nlohmann::json::parse(json_str);
    bson_free(json_str);
    return out;
}

}  // namespace

std::vector<std::map<std::string, nlohmann::json>> sample_flattened_mongo_docs(
    mongoc_collection_t* coll,
    std::size_t limit) {
    std::vector<std::map<std::string, nlohmann::json>> out;
    bson_t opts = BSON_INITIALIZER;
    bson_t sort_doc;
    BSON_APPEND_DOCUMENT_BEGIN(&opts, "sort", &sort_doc);
    BSON_APPEND_INT32(&sort_doc, "_id", 1);
    bson_append_document_end(&opts, &sort_doc);
    BSON_APPEND_INT32(&opts, "limit", static_cast<int>(limit));

    const bson_t empty_query = BSON_INITIALIZER;
    mongoc_cursor_t* cursor = mongoc_collection_find_with_opts(coll, &empty_query, &opts, nullptr);
    bson_destroy(&opts);

    const bson_t* doc = nullptr;
    while (mongoc_cursor_next(cursor, &doc)) {
        const auto j = bson_to_json_relaxed(doc);
        if (j.is_object()) {
            out.push_back(flatten_mongo_document(j));
        }
    }
    mongoc_cursor_destroy(cursor);
    return out;
}

MongoLakeDdlSyncResult sync_mongo_lake_columns_from_collection(
    PGconn* lake_pg,
    mongoc_collection_t* coll,
    const std::string& pg_schema,
    const std::string& pg_table,
    std::size_t sample_limit) {
    MongoLakeDdlSyncResult result;
    if (!pg_lake_table_exists(lake_pg, pg_schema, pg_table)) {
        return result;
    }
    const auto sample = sample_flattened_mongo_docs(coll, sample_limit);
    const auto pg_cols = infer_schema_from_flat_rows(sample, sample_limit);
    result.columns_added = sync_missing_mongo_columns(lake_pg, pg_schema, pg_table, pg_cols);
    result.columns_widened = sync_mongo_column_types(lake_pg, pg_schema, pg_table, pg_cols);
    return result;
}

#endif
