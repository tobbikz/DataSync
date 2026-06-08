#include "catalog_sync.hpp"

#include "mariadb_conn.hpp"
#include "mariadb_preflight.hpp"
#include "mongo_preflight.hpp"
#include "mssql_conn.hpp"
#include "mongo_conn.hpp"
#include "obs_log.hpp"
#include "pg_conn.hpp"
#include "runtime_config.hpp"

#include <mysql/mysql.h>

#include <chrono>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef HAVE_MONGOC
#include <mongoc/mongoc.h>
#endif

namespace {

using TableKey = std::pair<std::string, std::string>;

struct CatalogObjectKey {
    std::string source_database;
    std::string source_schema;
    std::string source_table;
};

struct MssqlCatalogObject {
    CatalogObjectKey key;
    std::string capture_instance;
    bool has_pk{false};
    std::string pk_columns;
};

struct PgTxn {
    PGconn* conn;
    bool ok{true};

    explicit PgTxn(PgConn& pg) : conn(pg.raw) {
        exec("BEGIN");
    }

    ~PgTxn() {
        exec(ok ? "COMMIT" : "ROLLBACK");
    }

    void exec(const std::string& sql) {
        PGresult* res = PQexec(conn, sql.c_str());
        if (!res) {
            ok = false;
            throw std::runtime_error("PQexec returned null");
        }
        const auto st = PQresultStatus(res);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(conn);
            PQclear(res);
            ok = false;
            throw std::runtime_error("SQL failed: " + err + " | " + sql);
        }
        PQclear(res);
    }

    int exec_count(const std::string& sql) {
        PGresult* res = PQexec(conn, sql.c_str());
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
            if (res) {
                PQclear(res);
            }
            ok = false;
            throw std::runtime_error("count query failed");
        }
        const int n = std::atoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        return n;
    }

    int exec_params_count(const char* sql, int n_params, const char* const* values) {
        PGresult* res = PQexecParams(conn, sql, n_params, nullptr, values, nullptr, nullptr, 0);
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
            if (res) {
                PQclear(res);
            }
            ok = false;
            throw std::runtime_error("params count query failed");
        }
        const int n = std::atoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        return n;
    }

    void exec_params(
        const char* sql,
        int n_params,
        const char* const* values,
        const int* lengths = nullptr,
        const int* formats = nullptr) {
        PGresult* res = PQexecParams(conn, sql, n_params, nullptr, values, lengths, formats, 0);
        if (!res) {
            ok = false;
            throw std::runtime_error("PQexecParams returned null");
        }
        const auto st = PQresultStatus(res);
        if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
            const std::string err = PQerrorMessage(conn);
            PQclear(res);
            ok = false;
            throw std::runtime_error(std::string("SQL params failed: ") + err);
        }
        PQclear(res);
    }
};

void log_checkpoint(
    PGconn* log_pg,
    LogLevel level,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& message,
    const nlohmann::json& context) {
    if (!log_pg) {
        return;
    }
    LogEvent ev;
    ev.level = level;
    ev.component = "catalog";
    ev.message = message;
    ev.batch_id = batch_id;
    ev.conn_id = conn_id;
    ev.context = context;
    log_write(log_pg, ev);
}

long long elapsed_ms(const std::chrono::steady_clock::time_point& start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

void sleep_ms(int ms) {
    if (ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
}

std::string escape_mysql_literal(MYSQL* mysql, const std::string& value) {
    std::string out;
    out.resize(value.size() * 2 + 1);
    const unsigned long len = mysql_real_escape_string(
        mysql, out.data(), value.c_str(), static_cast<unsigned long>(value.size()));
    out.resize(len);
    return out;
}

std::vector<CatalogObjectKey> fetch_mariadb_objects(MYSQL* mysql, const std::string& schema_filter) {
    std::ostringstream sql;
    sql << R"(
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
          AND table_type = 'BASE TABLE'
    )";
    if (!schema_filter.empty()) {
        sql << " AND table_schema = '" << escape_mysql_literal(mysql, schema_filter) << "'";
    }
    sql << " ORDER BY table_schema, table_name";

    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        throw std::runtime_error(std::string("MariaDB table scan failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        throw std::runtime_error(std::string("MariaDB store_result failed: ") + mysql_error(mysql));
    }

    std::vector<CatalogObjectKey> out;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (row[0] && row[1]) {
            out.push_back({"", row[0], row[1]});
        }
    }
    mysql_free_result(res);
    return out;
}

std::map<TableKey, std::string> fetch_mariadb_primary_keys(MYSQL* mysql, const std::string& schema_filter) {
    std::ostringstream sql;
    sql << R"(
        SELECT k.table_schema, k.table_name, k.column_name, k.ordinal_position
        FROM information_schema.key_column_usage k
        INNER JOIN information_schema.tables t
            ON t.table_schema = k.table_schema AND t.table_name = k.table_name
        WHERE k.constraint_name = 'PRIMARY'
          AND t.table_schema NOT IN ('information_schema', 'performance_schema', 'sys', 'mysql')
          AND t.table_type = 'BASE TABLE'
    )";
    if (!schema_filter.empty()) {
        sql << " AND t.table_schema = '" << escape_mysql_literal(mysql, schema_filter) << "'";
    }
    sql << " ORDER BY k.table_schema, k.table_name, k.ordinal_position";

    if (mysql_query(mysql, sql.str().c_str()) != 0) {
        throw std::runtime_error(std::string("MariaDB PK scan failed: ") + mysql_error(mysql));
    }

    MYSQL_RES* res = mysql_store_result(mysql);
    if (!res) {
        throw std::runtime_error(std::string("MariaDB PK store_result failed: ") + mysql_error(mysql));
    }

    std::map<TableKey, std::vector<std::string>> cols;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        if (!row[0] || !row[1] || !row[2]) {
            continue;
        }
        cols[{row[0], row[1]}].emplace_back(row[2]);
    }
    mysql_free_result(res);

    std::map<TableKey, std::string> out;
    for (auto& [key, parts] : cols) {
        std::ostringstream oss;
        for (std::size_t i = 0; i < parts.size(); ++i) {
            if (i) {
                oss << ',';
            }
            oss << parts[i];
        }
        out[key] = oss.str();
    }
    return out;
}

std::string catalog_discovered_schema(
    const std::string& db_engine,
    const CatalogObjectKey& key) {
    if (db_engine == "mongodb" && key.source_schema.empty()) {
        return key.source_database;
    }
    return key.source_schema;
}

void fill_discovered_temp(
    PgTxn& tx,
    const std::string& db_engine,
    const std::vector<CatalogObjectKey>& discovered) {
    tx.exec("CREATE TEMP TABLE IF NOT EXISTS tmp_catalog_discovered ("
            "source_database text NOT NULL DEFAULT '',"
            "source_schema text NOT NULL,"
            "source_table text NOT NULL,"
            "PRIMARY KEY (source_database, source_schema, source_table)) ON COMMIT DROP");
    tx.exec("TRUNCATE tmp_catalog_discovered");

    for (const auto& key : discovered) {
        const std::string schema = catalog_discovered_schema(db_engine, key);
        const char* vals[] = {key.source_database.c_str(), schema.c_str(), key.source_table.c_str()};
        tx.exec_params(
            "INSERT INTO tmp_catalog_discovered (source_database, source_schema, source_table)"
            " VALUES ($1, $2, $3)"
            " ON CONFLICT (source_database, source_schema, source_table) DO NOTHING",
            3,
            vals);
    }
}

void cleanup_orphans_before_prune(PgTxn& tx, const std::string& conn_id, const std::string& db_engine) {
    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    const std::string discovered_match =
        db_engine == "mongodb"
            ? R"(d.source_database = c.source_database
              AND d.source_table = c.source_table
              AND d.source_schema = COALESCE(NULLIF(c.source_schema, ''), c.source_database))"
            : R"(d.source_database = c.source_database
              AND d.source_schema = c.source_schema
              AND d.source_table = c.source_table)";
    const std::string kDoomed =
        "SELECT c.source_database, c.source_schema, c.source_table"
        " FROM cdc_catalog.catalog c"
        " WHERE c.conn_id = $1"
        "   AND c.db_engine = $2::cdc_catalog.db_engine"
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM tmp_catalog_discovered d"
        "     WHERE " +
        discovered_match + ")";

    tx.exec_params(
        ("WITH doomed AS (" + std::string(kDoomed) + R"()
        DELETE FROM cdc_catalog.cdc_applied_events ae
        USING doomed d
        WHERE ae.conn_id = $1
          AND ae.source_schema = d.source_schema
          AND ae.source_table = d.source_table
        )").c_str(),
        2,
        vals);

    tx.exec_params(
        ("WITH doomed AS (" + std::string(kDoomed) + R"()
        DELETE FROM cdc_catalog.cdc_mssql_lsn l
        USING doomed d
        WHERE l.conn_id = $1
          AND l.database = d.source_database
          AND l.schema_name = d.source_schema
          AND l.table_name = d.source_table
        )").c_str(),
        2,
        vals);

    tx.exec_params(
        ("WITH doomed AS (" + std::string(kDoomed) + R"()
        DELETE FROM cdc_catalog.cdc_mongo_resume r
        USING doomed d
        WHERE r.conn_id = $1
          AND r.database = d.source_database
          AND r.collection = d.source_table
        )").c_str(),
        2,
        vals);
}

int prune_missing_objects(
    PgTxn& tx,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::vector<CatalogObjectKey>& discovered) {
    fill_discovered_temp(tx, db_engine, discovered);
    cleanup_orphans_before_prune(tx, conn_id, db_engine);

    const char* vals[] = {conn_id.c_str(), db_engine.c_str()};
    const std::string discovered_match =
        db_engine == "mongodb"
            ? R"(d.source_database = c.source_database
              AND d.source_table = c.source_table
              AND d.source_schema = COALESCE(NULLIF(c.source_schema, ''), c.source_database))"
            : R"(d.source_database = c.source_database
              AND d.source_schema = c.source_schema
              AND d.source_table = c.source_table)";
    const std::string delete_sql =
        "WITH deleted AS ("
        "  DELETE FROM cdc_catalog.catalog c"
        "  WHERE c.conn_id = $1"
        "    AND c.db_engine = $2::cdc_catalog.db_engine"
        "    AND NOT EXISTS ("
        "      SELECT 1 FROM tmp_catalog_discovered d"
        "      WHERE " +
        discovered_match +
        "    )"
        "  RETURNING 1"
        ") SELECT COUNT(*)::int FROM deleted";
    return tx.exec_params_count(delete_sql.c_str(), 2, vals);
}

void upsert_mssql_catalog_objects(
    PgTxn& tx,
    const std::string& conn_id,
    const std::vector<MssqlCatalogObject>& batch) {
    static const char* kSql = R"(
        INSERT INTO cdc_catalog.catalog (
            conn_id, db_engine, source_database, source_schema, source_table,
            engine_meta, has_pk, pk_columns
        ) VALUES ($1, 'mssql'::cdc_catalog.db_engine, $2, $3, $4, $5::jsonb, $6, $7)
        ON CONFLICT (conn_id, db_engine, source_database, source_schema, source_table)
        DO UPDATE SET
            engine_meta = EXCLUDED.engine_meta,
            has_pk = EXCLUDED.has_pk,
            pk_columns = EXCLUDED.pk_columns,
            updated_at = now()
    )";

    for (const auto& obj : batch) {
        nlohmann::json meta = nlohmann::json::object();
        if (!obj.capture_instance.empty()) {
            meta["capture_instance"] = obj.capture_instance;
        }
        const std::string meta_str = meta.dump();
        const std::string has_pk_str = obj.has_pk ? "true" : "false";
        const char* pk_val = obj.has_pk && !obj.pk_columns.empty() ? obj.pk_columns.c_str() : nullptr;
        const char* vals[] = {
            conn_id.c_str(),
            obj.key.source_database.c_str(),
            obj.key.source_schema.c_str(),
            obj.key.source_table.c_str(),
            meta_str.c_str(),
            has_pk_str.c_str(),
            pk_val};
        tx.exec_params(kSql, 7, vals);
    }
}

void upsert_catalog_objects(
    PgTxn& tx,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::vector<CatalogObjectKey>& batch) {
    static const char* kSql = R"(
        INSERT INTO cdc_catalog.catalog (
            conn_id, db_engine, source_database, source_schema, source_table
        ) VALUES ($1, $2::cdc_catalog.db_engine, $3, $4, $5)
        ON CONFLICT (conn_id, db_engine, source_database, source_schema, source_table)
        DO UPDATE SET updated_at = now()
    )";
    static const char* kMongoSql = R"(
        INSERT INTO cdc_catalog.catalog (
            conn_id, db_engine, source_database, source_schema, source_table,
            has_pk, pk_columns
        ) VALUES ($1, 'mongodb'::cdc_catalog.db_engine, $2, $2, $3, true, 'mongo_id')
        ON CONFLICT (conn_id, db_engine, source_database, source_schema, source_table)
        DO UPDATE SET
            source_schema = EXCLUDED.source_schema,
            has_pk = true,
            pk_columns = 'mongo_id',
            updated_at = now()
    )";

    for (const auto& key : batch) {
        if (db_engine == "mongodb") {
            const char* vals[] = {
                conn_id.c_str(),
                key.source_database.c_str(),
                key.source_table.c_str()};
            tx.exec_params(kMongoSql, 3, vals);
            continue;
        }
        const char* vals[] = {
            conn_id.c_str(),
            db_engine.c_str(),
            key.source_database.c_str(),
            key.source_schema.c_str(),
            key.source_table.c_str()};
        tx.exec_params(kSql, 5, vals);
    }
}

int sync_mariadb_primary_keys(
    PgTxn& tx,
    const MariaDbSource& source,
    const std::map<TableKey, std::string>& pk_map,
    const std::vector<CatalogObjectKey>& discovered) {
    static const char* kUpdate = R"(
        UPDATE cdc_catalog.catalog
        SET has_pk = true, pk_columns = $1, updated_at = now()
        WHERE conn_id = $2
          AND db_engine = 'mariadb'
          AND source_database = ''
          AND source_schema = $3
          AND source_table = $4
    )";

    for (const auto& key : discovered) {
        const TableKey table_key{key.source_schema, key.source_table};
        auto it = pk_map.find(table_key);
        if (it != pk_map.end() && !it->second.empty()) {
            const char* vals[] = {
                it->second.c_str(), source.conn_id.c_str(), key.source_schema.c_str(), key.source_table.c_str()};
            tx.exec_params(kUpdate, 4, vals);
        } else {
            const char* vals[] = {source.conn_id.c_str(), key.source_schema.c_str(), key.source_table.c_str()};
            tx.exec_params(
                "UPDATE cdc_catalog.catalog"
                " SET has_pk = false, pk_columns = NULL, updated_at = now()"
                " WHERE conn_id = $1 AND db_engine = 'mariadb' AND source_database = ''"
                " AND source_schema = $2 AND source_table = $3",
                3,
                vals);
        }
    }
    return static_cast<int>(discovered.size());
}

std::pair<int, int> fetch_catalog_counts(PgConn& pg) {
    PgTxn tx(pg);
    const int total = tx.exec_count("SELECT COUNT(*)::int FROM cdc_catalog.catalog");
    const int active = tx.exec_count("SELECT COUNT(*)::int FROM cdc_catalog.catalog WHERE active = true");
    return {total, active};
}

#ifdef HAVE_FREETDS

bool mssql_db_filter_is_all(const std::string& database) {
    return database.empty() || database == "master";
}

std::vector<std::string> list_mssql_target_databases(MssqlConn& mssql, const std::string& database_filter) {
    if (!mssql_db_filter_is_all(database_filter)) {
        return {database_filter};
    }

    const MssqlQueryResult result = mssql.query(
        "SELECT name FROM sys.databases "
        "WHERE database_id > 4 AND state_desc = N'ONLINE' AND is_cdc_enabled = 1 "
        "ORDER BY name");
    std::vector<std::string> out;
    out.reserve(result.rows.size());
    for (const auto& row : result.rows) {
        if (!row.empty() && !row[0].text.empty()) {
            out.push_back(row[0].text);
        }
    }
    return out;
}

std::string fetch_mssql_primary_key(MssqlConn& mssql, const std::string& schema, const std::string& table) {
    const std::string sql =
        "SELECT c.name FROM sys.indexes i "
        "INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        "INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        "INNER JOIN sys.tables t ON i.object_id = t.object_id "
        "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE i.is_primary_key = 1 AND s.name = '" + schema + "' AND t.name = '" + table + "' "
        "ORDER BY ic.key_ordinal";
    const MssqlQueryResult result = mssql.query(sql);
    std::ostringstream pk;
    for (const auto& row : result.rows) {
        if (row.empty() || row[0].text.empty()) {
            continue;
        }
        if (pk.tellp() > 0) {
            pk << ',';
        }
        pk << row[0].text;
    }
    return pk.str();
}

std::vector<MssqlCatalogObject> fetch_mssql_objects(MssqlConn& mssql, const std::string& database_filter) {
    std::vector<MssqlCatalogObject> out;
    const auto databases = list_mssql_target_databases(mssql, database_filter);

    for (const auto& db : databases) {
        mssql.use_database(db);

        const MssqlQueryResult cdc_flag = mssql.query(
            "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()");
        const bool cdc_enabled =
            !cdc_flag.rows.empty() && !cdc_flag.rows[0].empty() && cdc_flag.rows[0][0].text == "1";

        if (!cdc_enabled) {
            continue;
        }

        const MssqlQueryResult cdc_tables = mssql.query(
            "SELECT ct.capture_instance, "
            "OBJECT_SCHEMA_NAME(ct.source_object_id) AS schema_name, "
            "OBJECT_NAME(ct.source_object_id) AS table_name "
            "FROM cdc.change_tables ct "
            "ORDER BY schema_name, table_name");
        for (const auto& row : cdc_tables.rows) {
            if (row.size() < 3 || row[1].text.empty() || row[2].text.empty()) {
                continue;
            }
            const std::string capture_instance = trim_mssql_text(row[0].text);
            if (capture_instance.empty()) {
                continue;
            }
            MssqlCatalogObject obj;
            obj.key = {db, trim_mssql_text(row[1].text), trim_mssql_text(row[2].text)};
            obj.capture_instance = capture_instance;
            obj.pk_columns = fetch_mssql_primary_key(mssql, obj.key.source_schema, obj.key.source_table);
            obj.has_pk = !obj.pk_columns.empty();
            out.push_back(std::move(obj));
        }
    }
    return out;
}

CatalogSyncStats run_mssql_catalog_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::vector<MssqlCatalogObject>& discovered) {
    std::vector<CatalogObjectKey> keys;
    keys.reserve(discovered.size());
    for (const auto& obj : discovered) {
        keys.push_back(obj.key);
    }

    const auto run_start = std::chrono::steady_clock::now();
    PgConn pg(cfg.datasync.conn_string());

    RuntimeConfig runtime;
    runtime.reload(pg.raw);
    const std::size_t chunk_size = runtime.get_size_t("catalog_chunk_size", 500, "catalog", conn_id);
    const int batch_sleep_ms = runtime.get_int("catalog_batch_sleep_ms", 200, "catalog", conn_id);

    CatalogSyncStats stats;
    stats.tables_discovered = static_cast<int>(discovered.size());

    if (discovered.empty()) {
        log_checkpoint(
            log_pg,
            LogLevel::Warning,
            batch_id,
            conn_id,
            "mssql scan returned zero objects; pruning stale catalog rows",
            nlohmann::json::object());
    }

    {
        PgTxn tx(pg);
        std::vector<MssqlCatalogObject> batch;
        batch.reserve(chunk_size);

        for (const auto& obj : discovered) {
            batch.push_back(obj);
            if (batch.size() >= chunk_size) {
                upsert_mssql_catalog_objects(tx, conn_id, batch);
                stats.tables_upserted += static_cast<int>(batch.size());
                stats.pk_rows_updated += static_cast<int>(batch.size());
                batch.clear();
                sleep_ms(batch_sleep_ms);
            }
        }
        if (!batch.empty()) {
            upsert_mssql_catalog_objects(tx, conn_id, batch);
            stats.tables_upserted += static_cast<int>(batch.size());
            stats.pk_rows_updated += static_cast<int>(batch.size());
        }

        stats.tables_pruned = prune_missing_objects(tx, conn_id, "mssql", keys);
    }

    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        conn_id,
        "catalog upsert and prune completed",
        {{"db_engine", "mssql"}, {"upserted", stats.tables_upserted}, {"pruned", stats.tables_pruned}});

    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        conn_id,
        "pk sync completed",
        {{"pk_updated", stats.pk_rows_updated}});

    auto [total, active] = fetch_catalog_counts(pg);
    stats.catalog_total = total;
    stats.catalog_active = active;

    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        conn_id,
        "discover conn completed",
        {{"db_engine", "mssql"},
         {"tables_discovered", stats.tables_discovered},
         {"tables_upserted", stats.tables_upserted},
         {"tables_pruned", stats.tables_pruned},
         {"pk_updated", stats.pk_rows_updated},
         {"catalog_total", stats.catalog_total},
         {"catalog_active", stats.catalog_active},
         {"duration_ms", elapsed_ms(run_start)}});

    return stats;
}

#endif

#ifdef HAVE_MONGOC

bool is_mongo_system_db(const std::string& name) {
    return name == "admin" || name == "config" || name == "local";
}

std::vector<CatalogObjectKey> fetch_mongo_objects(MongoConn& mongo, const std::string& db_filter) {
    bson_error_t error{};
    char** db_names = mongoc_client_get_database_names_with_opts(mongo.client, nullptr, &error);
    if (!db_names) {
        throw std::runtime_error(std::string("MongoDB list databases failed: ") + error.message);
    }

    std::vector<CatalogObjectKey> out;
    for (std::size_t i = 0; db_names[i] != nullptr; ++i) {
        const std::string db = db_names[i];
        if (is_mongo_system_db(db)) {
            continue;
        }
        if (!db_filter.empty() && db != db_filter) {
            continue;
        }

        bson_error_t coll_error{};
        char** collections = mongoc_database_get_collection_names_with_opts(
            mongoc_client_get_database(mongo.client, db.c_str()), nullptr, &coll_error);
        if (!collections) {
            bson_strfreev(db_names);
            throw std::runtime_error(
                std::string("MongoDB list collections failed for ") + db + ": " + coll_error.message);
        }
        for (std::size_t j = 0; collections[j] != nullptr; ++j) {
            // source_schema = source_database (matches upsert kMongoSql and reconcile/stats keys)
            out.push_back({db, db, collections[j]});
        }
        bson_strfreev(collections);
    }
    bson_strfreev(db_names);
    return out;
}

#endif

CatalogSyncStats run_catalog_sync(
    const AppConfig& cfg,
    PGconn* log_pg,
    const std::string& batch_id,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::vector<CatalogObjectKey>& discovered,
    bool sync_pk,
    const MariaDbSource* mariadb_source,
    const std::map<TableKey, std::string>* pk_map) {
    const auto run_start = std::chrono::steady_clock::now();
    PgConn pg(cfg.datasync.conn_string());

    RuntimeConfig runtime;
    runtime.reload(pg.raw);
    const std::size_t chunk_size = runtime.get_size_t("catalog_chunk_size", 500, "catalog", conn_id);
    const int batch_sleep_ms = runtime.get_int("catalog_batch_sleep_ms", 200, "catalog", conn_id);

    CatalogSyncStats stats;
    stats.tables_discovered = static_cast<int>(discovered.size());

    if (discovered.empty()) {
        log_checkpoint(
            log_pg,
            LogLevel::Warning,
            batch_id,
            conn_id,
            db_engine + " scan returned zero objects; pruning stale catalog rows",
            nlohmann::json::object());
    }

    {
        PgTxn tx(pg);
        std::vector<CatalogObjectKey> batch;
        batch.reserve(chunk_size);

        for (const auto& key : discovered) {
            batch.push_back(key);
            if (batch.size() >= chunk_size) {
                upsert_catalog_objects(tx, conn_id, db_engine, batch);
                stats.tables_upserted += static_cast<int>(batch.size());
                batch.clear();
                sleep_ms(batch_sleep_ms);
            }
        }
        if (!batch.empty()) {
            upsert_catalog_objects(tx, conn_id, db_engine, batch);
            stats.tables_upserted += static_cast<int>(batch.size());
        }

        stats.tables_pruned = prune_missing_objects(tx, conn_id, db_engine, discovered);
        if (sync_pk && mariadb_source && pk_map) {
            stats.pk_rows_updated = sync_mariadb_primary_keys(tx, *mariadb_source, *pk_map, discovered);
        }
    }

    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        conn_id,
        "catalog upsert and prune completed",
        {{"db_engine", db_engine}, {"upserted", stats.tables_upserted}, {"pruned", stats.tables_pruned}});

    if (sync_pk) {
        log_checkpoint(
            log_pg,
            LogLevel::Info,
            batch_id,
            conn_id,
            "pk sync completed",
            {{"pk_updated", stats.pk_rows_updated}});
    }

    auto [total, active] = fetch_catalog_counts(pg);
    stats.catalog_total = total;
    stats.catalog_active = active;

    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        conn_id,
        "discover conn completed",
        {{"db_engine", db_engine},
         {"tables_discovered", stats.tables_discovered},
         {"tables_upserted", stats.tables_upserted},
         {"tables_pruned", stats.tables_pruned},
         {"pk_updated", stats.pk_rows_updated},
         {"catalog_total", stats.catalog_total},
         {"catalog_active", stats.catalog_active},
         {"duration_ms", elapsed_ms(run_start)}});

    return stats;
}

}  // namespace

CatalogSyncStats sync_mariadb_catalog(
    const AppConfig& cfg,
    const MariaDbSource& source,
    PGconn* log_pg,
    const std::string& batch_id) {
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "discover conn started",
        {{"db_engine", "mariadb"}, {"host", source.host}, {"port", source.port}, {"schema_filter", source.db_name}});

    MariaDbConn mariadb(source);
    const MariaDbPreflightResult preflight = check_mariadb_cdc_ready(mariadb.handle);
    for (const auto& w : preflight.warnings) {
        log_checkpoint(
            log_pg,
            LogLevel::Warning,
            batch_id,
            source.conn_id,
            "mariadb cdc preflight warning",
            {{"detail", w}});
    }
    if (!preflight.ok) {
        nlohmann::json err_ctx = nlohmann::json::array();
        for (const auto& e : preflight.errors) {
            err_ctx.push_back(e);
        }
        log_checkpoint(
            log_pg,
            LogLevel::Error,
            batch_id,
            source.conn_id,
            "mariadb cdc preflight failed",
            {{"errors", err_ctx}});
        throw std::runtime_error("MariaDB CDC preflight failed for " + source.conn_id);
    }

    const auto discovered = fetch_mariadb_objects(mariadb.handle, source.db_name);
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "mariadb tables scanned",
        {{"tables_discovered", discovered.size()}});

    const auto pk_map = fetch_mariadb_primary_keys(mariadb.handle, source.db_name);
    return run_catalog_sync(
        cfg, log_pg, batch_id, source.conn_id, "mariadb", discovered, true, &source, &pk_map);
}

CatalogSyncStats sync_mssql_catalog(
    const AppConfig& cfg,
    const MssqlSource& source,
    PGconn* log_pg,
    const std::string& batch_id) {
#ifndef HAVE_FREETDS
    (void)cfg;
    (void)source;
    log_checkpoint(
        log_pg,
        LogLevel::Warning,
        batch_id,
        source.conn_id,
        "mssql discover skipped: rebuild with FreeTDS",
        nlohmann::json::object());
    return {};
#else
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "discover conn started",
        {{"db_engine", "mssql"}, {"host", source.host}, {"port", source.port}, {"database", source.db_name}});

    MssqlConn mssql(source);
        if (!mssql_db_filter_is_all(source.db_name)) {
        mssql.use_database(source.db_name.empty() ? "master" : source.db_name);
        const MssqlQueryResult cdc_flag = mssql.query(
            "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()");
        const bool cdc_enabled =
            !cdc_flag.rows.empty() && !cdc_flag.rows[0].empty() && cdc_flag.rows[0][0].text == "1";
        if (!cdc_enabled) {
            log_checkpoint(
                log_pg,
                LogLevel::Warning,
                batch_id,
                source.conn_id,
                "mssql discover skipped: database has CDC disabled",
                {{"database", source.db_name}});
            return {};
        }
    }

    const auto discovered = fetch_mssql_objects(mssql, source.db_name);
    if (discovered.empty()) {
        log_checkpoint(
            log_pg,
            LogLevel::Warning,
            batch_id,
            source.conn_id,
            "mssql discover empty scan",
            {{"database_filter", source.db_name.empty() ? "cdc_enabled_databases" : source.db_name}});
    }
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "mssql cdc tables scanned",
        {{"tables_discovered", discovered.size()},
         {"database_filter", source.db_name.empty() ? "cdc_enabled_databases" : source.db_name},
         {"scan_mode", "cdc_change_tables_only"}});

    return run_mssql_catalog_sync(cfg, log_pg, batch_id, source.conn_id, discovered);
#endif
}

CatalogSyncStats sync_mongo_catalog(
    const AppConfig& cfg,
    const MongoSource& source,
    PGconn* log_pg,
    const std::string& batch_id) {
#ifndef HAVE_MONGOC
    (void)cfg;
    log_checkpoint(
        log_pg,
        LogLevel::Warning,
        batch_id,
        source.conn_id,
        "mongo discover skipped: rebuild with libmongoc",
        nlohmann::json::object());
    return {};
#else
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "discover conn started",
        {{"db_engine", "mongodb"}, {"host", source.host}, {"port", source.port}, {"database", source.db_name}});

    MongoConn mongo(source);
    const auto preflight = check_mongo_cdc_ready(mongo, source);
    if (!preflight.ok) {
        nlohmann::json err_ctx = nlohmann::json::array();
        for (const auto& e : preflight.errors) {
            err_ctx.push_back(e);
        }
        log_checkpoint(
            log_pg,
            LogLevel::Error,
            batch_id,
            source.conn_id,
            "mongo CDC preflight failed",
            {{"errors", err_ctx}});
        throw std::runtime_error("mongo CDC preflight failed for " + source.conn_id);
    }
    for (const auto& warning : preflight.warnings) {
        log_checkpoint(
            log_pg,
            LogLevel::Warning,
            batch_id,
            source.conn_id,
            "mongo CDC preflight warning",
            {{"warning", warning}});
    }

    const auto discovered = fetch_mongo_objects(mongo, source.db_name);
    log_checkpoint(
        log_pg,
        LogLevel::Info,
        batch_id,
        source.conn_id,
        "mongo collections scanned",
        {{"tables_discovered", discovered.size()}});

    return run_catalog_sync(cfg, log_pg, batch_id, source.conn_id, "mongodb", discovered, false, nullptr, nullptr);
#endif
}

int sync_all_catalogs(const AppConfig& cfg, PGconn* log_pg, const std::string& batch_id) {
    int failures = 0;

    for (const auto& source : cfg.mariadb_sources) {
        try {
            sync_mariadb_catalog(cfg, source, log_pg, batch_id);
        } catch (const std::exception& ex) {
            failures += 1;
            log_checkpoint(
                log_pg,
                LogLevel::Error,
                batch_id,
                source.conn_id,
                "discover conn failed",
                {{"db_engine", "mariadb"}, {"error", ex.what()}});
        }
    }

    for (const auto& source : cfg.mssql_sources) {
        try {
            sync_mssql_catalog(cfg, source, log_pg, batch_id);
        } catch (const std::exception& ex) {
            failures += 1;
            log_checkpoint(
                log_pg,
                LogLevel::Error,
                batch_id,
                source.conn_id,
                "discover conn failed",
                {{"db_engine", "mssql"}, {"error", ex.what()}});
        }
    }

    for (const auto& source : cfg.mongo_sources) {
        try {
            sync_mongo_catalog(cfg, source, log_pg, batch_id);
        } catch (const std::exception& ex) {
            failures += 1;
            log_checkpoint(
                log_pg,
                LogLevel::Error,
                batch_id,
                source.conn_id,
                "discover conn failed",
                {{"db_engine", "mongodb"}, {"error", ex.what()}});
        }
    }

    return failures;
}

int fetch_catalog_headline_counts(const AppConfig& cfg, int& total_out, int& active_out) {
    PgConn pg(cfg.datasync.conn_string());
    auto [total, active] = fetch_catalog_counts(pg);
    total_out = total;
    active_out = active;
    return 0;
}
