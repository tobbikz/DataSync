#pragma once

#include "config.hpp"

#include <libpq-fe.h>
#include <string>

struct CatalogSyncStats {
    int tables_discovered{0};
    int tables_upserted{0};
    int tables_pruned{0};
    int pk_rows_updated{0};
    int catalog_total{0};
    int catalog_active{0};
};

CatalogSyncStats sync_mariadb_catalog(
    const AppConfig& cfg,
    const MariaDbSource& source,
    PGconn* log_pg,
    const std::string& batch_id);

CatalogSyncStats sync_mssql_catalog(
    const AppConfig& cfg,
    const MssqlSource& source,
    PGconn* log_pg,
    const std::string& batch_id);

CatalogSyncStats sync_mongo_catalog(
    const AppConfig& cfg,
    const MongoSource& source,
    PGconn* log_pg,
    const std::string& batch_id);

/** Discover + prune all active connections. Returns failure count. */
int sync_all_catalogs(const AppConfig& cfg, PGconn* log_pg, const std::string& batch_id);

int fetch_catalog_headline_counts(const AppConfig& cfg, int& total_out, int& active_out);
