#pragma once

#include <libpq-fe.h>

#include <string>

struct SchemaMigrateOptions {
    bool baseline{false};
    bool lake{false};
    bool incremental{true};
    bool diagnostics{false};
};

bool catalog_schema_exists(PGconn* pg);
bool lake_schema_exists(PGconn* pg);

int run_schema_migrate(PGconn* log_pg, PGconn* lake_pg, const SchemaMigrateOptions& options);

/** Idempotent cdc_catalog patches on process startup (daemon, catalog sync, etc.). */
void run_startup_schema_migrate(PGconn* log_pg);
