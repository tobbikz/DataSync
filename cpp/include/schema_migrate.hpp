#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

struct SchemaMigrateOptions {
    bool baseline{false};
    bool lake{false};
    bool incremental{true};
    bool diagnostics{false};
};

bool catalog_schema_exists(PGconn* pg);
bool lake_schema_exists(PGconn* pg);

int run_schema_migrate(PGconn* log_pg, PGconn* lake_pg, const SchemaMigrateOptions& options);

/** Idempotent cdc_catalog patches on process startup (daemon, kafka-apply, etc.). */
void run_startup_schema_migrate(PGconn* log_pg);

/** Required cdc_catalog tables after migration 052 (baseline post-046 + 050/051/052). */
constexpr int kExpectedCatalogTableCount = 14;

/** Return missing table/view names (empty = schema complete for current binary). */
std::vector<std::string> missing_catalog_schema_objects(PGconn* pg);

/** Fail with actionable error if required cdc_catalog objects are absent. */
void validate_catalog_schema(PGconn* pg);
