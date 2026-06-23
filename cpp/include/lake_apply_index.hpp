#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

/** Non-unique mirror apply index: dl_mir_{schema}_{table}_pk (truncated to 63 chars). */
std::string mirror_apply_pk_index_name(const std::string& schema, const std::string& table);

/** CREATE INDEX IF NOT EXISTS on lake business PK columns (idempotent per process). */
void ensure_mirror_apply_pk_index(
    PGconn* pg,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::vector<std::string>& pk_cols);

struct MirrorApplyIndexBackfillStats {
    int tables_seen{0};
    int indexes_created{0};
    int tables_skipped{0};
    int errors{0};
};

/** Ensure mirror apply PK indexes for catalog tables (optional conn_id filter). */
MirrorApplyIndexBackfillStats backfill_mirror_apply_pk_indexes(
    PGconn* app_pg,
    PGconn* lake_pg,
    const std::string& conn_id = {});
