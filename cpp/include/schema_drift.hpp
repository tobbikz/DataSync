#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

/**
 * What the lake has to do to catch up with a source schema change.
 *
 * A rename is indistinguishable from a drop plus an add: the source exposes the current
 * column list, never the DDL that produced it. So a diff carrying both sides is treated as
 * ambiguous and resolved by reloading the table instead of guessing which new column
 * inherits the data of which old one.
 */
struct SchemaDriftPlan {
    /** In the source, missing from the lake. */
    std::vector<std::string> added;
    /** In the lake, gone from the source. */
    std::vector<std::string> removed;
    /** Adds and removes in the same diff: could be a rename, so the table is reloaded. */
    bool ambiguous{false};
    /** A PK column vanished: the apply key itself changed, never resolve that in place. */
    bool pk_column_removed{false};
    /** Source column list came back empty (permissions, transient failure): do nothing. */
    bool source_unavailable{false};

    bool needs_full_load() const { return ambiguous || pk_column_removed; }
    bool drops_in_place() const { return !removed.empty() && !needs_full_load(); }

    /** One-line motive for catalog.last_error and the logs; empty when nothing drifted. */
    std::string reason() const;
};

/** pk_cols comes from catalog.pk_columns: a dropped column is absent from source_cols by then. */
SchemaDriftPlan plan_schema_drift(
    const std::vector<std::string>& source_cols,
    const std::vector<std::string>& lake_cols,
    const std::vector<std::string>& pk_cols);

/** Removals safe to apply in place: PK columns carry the apply index and are reloaded instead. */
std::vector<std::string> droppable_columns(
    const SchemaDriftPlan& plan,
    const std::vector<std::string>& pk_cols);

/** Lake columns that mirror source data; the _dl_* bookkeeping ones are never drift candidates. */
std::vector<std::string> fetch_lake_data_columns(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& table);

int drop_lake_columns(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& columns);

/** Reason string for a lake type migration the engine refused to perform in place. */
std::string type_migration_drift_reason(
    const std::string& column,
    const std::string& from_type,
    const std::string& to_type);

/**
 * Flag one table for a full-load reboot, mirroring the binlog-purged path: the reload
 * rebuilds the table and CDC keeps capturing meanwhile. Returns true when the row was
 * flagged (a table already awaiting a reload is left alone).
 */
bool request_full_load_reboot(
    PGconn* catalog_pg,
    long long catalog_id,
    const std::string& reason);
