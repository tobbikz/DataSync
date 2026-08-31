#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

/**
 * SCD Type 2 history for a lake mirror table (opt-in per table via catalog.scd2_enabled).
 *
 * The mirror keeps holding the current state, exactly as before. Next to it lives
 * `<table>_history`, which keeps every version of every row with a validity interval, so a
 * query can ask what a row looked like at an arbitrary instant instead of only right now.
 *
 * The interval reuses the columns the lake already has rather than inventing a second
 * timestamp: `_dl_load_timestamp` **is** the version's valid-from, which is also why it is
 * both the partition key and the tail of the primary key, just like in the mirror. Only the
 * closing side needs new columns.
 */
namespace lake_history {

/** Suffix appended to the mirror table name. */
constexpr const char* kSuffix = "_history";

constexpr const char* kValidTo = "_scd_valid_to";
constexpr const char* kIsCurrent = "_scd_is_current";
constexpr const char* kIsDeleted = "_scd_is_deleted";

/** `<mirror>_history`, truncated to fit PostgreSQL's 63-byte identifier limit. */
std::string history_table_name(const std::string& mirror_table);

/**
 * Create the history table if missing and align its columns with the mirror.
 *
 * Column alignment runs on every call because the mirror gains columns at runtime (schema
 * drift, and Mongo infers new fields mid-batch); a history table one column behind would
 * silently drop that field from every version it stores.
 */
void ensure_history_table(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    int partition_months_ahead);

/**
 * Record the versions carried by an apply staging table.
 *
 * Closes whatever version was open for each key and appends **every** row in the staging
 * table, not just the surviving one: the mirror collapses repeated changes to the same key
 * within a batch, but discarding the intermediate versions is precisely what history is
 * supposed to prevent. Each version's valid-to is the valid-from of the next one, computed
 * with a window over the staging rows.
 *
 * `staging` must already hold the rows COPYed for the mirror, with `_dl_load_timestamp`
 * populated and unique per row.
 */
long long record_history_from_staging(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::string& staging,
    const std::vector<std::string>& all_cols,
    const std::vector<std::string>& pk_cols);

/**
 * Close the open version of each deleted key, marking it deleted rather than superseded.
 *
 * `pk_ts_lines` are CSV lines of the primary key columns followed by the event timestamp,
 * which becomes the valid-to of the version being closed.
 */
long long close_history_for_deletes(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    const std::vector<std::string>& pk_col_types,
    const std::vector<std::string>& pk_ts_lines);

/**
 * Seed history from the mirror after a full load.
 *
 * A reload does not erase history: the versions that were open get closed at `load_ts` and
 * the reloaded rows open new ones. Wiping instead would destroy the very thing the table was
 * opted into, and "this is what we held before the reload" is a legitimate answer.
 */
long long seed_history_from_mirror(
    PGconn* lake_pg,
    const std::string& schema,
    const std::string& mirror_table,
    const std::vector<std::string>& pk_cols,
    int partition_months_ahead);

/** Reads catalog.scd2_enabled; false when the row or the column is missing. */
bool scd2_enabled_for_catalog(PGconn* catalog_pg, long long catalog_id);

}  // namespace lake_history
