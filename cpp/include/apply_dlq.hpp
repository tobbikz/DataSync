#pragma once

#include <libpq-fe.h>

#include <string>
#include <vector>

#include "kafka_apply_detail.hpp"

/**
 * Dead-letter queue for rows the apply cannot write.
 *
 * Before this existed, a single unwritable row failed its table's transaction and
 * quarantined the whole table on the first error. Worse, the quarantine was not merely a
 * pause: the next process start skipped that table's messages and committed their offsets,
 * so the events were lost with no record of what they contained.
 *
 * The queue splits that single outcome in two. An error caused by the *data* parks the
 * offending rows here and lets everything else through, so one bad row costs one row. An
 * error caused by the *environment* (a dropped connection, a deadlock) is not the row's
 * fault and keeps the old behaviour, because retrying is the correct response and parking
 * would discard a row that was never invalid.
 */
namespace apply_dlq {

/** Why an event could not be applied. Mirrors the `reason` check on `cdc_catalog.apply_dlq`. */
enum class Reason {
    /** Postgres rejected the row itself. */
    ApplyError,
    /** The event's target table never resolved, so there was nothing to write to. */
    UnresolvedTable,
};

/**
 * Whether an error is caused by the data rather than the environment.
 *
 * Only deterministic failures qualify: replaying them produces the same error forever, so
 * isolating and parking the row makes progress. A transient failure must never be parked —
 * the row is fine, and parking it would turn a recoverable hiccup into permanent data loss.
 */
bool error_is_deterministic(const std::string& sqlstate, const std::string& message);

/**
 * Parks one event. `error_message` is stored verbatim so triage sees what Postgres said.
 *
 * Returns false when the row could not be recorded, which the caller must treat as a
 * failure to park: dropping the event silently is the behaviour this module exists to end.
 */
bool park_event(
    PGconn* app_pg,
    const kafka_apply_detail::ApplyEvent& event,
    const std::string& conn_id,
    const std::string& db_engine,
    const std::string& source_schema,
    const std::string& source_table,
    const std::string& lake_schema,
    const std::string& lake_table,
    const std::string& batch_id,
    const std::string& sqlstate,
    const std::string& error_message,
    Reason reason);

/** Count of rows still parked for a table, used to surface the queue in health output. */
long long parked_count(PGconn* app_pg, long long catalog_id);

}  // namespace apply_dlq
