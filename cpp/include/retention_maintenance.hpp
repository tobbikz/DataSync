#pragma once

#include <libpq-fe.h>

class RuntimeConfig;

// Advisory-lock + batched SQL prune once per day in the maintenance window (03:00 CST).
// Safe to call every daemon cycle; no-ops outside the window or when another runner holds the lock.
void maybe_run_scheduled_retention_maintenance(PGconn* log_pg, RuntimeConfig& runtime);
