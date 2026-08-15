#pragma once

#include <libpq-fe.h>

// Hourly batched prune (idle+age apply_batch_stats, then logs). Cap 1M rows per prune
// per hour. Safe to call every daemon cycle; no-ops if this CST hour already ran.
void maybe_run_scheduled_retention_maintenance(PGconn* log_pg);
