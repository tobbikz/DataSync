#pragma once

#include <libpq-fe.h>

/** Apply idempotent cdc_catalog DDL from embedded SQL on startup. */
void ensure_cdc_catalog_schema(PGconn* pg);

/** Apply lake helper functions (partition management) on the datalake DB. */
void ensure_lake_schema(PGconn* pg);
