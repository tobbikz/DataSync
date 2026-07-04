-- One-time ops: clear reconciliation dashboard noise.
-- After deploy with single-cycle reconcile (reconciliation_run.conn_id = '*'), run once.
-- Safe for lake data and cdc_catalog.catalog; only wipes reconcile history.

BEGIN;

TRUNCATE cdc_catalog.reconciliation_run RESTART IDENTITY CASCADE;

COMMIT;

-- CASCADE truncates cdc_catalog.reconciliation_result (FK on run_id).
-- New cycles create ONE run row (conn_id='*') covering all connections.
