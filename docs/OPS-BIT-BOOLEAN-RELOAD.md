# Post-deploy: reload `communication_service.chat_surveys`

## Why

MariaDB `BIT(1)` columns mapped to Postgres `BOOLEAN` previously fell back to **`false`** for unrecognized wire forms (`b'1'`, `\x01`, etc.). After the BIT→BOOLEAN mapper fix, new CDC/full-load rows are correct, but **existing lake rows remain wrong** until reloaded.

P0 table (CSAT / chat surveys):

- Lake: `communication_service.chat_surveys`
- Validate row: `id = 478408` → `did_we_solve_your_problem` / `satisfied_with_service` must be `true`/`true` after reload (matches MariaDB `1`/`1`).

## After deploying DataSync with the boolean mapper fix

1. Run a **full load** (or reconcile) for `communication_service.chat_surveys` only, using the usual DataSync single-table full-load path for that conn/schema/table.
2. Confirm CDC apply is healthy for the same table so subsequent BIT updates stay correct.
3. Validate:

```sql
SELECT id, did_we_solve_your_problem, satisfied_with_service, _dl_load_timestamp
FROM communication_service.chat_surveys
WHERE id = 478408
ORDER BY _dl_load_timestamp DESC
LIMIT 1;
-- expect true / true
```

## Out of scope here

- Broader inventory of MariaDB `BIT(1)` columns via `information_schema` (follow-up).
- Do **not** invert CSAT Yes/No presentation logic; the lake booleans must match source.
