import { mutate, query } from "./db";

export const SIGNAL_ACTIONS = [
  "execute_incremental",
  "execute_blocking",
  "pause",
  "resume",
  "stop",
  "reload_config",
  "log",
  "notify",
] as const;

export type SignalAction = (typeof SIGNAL_ACTIONS)[number];

export interface SnapshotSignal {
  signal_id: number;
  requested_at: string;
  updated_at: string;
  conn_id: string | null;
  catalog_id: number | null;
  action: string;
  status: string;
  payload: Record<string, unknown>;
  last_error: string | null;
}

export function isSignalAction(value: string): value is SignalAction {
  return (SIGNAL_ACTIONS as readonly string[]).includes(value);
}

export async function listSignals(limit = 50) {
  return query<SnapshotSignal>(
    `
    SELECT signal_id, requested_at, updated_at, conn_id, catalog_id,
           action, status, payload, last_error
    FROM cdc_catalog.snapshot_signal
    ORDER BY requested_at DESC
    LIMIT $1
  `,
    [limit],
  );
}

/** Mirror CLI `snapshot-signal`: enqueue + arm catalog for incremental/blocking. */
export async function enqueueSignal(input: {
  action: SignalAction;
  connId?: string;
  catalogId?: number;
  payload?: Record<string, unknown>;
}): Promise<
  { ok: true; signal_id: number } | { ok: false; error: string }
> {
  const payload = input.payload ?? {};
  const inserted = await mutate<{ signal_id: number }>(
    `
    INSERT INTO cdc_catalog.snapshot_signal (conn_id, catalog_id, action, status, payload)
    VALUES ($1, $2, $3, 'pending', $4::jsonb)
    RETURNING signal_id
  `,
    [
      input.connId ?? null,
      input.catalogId ?? null,
      input.action,
      JSON.stringify(payload),
    ],
  );

  if (!inserted.ok || !inserted.rows[0]) {
    return { ok: false, error: inserted.ok ? "insert failed" : inserted.error };
  }

  const signalId = Number(inserted.rows[0].signal_id);

  if (
    input.action === "execute_incremental" ||
    input.action === "execute_blocking"
  ) {
    if (input.catalogId) {
      const mode =
        input.action === "execute_incremental" ? "incremental" : "when_needed";
      await mutate(
        `
        UPDATE cdc_catalog.catalog SET
          needs_full_load = true,
          snapshot_mode = CASE WHEN $2 = 'incremental' THEN 'incremental' ELSE snapshot_mode END,
          engine_meta = COALESCE(engine_meta,'{}'::jsonb)
            || jsonb_build_object('snapshot_kind','incremental'),
          capture_during_full_load = true,
          status = 'pending',
          updated_at = now()
        WHERE catalog_id = $1::bigint
      `,
        [input.catalogId, mode],
      );
    } else if (input.connId) {
      await mutate(
        `
        UPDATE cdc_catalog.catalog SET
          needs_full_load = true,
          snapshot_mode = 'incremental',
          engine_meta = COALESCE(engine_meta,'{}'::jsonb)
            || jsonb_build_object('snapshot_kind','incremental'),
          capture_during_full_load = true,
          status = 'pending',
          updated_at = now()
        WHERE conn_id = $1 AND active = true AND has_pk = true
      `,
        [input.connId],
      );
    }
    await mutate(
      `UPDATE cdc_catalog.snapshot_signal
       SET status = 'running', updated_at = now()
       WHERE signal_id = $1`,
      [signalId],
    );
  }

  await mutate(
    `
    INSERT INTO cdc_catalog.logs (level, component, message, conn_id, context)
    VALUES ('info', 'ui', $1, $2, $3::jsonb)
  `,
    [
      `snapshot signal enqueued: ${input.action}`,
      input.connId ?? null,
      JSON.stringify({
        signal_id: signalId,
        action: input.action,
        catalog_id: input.catalogId ?? null,
        via: "ui",
      }),
    ],
  );

  return { ok: true, signal_id: signalId };
}
