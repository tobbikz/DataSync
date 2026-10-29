import { NextResponse } from "next/server";
import { normalizeAlias, logConnectionEvent } from "@/lib/connection-api";
import { mutate, query } from "@/lib/db";
import { enqueueSignal } from "@/lib/signals";

type RouteParams = { params: Promise<{ alias: string }> };

/** U5: pause/resume capture by toggling connections.active (+ cooperative signal). */
export async function POST(request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  let body: { action?: string; confirm?: boolean };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (!body.confirm) {
    return NextResponse.json(
      { error: "Confirmation required (confirm: true)" },
      { status: 400 },
    );
  }

  const action = body.action?.trim();
  if (action !== "pause" && action !== "resume") {
    return NextResponse.json(
      { error: 'action must be "pause" or "resume"' },
      { status: 400 },
    );
  }

  const existing = await query<{ alias: string; active: boolean }>(
    `SELECT alias, active FROM cdc_catalog.connections WHERE alias = $1`,
    [alias],
  );
  if (!existing.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }
  if (!existing.rows[0]) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  const active = action === "resume";
  const updated = await mutate<{ alias: string; active: boolean }>(
    `
    UPDATE cdc_catalog.connections
    SET active = $2,
        extras = COALESCE(extras, '{}'::jsonb)
          || jsonb_build_object('capture_paused', $3::boolean),
        updated_at = now()
    WHERE alias = $1
    RETURNING alias, active
  `,
    [alias, active, !active],
  );

  if (!updated.ok || !updated.rows[0]) {
    return NextResponse.json(
      { error: updated.ok ? "update failed" : updated.error },
      { status: 500 },
    );
  }

  const sig = await enqueueSignal({
    action: action === "pause" ? "pause" : "resume",
    connId: alias,
    payload: { via: "ui", scope: "capture" },
  });

  await logConnectionEvent(
    action === "pause"
      ? `Capture paused via UI: ${alias}`
      : `Capture resumed via UI: ${alias}`,
    alias,
  );

  await mutate(
    `
    INSERT INTO cdc_catalog.notifications
      (channel, severity, conn_id, title, body, context)
    VALUES (
      'capture',
      'info',
      $1,
      $2,
      $3,
      $4::jsonb
    )
  `,
    [
      alias,
      action === "pause" ? "Capture paused" : "Capture resumed",
      `Connection ${alias} set active=${active}`,
      JSON.stringify({
        via: "ui",
        action,
        signal_id: sig.ok ? sig.signal_id : null,
      }),
    ],
  );

  return NextResponse.json({
    ok: true,
    alias,
    active,
    action,
    signal_id: sig.ok ? sig.signal_id : null,
  });
}
