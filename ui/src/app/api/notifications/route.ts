import { NextRequest, NextResponse } from "next/server";
import { mutate, query } from "@/lib/db";

export async function GET(request: NextRequest) {
  const sp = request.nextUrl.searchParams;
  const unackedOnly = sp.get("unacked") === "1" || sp.get("unacked") === "true";
  const limit = Math.min(Number(sp.get("limit") ?? "100") || 100, 500);

  const result = await query<{
    notification_id: number;
    created_at: string;
    channel: string;
    severity: string;
    conn_id: string | null;
    catalog_id: number | null;
    title: string;
    body: string;
    context: Record<string, unknown>;
    acked_at: string | null;
  }>(
    `
    SELECT notification_id, created_at, channel, severity, conn_id, catalog_id,
           title, body, context, acked_at
    FROM cdc_catalog.notifications
    WHERE ($1::boolean IS FALSE OR acked_at IS NULL)
    ORDER BY created_at DESC
    LIMIT $2
  `,
    [unackedOnly, limit],
  );

  if (!result.ok) {
    return NextResponse.json(
      { error: result.error ?? "Database unavailable" },
      { status: 503 },
    );
  }

  const unacked = await query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM cdc_catalog.notifications WHERE acked_at IS NULL`,
  );

  return NextResponse.json({
    items: result.rows,
    unacked: Number(unacked.rows[0]?.n ?? 0),
  });
}

export async function PATCH(request: NextRequest) {
  let body: { ids?: number[]; ackAll?: boolean };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (body.ackAll) {
    const result = await mutate<{ notification_id: number }>(
      `
      UPDATE cdc_catalog.notifications
      SET acked_at = now()
      WHERE acked_at IS NULL
      RETURNING notification_id
    `,
    );
    if (!result.ok) {
      return NextResponse.json({ error: result.error }, { status: 500 });
    }
    return NextResponse.json({ ok: true, acked: result.rows.length });
  }

  const ids = (body.ids ?? []).map(Number).filter((n) => Number.isFinite(n) && n > 0);
  if (ids.length === 0) {
    return NextResponse.json({ error: "ids required" }, { status: 400 });
  }

  const result = await mutate<{ notification_id: number }>(
    `
    UPDATE cdc_catalog.notifications
    SET acked_at = now()
    WHERE notification_id = ANY($1::bigint[])
      AND acked_at IS NULL
    RETURNING notification_id
  `,
    [ids],
  );

  if (!result.ok) {
    return NextResponse.json({ error: result.error }, { status: 500 });
  }

  return NextResponse.json({ ok: true, acked: result.rows.length });
}
