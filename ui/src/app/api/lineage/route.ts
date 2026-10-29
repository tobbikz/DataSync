import { NextRequest, NextResponse } from "next/server";
import { query } from "@/lib/db";

export async function GET(request: NextRequest) {
  const sp = request.nextUrl.searchParams;
  const limit = Math.min(Number(sp.get("limit") ?? "100") || 100, 500);
  const connId = sp.get("conn")?.trim() || null;

  const result = await query<{
    lineage_id: number;
    created_at: string;
    event_type: string;
    conn_id: string | null;
    catalog_id: number | null;
    job_name: string;
    run_id: string | null;
    inputs: unknown;
    outputs: unknown;
    context: Record<string, unknown>;
  }>(
    `
    SELECT lineage_id, created_at, event_type, conn_id, catalog_id,
           job_name, run_id, inputs, outputs, context
    FROM cdc_catalog.lineage_events
    WHERE ($1::text IS NULL OR conn_id = $1)
    ORDER BY created_at DESC
    LIMIT $2
  `,
    [connId, limit],
  );

  if (!result.ok) {
    return NextResponse.json(
      { error: result.error ?? "Database unavailable" },
      { status: 503 },
    );
  }

  return NextResponse.json({ items: result.rows });
}
