import { NextResponse } from "next/server";
import { parsePagination } from "@/lib/pagination";
import { mutate, query } from "@/lib/db";

export async function GET(request: Request) {
  const { page, limit, offset } = parsePagination(new URL(request.url).searchParams);

  const result = await query<{
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
    username: string;
    active: boolean;
    total_count: string;
  }>(`
    SELECT alias, db_engine::text, host, port, db_name, username, active,
           COUNT(*) OVER() AS total_count
    FROM cdc_catalog.connections
    ORDER BY alias
    LIMIT $1 OFFSET $2
  `, [limit, offset]);

  if (!result.ok) {
    return NextResponse.json({ items: [], total: 0, page, limit });
  }

  const total = result.rows.length
    ? Number(result.rows[0].total_count)
    : 0;

  const items = result.rows.map(({ total_count: _total, ...row }) => row);

  return NextResponse.json({ items, total, page, limit });
}

export async function POST(request: Request) {
  const {
    logConnectionEvent,
    parseConnectionBody,
    runConnectionPreflight,
  } = await import("@/lib/connection-api");

  let body: Record<string, unknown>;

  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const parsed = parseConnectionBody(body as Parameters<typeof parseConnectionBody>[0]);
  if (!parsed.ok) {
    return NextResponse.json({ error: parsed.error }, { status: 400 });
  }

  const payload = parsed.value;

  const preflight = await runConnectionPreflight(payload);
  if (!preflight.ok) {
    return NextResponse.json(
      {
        error: "Connection rejected — CDC prerequisites not met",
        reasons: preflight.reasons,
      },
      { status: 422 },
    );
  }

  const insert = await mutate<{ alias: string }>(
    `
    INSERT INTO cdc_catalog.connections (
      alias, db_engine, host, port, db_name, username, password, extras, active
    ) VALUES (
      $1, $2::cdc_catalog.db_engine, $3, $4, $5, $6, $7, $8::jsonb, $9
    )
    RETURNING alias
  `,
    [
      payload.alias,
      payload.db_engine,
      payload.host,
      payload.port,
      payload.db_name,
      payload.username,
      payload.password,
      JSON.stringify(payload.extras),
      payload.active,
    ],
  );

  if (!insert.ok) {
    const msg = insert.error.includes("duplicate key")
      ? `Connection alias already exists: ${payload.alias}`
      : insert.error;
    return NextResponse.json({ error: msg }, { status: insert.error.includes("duplicate key") ? 400 : 503 });
  }

  if (insert.rows.length === 0) {
    return NextResponse.json({ error: "Insert failed" }, { status: 500 });
  }

  await logConnectionEvent(`Connection created via UI: ${payload.alias}`, payload.alias);

  return NextResponse.json({ alias: insert.rows[0].alias }, { status: 201 });
}
