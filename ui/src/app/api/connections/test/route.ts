import { NextResponse } from "next/server";
import type { DbEngine } from "@/lib/connection-types";
import {
  normalizeAlias,
  parseConnectionBody,
  runConnectionPreflight,
} from "@/lib/connection-api";
import { query } from "@/lib/db";

export async function POST(request: Request) {
  let body: Record<string, unknown>;

  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  let payload: Parameters<typeof runConnectionPreflight>[0];

  if (
    typeof body.alias === "string" &&
    body.alias.trim() &&
    (body.password === undefined || body.password === "")
  ) {
    const alias = normalizeAlias(body.alias);
    const existing = await query<{
      db_engine: string;
      host: string;
      port: number;
      db_name: string;
      username: string;
      password: string;
      extras: Record<string, unknown>;
    }>(
      `
      SELECT db_engine::text, host, port, db_name, username, password, extras
      FROM cdc_catalog.connections
      WHERE alias = $1
    `,
      [alias],
    );

    if (!existing.ok) {
      return NextResponse.json(
        { error: "Database unavailable" },
        { status: 503 },
      );
    }

    const row = existing.rows[0];
    if (!row) {
      return NextResponse.json({ error: "Connection not found" }, { status: 404 });
    }

    payload = {
      db_engine: row.db_engine as DbEngine,
      host: typeof body.host === "string" ? body.host.trim() : row.host,
      port: body.port !== undefined ? Number(body.port) : row.port,
      db_name:
        typeof body.db_name === "string" ? body.db_name.trim() : row.db_name,
      username:
        typeof body.username === "string" ? body.username.trim() : row.username,
      password: row.password,
      extras: (body.extras as Record<string, unknown> | undefined) ?? row.extras,
    };
  } else {
    const parsed = parseConnectionBody(
      body as Parameters<typeof parseConnectionBody>[0],
    );
    if (!parsed.ok) {
      return NextResponse.json({ error: parsed.error }, { status: 400 });
    }
    payload = parsed.value;
  }

  const preflight = await runConnectionPreflight(payload);

  return NextResponse.json({
    ok: preflight.ok,
    reasons: preflight.reasons,
  });
}
