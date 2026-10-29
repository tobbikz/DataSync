import { NextResponse } from "next/server";
import { normalizeAlias, logConnectionEvent } from "@/lib/connection-api";
import { mutate, query } from "@/lib/db";

type RouteParams = { params: Promise<{ alias: string }> };

function asStringArray(value: unknown): string[] | null {
  if (value == null) return [];
  if (!Array.isArray(value)) return null;
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== "string") return null;
    const trimmed = item.trim();
    if (trimmed) out.push(trimmed);
  }
  return out;
}

/** U4: table_include / table_exclude (+ capture_online_ddl_helpers) on connections.extras. */
export async function GET(_request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  const result = await query<{ extras: Record<string, unknown> }>(
    `SELECT extras FROM cdc_catalog.connections WHERE alias = $1`,
    [alias],
  );
  if (!result.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }
  if (!result.rows[0]) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  const extras = result.rows[0].extras ?? {};
  return NextResponse.json({
    alias,
    table_include: Array.isArray(extras.table_include) ? extras.table_include : [],
    table_exclude: Array.isArray(extras.table_exclude) ? extras.table_exclude : [],
    capture_online_ddl_helpers: extras.capture_online_ddl_helpers === true,
  });
}

export async function PUT(request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  let body: {
    table_include?: unknown;
    table_exclude?: unknown;
    capture_online_ddl_helpers?: boolean;
    confirm?: boolean;
  };
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

  const include = asStringArray(body.table_include);
  const exclude = asStringArray(body.table_exclude);
  if (include == null || exclude == null) {
    return NextResponse.json(
      { error: "table_include / table_exclude must be string arrays" },
      { status: 400 },
    );
  }

  const existing = await query<{ extras: Record<string, unknown> }>(
    `SELECT extras FROM cdc_catalog.connections WHERE alias = $1`,
    [alias],
  );
  if (!existing.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }
  if (!existing.rows[0]) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  const extras = { ...(existing.rows[0].extras ?? {}) };
  if (include.length > 0) extras.table_include = include;
  else delete extras.table_include;
  if (exclude.length > 0) extras.table_exclude = exclude;
  else delete extras.table_exclude;
  if (body.capture_online_ddl_helpers === true) {
    extras.capture_online_ddl_helpers = true;
  } else {
    delete extras.capture_online_ddl_helpers;
  }

  const updated = await mutate<{ alias: string }>(
    `
    UPDATE cdc_catalog.connections
    SET extras = $2::jsonb, updated_at = now()
    WHERE alias = $1
    RETURNING alias
  `,
    [alias, JSON.stringify(extras)],
  );

  if (!updated.ok) {
    return NextResponse.json({ error: updated.error }, { status: 500 });
  }

  await logConnectionEvent(`Table filters updated via UI: ${alias}`, alias);

  return NextResponse.json({
    ok: true,
    alias,
    table_include: include,
    table_exclude: exclude,
    capture_online_ddl_helpers: body.capture_online_ddl_helpers === true,
  });
}
