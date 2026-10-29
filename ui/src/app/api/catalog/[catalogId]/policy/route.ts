import { NextResponse } from "next/server";
import { mutate, query } from "@/lib/db";

type RouteParams = { params: Promise<{ catalogId: string }> };

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

function asMaskMap(value: unknown): Record<string, string> | null {
  if (value == null) return {};
  if (typeof value !== "object" || Array.isArray(value)) return null;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (typeof v !== "string") return null;
    const key = k.trim();
    if (!key) continue;
    out[key] = v.trim() || "hash";
  }
  return out;
}

/** U4: column_include / column_exclude / column_mask on catalog.engine_meta. */
export async function GET(_request: Request, { params }: RouteParams) {
  const id = Number((await params).catalogId);
  if (!Number.isFinite(id) || id <= 0) {
    return NextResponse.json({ error: "Invalid catalog id" }, { status: 400 });
  }

  const result = await query<{
    catalog_id: number;
    conn_id: string;
    source_schema: string;
    source_table: string;
    engine_meta: Record<string, unknown>;
  }>(
    `
    SELECT catalog_id, conn_id, source_schema, source_table,
           COALESCE(engine_meta, '{}'::jsonb) AS engine_meta
    FROM cdc_catalog.catalog
    WHERE catalog_id = $1
  `,
    [id],
  );

  if (!result.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }
  if (!result.rows[0]) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  const meta = result.rows[0].engine_meta ?? {};
  return NextResponse.json({
    catalog_id: result.rows[0].catalog_id,
    conn_id: result.rows[0].conn_id,
    source_schema: result.rows[0].source_schema,
    source_table: result.rows[0].source_table,
    column_include: Array.isArray(meta.column_include) ? meta.column_include : [],
    column_exclude: Array.isArray(meta.column_exclude) ? meta.column_exclude : [],
    column_mask:
      meta.column_mask && typeof meta.column_mask === "object"
        ? meta.column_mask
        : {},
  });
}

export async function PUT(request: Request, { params }: RouteParams) {
  const id = Number((await params).catalogId);
  if (!Number.isFinite(id) || id <= 0) {
    return NextResponse.json({ error: "Invalid catalog id" }, { status: 400 });
  }

  let body: {
    column_include?: unknown;
    column_exclude?: unknown;
    column_mask?: unknown;
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

  const include = asStringArray(body.column_include);
  const exclude = asStringArray(body.column_exclude);
  const mask = asMaskMap(body.column_mask);
  if (include == null || exclude == null || mask == null) {
    return NextResponse.json(
      { error: "Invalid column_include / column_exclude / column_mask" },
      { status: 400 },
    );
  }

  const existing = await query<{
    engine_meta: Record<string, unknown>;
    conn_id: string;
    source_schema: string;
    source_table: string;
  }>(
    `
    SELECT COALESCE(engine_meta, '{}'::jsonb) AS engine_meta,
           conn_id, source_schema, source_table
    FROM cdc_catalog.catalog WHERE catalog_id = $1
  `,
    [id],
  );
  if (!existing.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }
  if (!existing.rows[0]) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  const meta = { ...(existing.rows[0].engine_meta ?? {}) };
  if (include.length > 0) meta.column_include = include;
  else delete meta.column_include;
  if (exclude.length > 0) meta.column_exclude = exclude;
  else delete meta.column_exclude;
  if (Object.keys(mask).length > 0) meta.column_mask = mask;
  else delete meta.column_mask;

  const updated = await mutate<{ catalog_id: number }>(
    `
    UPDATE cdc_catalog.catalog
    SET engine_meta = $2::jsonb, updated_at = now()
    WHERE catalog_id = $1
    RETURNING catalog_id
  `,
    [id, JSON.stringify(meta)],
  );

  if (!updated.ok) {
    return NextResponse.json({ error: updated.error }, { status: 500 });
  }

  await mutate(
    `
    INSERT INTO cdc_catalog.logs (level, component, message, conn_id, source_schema, source_table, context)
    VALUES ('info', 'ui', 'column policy updated', $1, $2, $3, $4::jsonb)
  `,
    [
      existing.rows[0].conn_id,
      existing.rows[0].source_schema,
      existing.rows[0].source_table,
      JSON.stringify({ catalog_id: id, via: "ui" }),
    ],
  );

  return NextResponse.json({
    ok: true,
    catalog_id: id,
    column_include: include,
    column_exclude: exclude,
    column_mask: mask,
  });
}
