import { NextResponse } from "next/server";
import type { DbEngine } from "@/lib/connection-types";
import {
  logConnectionEvent,
  normalizeAlias,
  runConnectionPreflight,
} from "@/lib/connection-api";
import { mutate, query } from "@/lib/db";

type RouteParams = { params: Promise<{ alias: string }> };

export async function GET(_request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  const result = await query<{
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
    username: string;
    password: string;
    extras: Record<string, unknown>;
    active: boolean;
    created_at: string;
    updated_at: string;
  }>(
    `
    SELECT alias, db_engine::text, host, port, db_name, username, password,
           extras, active, created_at, updated_at
    FROM cdc_catalog.connections
    WHERE alias = $1
  `,
    [alias],
  );

  if (!result.ok) {
    return NextResponse.json(
      { error: "Database unavailable" },
      { status: 503 },
    );
  }

  const row = result.rows[0];
  if (!row) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  return NextResponse.json({
    alias: row.alias,
    db_engine: row.db_engine,
    host: row.host,
    port: row.port,
    db_name: row.db_name,
    username: row.username,
    has_password: row.password.length > 0,
    extras: row.extras ?? {},
    active: row.active,
    created_at: row.created_at,
    updated_at: row.updated_at,
  });
}

export async function PATCH(request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  let body: {
    host?: string;
    port?: number;
    db_name?: string;
    username?: string;
    password?: string;
    extras?: Record<string, unknown>;
    active?: boolean;
  };

  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const existing = await query<{
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
    username: string;
    password: string;
    extras: Record<string, unknown>;
    active: boolean;
  }>(
    `
    SELECT alias, db_engine::text, host, port, db_name, username, password, extras, active
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

  const host = body.host?.trim() ?? row.host;
  const port = body.port !== undefined ? Number(body.port) : row.port;
  const db_name = body.db_name?.trim() ?? row.db_name;
  const username = body.username?.trim() ?? row.username;
  const password =
    body.password !== undefined && body.password !== ""
      ? body.password
      : row.password;
  const extras = body.extras ?? row.extras ?? {};
  const active = true;

  if (!host || !db_name) {
    return NextResponse.json(
      { error: "Host and database are required" },
      { status: 400 },
    );
  }
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    return NextResponse.json({ error: "Invalid port" }, { status: 400 });
  }
  if (row.db_engine === "mongodb") {
    const rs = extras.replica_set;
    if (typeof rs !== "string" || !rs.trim()) {
      return NextResponse.json(
        { error: "MongoDB requires extras.replica_set" },
        { status: 400 },
      );
    }
  }

  const connectionChanged =
    host !== row.host ||
    port !== row.port ||
    db_name !== row.db_name ||
    username !== row.username ||
    password !== row.password ||
    JSON.stringify(extras) !== JSON.stringify(row.extras ?? {});

  if (active && (connectionChanged || !row.active)) {
    const preflight = await runConnectionPreflight({
      db_engine: row.db_engine as DbEngine,
      host,
      port,
      db_name,
      username,
      password,
      extras,
    });
    if (!preflight.ok) {
      return NextResponse.json(
        {
          error: "Connection rejected — CDC prerequisites not met",
          reasons: preflight.reasons,
        },
        { status: 422 },
      );
    }
  }

  const updated = await mutate<{ alias: string }>(
    `
    UPDATE cdc_catalog.connections
    SET host = $2,
        port = $3,
        db_name = $4,
        username = $5,
        password = $6,
        extras = $7::jsonb,
        active = $8,
        updated_at = now()
    WHERE alias = $1
    RETURNING alias
  `,
    [
      alias,
      host,
      port,
      db_name,
      username,
      password,
      JSON.stringify(extras),
      active,
    ],
  );

  if (!updated.ok) {
    return NextResponse.json({ error: updated.error }, { status: 400 });
  }

  await logConnectionEvent(`Connection updated via UI: ${alias}`, alias);

  return NextResponse.json({ alias });
}

export async function DELETE(_request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  const catalog = await query<{ n: string }>(
    `SELECT COUNT(*)::text AS n FROM cdc_catalog.catalog WHERE conn_id = $1`,
    [alias],
  );

  if (!catalog.ok) {
    return NextResponse.json(
      { error: "Database unavailable" },
      { status: 503 },
    );
  }

  const count = Number(catalog.rows[0]?.n ?? 0);
  if (count > 0) {
    return NextResponse.json(
      {
        error: `Cannot delete ${alias}: ${count} catalog table(s) still reference this connection`,
      },
      { status: 409 },
    );
  }

  const deleted = await mutate<{ alias: string }>(
    `DELETE FROM cdc_catalog.connections WHERE alias = $1 RETURNING alias`,
    [alias],
  );

  if (!deleted.ok) {
    return NextResponse.json({ error: deleted.error }, { status: 400 });
  }

  if (deleted.rows.length === 0) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  await logConnectionEvent(`Connection deleted via UI: ${alias}`, alias);

  return NextResponse.json({ alias, deleted: true });
}
