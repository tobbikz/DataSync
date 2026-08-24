import { NextResponse } from "next/server";
import { normalizeAlias } from "@/lib/connection-api";
import {
  CONNECTION_CAPTURE_SQL,
  CONNECTION_CATALOG_SUMMARY_SQL,
  CONNECTION_HOURLY_METRICS_SQL,
  CONNECTION_RECENT_EVENTS_SQL,
} from "@/lib/connection-detail-query";
import { query } from "@/lib/db";

type RouteParams = { params: Promise<{ alias: string }> };

export async function GET(_request: Request, { params }: RouteParams) {
  const alias = normalizeAlias((await params).alias);
  if (!alias) {
    return NextResponse.json({ error: "Invalid alias" }, { status: 400 });
  }

  const conn = await query<{
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
    username: string;
    active: boolean;
    created_at: string;
    updated_at: string;
  }>(
    `
    SELECT alias, db_engine::text, host, port, db_name, username, active,
           created_at, updated_at
    FROM cdc_catalog.connections
    WHERE alias = $1
  `,
    [alias],
  );

  if (!conn.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }

  const row = conn.rows[0];
  if (!row) {
    return NextResponse.json({ error: "Connection not found" }, { status: 404 });
  }

  const [catalog, hourly, capture, recent] = await Promise.all([
    query<{
      tables_total: number;
      tables_active: number;
      cdc_ready: number;
      needs_full_load: number;
      success: number;
      failed: number;
    }>(CONNECTION_CATALOG_SUMMARY_SQL, [alias]),
    query<{
      bucket: string;
      events: string;
      kafka_lag: string;
      apply_lag: number;
    }>(CONNECTION_HOURLY_METRICS_SQL, [alias]),
    query<{
      status: string;
      capture_lag_seconds: number | null;
    }>(CONNECTION_CAPTURE_SQL, [alias]),
    query<{ events_5m: string }>(CONNECTION_RECENT_EVENTS_SQL, [alias]),
  ]);

  if (!catalog.ok || !hourly.ok || !capture.ok || !recent.ok) {
    return NextResponse.json({ error: "Database unavailable" }, { status: 503 });
  }

  const summary = catalog.rows[0] ?? {
    tables_total: 0,
    tables_active: 0,
    cdc_ready: 0,
    needs_full_load: 0,
    success: 0,
    failed: 0,
  };

  return NextResponse.json({
    connection: {
      alias: row.alias,
      db_engine: row.db_engine,
      host: row.host,
      port: row.port,
      db_name: row.db_name,
      username: row.username,
      active: row.active,
      created_at: row.created_at,
      updated_at: row.updated_at,
    },
    catalog: summary,
    capture: capture.rows[0] ?? null,
    hourly: hourly.rows.map((h) => ({
      bucket: h.bucket,
      events: Number(h.events),
      kafka_lag: Number(h.kafka_lag),
      apply_lag: h.apply_lag,
    })),
    events_5m: Number(recent.rows[0]?.events_5m ?? 0),
  });
}
