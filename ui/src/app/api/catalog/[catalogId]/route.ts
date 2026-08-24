import { NextResponse } from "next/server";
import { catalogDetailSql } from "@/lib/catalog-query";
import type { CatalogDetailResponse, CatalogRow } from "@/lib/catalog-types";
import {
  DISABLE_CATALOG_REPLICATION_SQL,
  ENABLE_CATALOG_REPLICATION_SQL,
  LOG_CATALOG_REPLICATION_SQL,
  type CatalogReplicationMode,
} from "@/lib/catalog-replication";
import { mutate, query } from "@/lib/db";

type DetailRow = CatalogRow & {
  has_pk: boolean | null;
  pk_columns: string | null;
  apply_status: string | null;
  kafka_topic: string | null;
  kafka_partition: number | null;
  kafka_offset: number | null;
  quarantined_at: string | null;
  last_error: string | null;
};

export async function GET(
  _request: Request,
  context: { params: Promise<{ catalogId: string }> },
) {
  const { catalogId } = await context.params;
  const id = Number(catalogId);
  if (!Number.isFinite(id) || id <= 0) {
    return NextResponse.json({ error: "Invalid catalog id" }, { status: 400 });
  }

  const result = await query<DetailRow>(catalogDetailSql, [id]);

  if (!result.ok || result.rows.length === 0) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  const row = result.rows[0];
  const {
    has_pk: _hasPk,
    pk_columns: _pkColumns,
    apply_status,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    quarantined_at,
    last_error,
    ...catalog
  } = row;

  const statsResult = await query<{
    logged_at: string;
    events_total: number | null;
    kafka_consumer_lag: number | null;
    apply_health_rag: string | null;
    health_reason: string | null;
  }>(
    `
    SELECT logged_at, events_total, kafka_consumer_lag, apply_health_rag, health_reason
    FROM cdc_catalog.apply_batch_stats
    WHERE catalog_id = $1
    ORDER BY logged_at DESC
    LIMIT 5
  `,
    [id],
  );

  const logsResult = await query<{
    logged_at: string;
    level: string;
    component: string;
    message: string;
  }>(
    `
    SELECT logged_at, level::text, component, message
    FROM cdc_catalog.logs
    WHERE conn_id = $1
      AND source_schema = $2
      AND source_table = $3
    ORDER BY logged_at DESC
    LIMIT 5
  `,
    [row.conn_id, row.source_schema, row.source_table],
  );

  const response: CatalogDetailResponse = {
    catalog: {
      ...catalog,
      kafka_lag: catalog.kafka_lag != null ? Number(catalog.kafka_lag) : null,
      capture_lag_seconds:
        catalog.capture_lag_seconds != null
          ? Number(catalog.capture_lag_seconds)
          : null,
      apply_lag_seconds:
        catalog.apply_lag_seconds != null
          ? Number(catalog.apply_lag_seconds)
          : null,
    },
    apply_position: apply_status
      ? {
          status: apply_status,
          kafka_topic,
          kafka_partition:
            kafka_partition != null ? Number(kafka_partition) : null,
          kafka_offset: kafka_offset != null ? Number(kafka_offset) : null,
          apply_lag_seconds:
            catalog.apply_lag_seconds != null
              ? Number(catalog.apply_lag_seconds)
              : null,
          last_applied_at: catalog.last_apply_at,
          last_error,
          quarantine_reason: catalog.quarantine_reason,
          quarantined_at,
        }
      : null,
    recent_stats: statsResult.ok
      ? statsResult.rows.map((s) => ({
          ...s,
          events_total: s.events_total != null ? Number(s.events_total) : null,
          kafka_consumer_lag:
            s.kafka_consumer_lag != null ? Number(s.kafka_consumer_lag) : null,
        }))
      : [],
    recent_logs: logsResult.ok ? logsResult.rows : [],
  };

  return NextResponse.json(response);
}

type ReplicationRow = {
  catalog_id: number;
  conn_id: string;
  source_schema: string;
  source_table: string;
  active: boolean;
  cdc_enabled: boolean;
  capture_during_full_load: boolean;
};

export async function PATCH(
  request: Request,
  context: { params: Promise<{ catalogId: string }> },
) {
  const { catalogId } = await context.params;
  const id = Number(catalogId);
  if (!Number.isFinite(id) || id <= 0) {
    return NextResponse.json({ error: "Invalid catalog id" }, { status: 400 });
  }

  let body: { mode?: CatalogReplicationMode };
  try {
    body = (await request.json()) as { mode?: CatalogReplicationMode };
  } catch {
    return NextResponse.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  if (body.mode !== "enable" && body.mode !== "disable") {
    return NextResponse.json(
      { error: 'mode must be "enable" or "disable"' },
      { status: 400 },
    );
  }

  const sql =
    body.mode === "enable"
      ? ENABLE_CATALOG_REPLICATION_SQL
      : DISABLE_CATALOG_REPLICATION_SQL;

  const result = await mutate<ReplicationRow>(sql, [id]);
  if (!result.ok) {
    return NextResponse.json({ error: result.error }, { status: 500 });
  }
  if (result.rows.length === 0) {
    return NextResponse.json({ error: "Catalog entry not found" }, { status: 404 });
  }

  const row = result.rows[0];
  const message =
    body.mode === "enable"
      ? "catalog replication enabled (active, cdc_enabled, capture_during_full_load)"
      : "catalog replication disabled";

  await mutate(LOG_CATALOG_REPLICATION_SQL, [
    message,
    row.conn_id,
    row.source_schema,
    row.source_table,
    JSON.stringify({
      catalog_id: row.catalog_id,
      active: row.active,
      cdc_enabled: row.cdc_enabled,
      capture_during_full_load: row.capture_during_full_load,
      via: "ui",
    }),
  ]);

  return NextResponse.json({
    ok: true,
    mode: body.mode,
    catalog: row,
  });
}
