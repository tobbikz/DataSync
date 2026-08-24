import { NextResponse } from "next/server";
import {
  buildOverviewQueueQuery,
  buildOverviewSummaryQuery,
} from "@/lib/catalog-query";
import type { CatalogRow, OverviewResponse } from "@/lib/catalog-types";
import { query } from "@/lib/db";

const EMPTY_SUMMARY: OverviewResponse["summary"] = {
  connections: 0,
  tables_active: 0,
  tables_cdc: 0,
  errors_24h: 0,
  rag_red: 0,
  rag_amber: 0,
  rag_green: 0,
  quarantined: 0,
  needs_full_load: 0,
};

function normalizeRow(row: CatalogRow): CatalogRow {
  return {
    ...row,
    kafka_lag: row.kafka_lag != null ? Number(row.kafka_lag) : null,
    capture_lag_seconds:
      row.capture_lag_seconds != null ? Number(row.capture_lag_seconds) : null,
    apply_lag_seconds:
      row.apply_lag_seconds != null ? Number(row.apply_lag_seconds) : null,
  };
}

export async function GET() {
  const summaryQuery = buildOverviewSummaryQuery();
  const queueQuery = buildOverviewQueueQuery(50);

  const [summaryResult, queueResult] = await Promise.all([
    query(summaryQuery.sql, summaryQuery.params),
    query<CatalogRow>(queueQuery.sql, queueQuery.params),
  ]);

  if (!summaryResult.ok || !queueResult.ok) {
    return NextResponse.json({
      summary: EMPTY_SUMMARY,
      queue: [],
    } satisfies OverviewResponse);
  }

  const summaryRow = summaryResult.rows[0] as OverviewResponse["summary"];

  const response: OverviewResponse = {
    summary: {
      connections: Number(summaryRow.connections ?? 0),
      tables_active: Number(summaryRow.tables_active ?? 0),
      tables_cdc: Number(summaryRow.tables_cdc ?? 0),
      errors_24h: Number(summaryRow.errors_24h ?? 0),
      rag_red: Number(summaryRow.rag_red ?? 0),
      rag_amber: Number(summaryRow.rag_amber ?? 0),
      rag_green: Number(summaryRow.rag_green ?? 0),
      quarantined: Number(summaryRow.quarantined ?? 0),
      needs_full_load: Number(summaryRow.needs_full_load ?? 0),
    },
    queue: queueResult.rows.map(normalizeRow),
  };

  return NextResponse.json(response);
}
