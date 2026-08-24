import { NextResponse } from "next/server";
import {
  OPS_APPLY_TAIL_SQL,
  OPS_CAPTURE_LAG_SQL,
  OPS_CATALOG_STATUS_SQL,
  OPS_ENGINE_MIX_SQL,
  OPS_ERRORS_BY_COMPONENT_SQL,
  OPS_EVENTS_HOURLY_SQL,
  OPS_GAP_EVENTS_SQL,
  OPS_KPIS_SQL,
  OPS_PIPELINE_TABLES_SQL,
  OPS_RECENT_ERRORS_SQL,
} from "@/lib/ops-queries";
import { query, mutate } from "@/lib/db";

export async function GET(request: Request) {
  const refresh = new URL(request.url).searchParams.get("refresh") === "1";

  if (refresh) {
    const views = [
      "cdc_catalog.mv_tab_health_latest_3d",
      "cdc_catalog.mv_tab_catalog",
      "cdc_catalog.mv_tab_capture_latest",
      "cdc_catalog.mv_tab_events_hourly_3d",
      "cdc_catalog.mv_tab_kafka_hourly_3d",
      "cdc_catalog.mv_tab_logs_hourly_3d",
    ];
    for (const view of views) {
      await mutate(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`);
    }
  }

  const [
    kpis,
    eventsHourly,
    captureLag,
    catalogStatus,
    engineMix,
    errorsByComponent,
    recentErrors,
    applyTail,
    pipelineTables,
    gapEvents,
  ] = await Promise.all([
    query(OPS_KPIS_SQL),
    query(OPS_EVENTS_HOURLY_SQL),
    query(OPS_CAPTURE_LAG_SQL),
    query(OPS_CATALOG_STATUS_SQL),
    query(OPS_ENGINE_MIX_SQL),
    query(OPS_ERRORS_BY_COMPONENT_SQL),
    query(OPS_RECENT_ERRORS_SQL),
    query(OPS_APPLY_TAIL_SQL),
    query(OPS_PIPELINE_TABLES_SQL),
    query(OPS_GAP_EVENTS_SQL),
  ]);

  if (!kpis.ok) {
    return NextResponse.json({ error: kpis.error ?? "ops query failed" }, { status: 500 });
  }

  const k = kpis.rows[0] ?? {};

  return NextResponse.json({
    kpis: {
      connections: Number(k.connections ?? 0),
      cdcReady: Number(k.cdc_ready ?? 0),
      cdcInProgress: Number(k.cdc_in_progress ?? 0),
      needsFullLoad: Number(k.needs_full_load ?? 0),
      applyGreen: Number(k.apply_green ?? 0),
      applyAmber: Number(k.apply_amber ?? 0),
      applyRed: Number(k.apply_red ?? 0),
      errors24h: Number(k.errors_24h ?? 0),
      totalKafkaLag: Number(k.total_kafka_lag ?? 0),
    },
    eventsHourly: eventsHourly.ok ? eventsHourly.rows : [],
    captureLag: captureLag.ok ? captureLag.rows : [],
    catalogStatus: catalogStatus.ok ? catalogStatus.rows : [],
    engineMix: engineMix.ok ? engineMix.rows : [],
    errorsByComponent: errorsByComponent.ok ? errorsByComponent.rows : [],
    recentErrors: recentErrors.ok ? recentErrors.rows : [],
    applyTail: applyTail.ok ? applyTail.rows : [],
    pipelineTables: pipelineTables.ok ? pipelineTables.rows : [],
    gapEvents: gapEvents.ok ? gapEvents.rows : [],
    refreshed: refresh,
  });
}
