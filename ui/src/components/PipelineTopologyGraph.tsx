"use client";

import { CHART } from "@/lib/chart-theme";
import {
  curvePath,
  FlowEdgePulse,
  FlowGraphShell,
  FlowNode,
  flowStatusColor,
  type FlowNodeStatus,
} from "./flow-graph-shared";

export interface PipelineTableNode {
  source_schema: string;
  source_table: string;
  status: string;
  cdc_enabled: boolean;
  needs_full_load: boolean;
  events_24h: number;
  events_5m: number;
  kafka_lag: number | null;
}

export interface PipelineConnectionFlow {
  conn_id: string;
  db_engine: string;
  capture_status: string | null;
  capture_lag_seconds: number | null;
  tables: PipelineTableNode[];
}

type NodeStatus = FlowNodeStatus;

function tableStatus(t: PipelineTableNode): NodeStatus {
  if (t.status === "failed" || t.status === "error") return "error";
  if (t.needs_full_load || !t.cdc_enabled) return "warn";
  if (t.status === "success" && t.cdc_enabled) return "ok";
  return "idle";
}

function statusColor(s: NodeStatus) {
  return flowStatusColor(s);
}

export function PipelineTopologyGraph({
  flow,
  compact = true,
  hideTitle = false,
}: {
  flow: PipelineConnectionFlow;
  compact?: boolean;
  hideTitle?: boolean;
}) {
  const tables = flow.tables.slice(0, 12);
  const hidden = flow.tables.length - tables.length;
  const connEvents5m = tables.reduce((s, t) => s + t.events_5m, 0);

  const nodeW = compact ? 96 : 168;
  const nodeH = compact ? 44 : 76;
  const tableW = compact ? 108 : 180;
  const tableH = compact ? 40 : 68;
  const col = compact
    ? { source: 8, capture: 120, table: 236, lake: 368 }
    : { source: 24, capture: 216, table: 420, lake: 628 };
  const rowGap = compact ? 8 : 18;
  const tableBlockH = tables.length * (tableH + rowGap) - rowGap;
  const height = Math.max(compact ? 112 : 200, tableBlockH + 64);
  const width = col.lake + nodeW + 24;
  const midY = height / 2;

  const captureStatus: NodeStatus =
    flow.capture_status === "healthy" ? "ok" : flow.capture_status ? "warn" : "idle";

  const sourceStatus: NodeStatus = tables.length > 0 ? "ok" : "idle";
  const lakeStatus: NodeStatus = tables.some((t) => t.status === "success") ? "ok" : "idle";

  const sourceY = midY - nodeH / 2;
  const captureY = midY - nodeH / 2;
  const lakeY = midY - nodeH / 2;

  const tableYs = tables.map((_, i) => {
    const blockStart = midY - tableBlockH / 2;
    return blockStart + i * (tableH + rowGap);
  });

  const trunkPath = curvePath(
    col.source + nodeW,
    sourceY + nodeH / 2,
    col.capture,
    captureY + nodeH / 2,
  );

  const graphContent = (
    <>
      <FlowEdgePulse
        path={trunkPath}
        activityCount={connEvents5m}
        live={connEvents5m > 0}
        edgePhase={0}
        color={CHART.accent}
      />
      {tables.map((table, i) => {
        const ty = tableYs[i] + tableH / 2;
        const ts = tableStatus(table);
        const color = statusColor(ts);
        const live = table.events_5m > 0;
        const inPath = curvePath(col.capture + nodeW, captureY + nodeH / 2, col.table, ty);
        const outPath = curvePath(col.table + tableW, ty, col.lake, lakeY + nodeH / 2);
        return (
          <g key={`${table.source_schema}.${table.source_table}`}>
            <FlowEdgePulse path={inPath} activityCount={table.events_5m} live={live} edgePhase={i + 1} color={color} strokeWidth={1} />
            <FlowEdgePulse path={outPath} activityCount={table.events_5m} live={live} edgePhase={i + 3} color={CHART.accent} strokeWidth={1} />
          </g>
        );
      })}
      <FlowNode x={col.source} y={sourceY} w={nodeW} h={nodeH} title="Source" sub={flow.db_engine} status={sourceStatus} lines={[`${flow.conn_id}`]} />
      <FlowNode
        x={col.capture}
        y={captureY}
        w={nodeW}
        h={nodeH}
        title="Capture"
        sub="binlog → kafka"
        status={captureStatus}
        lines={[flow.capture_status ?? "—", flow.capture_lag_seconds != null ? `${flow.capture_lag_seconds}s` : "—"]}
      />
      {tables.map((table, i) => (
        <FlowNode
          key={`node-${table.source_table}`}
          x={col.table}
          y={tableYs[i]}
          w={tableW}
          h={tableH}
          title={table.source_table}
          sub={table.source_schema}
          status={tableStatus(table)}
          lines={[table.status, liveLabel(table.events_5m, table.events_24h)]}
        />
      ))}
      <FlowNode
        x={col.lake}
        y={lakeY}
        w={nodeW}
        h={nodeH}
        title="Lake"
        sub="kafka → pg"
        status={lakeStatus}
        lines={[`${tables.filter((t) => t.status === "success").length} synced`, connEvents5m > 0 ? "receiving" : "idle"]}
      />
    </>
  );

  if (hideTitle) {
    return (
      <div className="rounded border border-border bg-surface p-2.5">
        <div className="h-[440px] w-full overflow-x-auto">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="h-full w-full"
            preserveAspectRatio="xMidYMid meet"
            role="img"
          >
            {graphContent}
          </svg>
        </div>
      </div>
    );
  }

  return (
    <FlowGraphShell
      title={`${flow.conn_id} · topology`}
      meta={`${flow.db_engine} · ${tables.length} tbl${hidden > 0 ? ` (+${hidden})` : ""}${connEvents5m > 0 ? ` · ${connEvents5m} ev/5m` : ""}`}
      width={width}
      height={height}
      minWidth={720}
      size="large"
    >
      {graphContent}
    </FlowGraphShell>
  );
}

function liveLabel(events5m: number, events24h: number) {
  if (events5m > 0) return `${events5m} ev/5m`;
  if (events24h > 0) return `${events24h} ev/24h`;
  return "no activity";
}

export function groupPipelineFlows(
  rows: {
    conn_id: string;
    db_engine: string;
    source_schema: string;
    source_table: string;
    status: string;
    cdc_enabled: boolean;
    needs_full_load: boolean;
    events_24h: string | number;
    events_5m?: string | number;
    capture_status: string | null;
    capture_lag_seconds: number | null;
    kafka_consumer_lag: number | null;
  }[],
): PipelineConnectionFlow[] {
  const byConn = new Map<string, PipelineConnectionFlow>();
  for (const row of rows) {
    let flow = byConn.get(row.conn_id);
    if (!flow) {
      flow = {
        conn_id: row.conn_id,
        db_engine: row.db_engine,
        capture_status: row.capture_status,
        capture_lag_seconds: row.capture_lag_seconds,
        tables: [],
      };
      byConn.set(row.conn_id, flow);
    }
    flow.tables.push({
      source_schema: row.source_schema,
      source_table: row.source_table,
      status: row.status,
      cdc_enabled: row.cdc_enabled,
      needs_full_load: row.needs_full_load,
      events_24h: Number(row.events_24h ?? 0),
      events_5m: Number(row.events_5m ?? 0),
      kafka_lag: row.kafka_consumer_lag,
    });
  }
  return Array.from(byConn.values());
}
