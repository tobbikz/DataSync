"use client";

import {
  curvePath,
  FlowEdgePulse,
  FlowGraphShell,
  FlowNode,
  type FlowNodeStatus,
} from "./flow-graph-shared";
import type { FlowConnectionData } from "./KafkaFlowGraph";
import { CHART } from "@/lib/chart-theme";

type CatalogStage = "discovered" | "full_load" | "cdc_setup" | "live" | "error";

function catalogStage(t: FlowConnectionData["tables"][0]): CatalogStage {
  if (t.status === "failed" || t.status === "error") return "error";
  if (t.needs_full_load) return "full_load";
  if (!t.cdc_enabled || t.status === "cdc_in_progress") return "cdc_setup";
  if (t.status === "success" && t.cdc_enabled) return "live";
  return "discovered";
}

function stageStatus(stage: CatalogStage, count: number): FlowNodeStatus {
  if (count === 0) return "idle";
  if (stage === "error") return "error";
  if (stage === "full_load") return "warn";
  if (stage === "live") return "ok";
  return "warn";
}

const STAGES: { id: CatalogStage; label: string; sub: string }[] = [
  { id: "discovered", label: "Discovered", sub: "registered" },
  { id: "full_load", label: "Full load", sub: "snapshot" },
  { id: "cdc_setup", label: "CDC setup", sub: "enabling" },
  { id: "live", label: "Live", sub: "replicating" },
  { id: "error", label: "Error", sub: "failed" },
];

export function CatalogFlowGraph({ flow }: { flow: FlowConnectionData }) {
  const buckets = STAGES.map((stage) => ({
    ...stage,
    tables: flow.tables.filter((t) => catalogStage(t) === stage.id),
  }));

  const nodeW = 72;
  const nodeH = 34;
  const stageW = 82;
  const stageH = 36;
  const tableW = 84;
  const tableH = 28;
  const col = { conn: 6, s1: 88, s2: 178, s3: 268, s4: 358, s5: 448 };
  const cols = [col.s1, col.s2, col.s3, col.s4, col.s5];
  const hubY = 14;
  const maxTableRows = Math.max(1, ...buckets.map((b) => Math.min(b.tables.length, 3)));
  const height = hubY + stageH + 12 + maxTableRows * (tableH + 4) + 8;
  const width = col.s5 + stageW + 8;

  const liveCount = flow.tables.filter((t) => t.events_5m > 0).length;

  return (
    <FlowGraphShell
      title={`${flow.conn_id} · catalog`}
      meta={`${flow.tables.length} tables · ${liveCount} active`}
      width={width}
      height={height}
      minWidth={520}
    >
      {cols.slice(0, -1).map((x, i) => {
        const next = cols[i + 1];
        const count = buckets[i].tables.length + buckets[i + 1].tables.length;
        const path = curvePath(x + stageW, hubY + stageH / 2, next, hubY + stageH / 2);
        return (
          <FlowEdgePulse
            key={`stage-${i}`}
            path={path}
            activityCount={count}
            live={buckets[i + 1].tables.some((t) => t.events_5m > 0)}
            edgePhase={i}
            color={CHART.accent}
            strokeWidth={1}
          />
        );
      })}

      <FlowEdgePulse
        path={curvePath(col.conn + nodeW, hubY + nodeH / 2, col.s1, hubY + stageH / 2)}
        activityCount={flow.tables.length}
        live={flow.tables.length > 0}
        edgePhase={0}
        color={CHART.accent}
      />

      <FlowNode
        x={col.conn}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title={flow.conn_id}
        sub={flow.db_engine}
        status={flow.tables.length > 0 ? "ok" : "idle"}
        lines={[`${flow.tables.length} tbl`]}
      />

      {buckets.map((bucket, i) => {
        const count = bucket.tables.length;
        const status = stageStatus(bucket.id, count);
        const x = cols[i];
        const tableLine = bucket.tables[0]?.source_table;
        return (
          <g key={bucket.id}>
            <FlowNode
              x={x}
              y={hubY}
              w={stageW}
              h={stageH}
              title={bucket.label}
              sub={bucket.sub}
              status={status}
              lines={[
                `${count} tbl`,
                count > 1 ? `+${count - 1} more` : tableLine ?? "—",
              ]}
            />
            {bucket.tables.slice(0, 3).map((table, ti) => (
              <FlowNode
                key={table.source_table}
                x={x + 2}
                y={hubY + stageH + 8 + ti * (tableH + 4)}
                w={tableW}
                h={tableH}
                title={table.source_table}
                sub={table.status}
                status={table.events_5m > 0 ? "ok" : status}
                lines={[table.cdc_enabled ? "cdc on" : "cdc off"]}
              />
            ))}
          </g>
        );
      })}
    </FlowGraphShell>
  );
}

export function buildFlowConnections(
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
    events_inserts_5m?: string | number;
    events_updates_5m?: string | number;
    events_deletes_5m?: string | number;
    kafka_topic?: string;
    kafka_consumer_lag: string | number | null;
    kafka_partition?: number | null;
    capture_status: string | null;
    capture_lag_seconds: number | null;
  }[],
): FlowConnectionData[] {
  const byConn = new Map<string, FlowConnectionData>();
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
      events_inserts_5m: Number(row.events_inserts_5m ?? 0),
      events_updates_5m: Number(row.events_updates_5m ?? 0),
      events_deletes_5m: Number(row.events_deletes_5m ?? 0),
      kafka_topic: row.kafka_topic,
      kafka_consumer_lag: Number(row.kafka_consumer_lag ?? 0),
      kafka_partition: row.kafka_partition ?? null,
    });
  }
  return Array.from(byConn.values());
}
