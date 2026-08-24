"use client";

import { CHART } from "@/lib/chart-theme";
import {
  curvePath,
  FlowEdgePulse,
  FlowGraphShell,
  FlowNode,
  type FlowNodeStatus,
} from "./flow-graph-shared";

export interface FlowTableRow {
  source_schema: string;
  source_table: string;
  status: string;
  cdc_enabled: boolean;
  needs_full_load: boolean;
  events_24h: number;
  events_5m: number;
  events_inserts_5m?: number;
  events_updates_5m?: number;
  events_deletes_5m?: number;
  kafka_topic?: string;
  kafka_consumer_lag: number;
  kafka_partition?: number | null;
}

export interface FlowConnectionData {
  conn_id: string;
  db_engine: string;
  capture_status: string | null;
  capture_lag_seconds: number | null;
  tables: FlowTableRow[];
}

function lagStatus(lag: number): FlowNodeStatus {
  if (lag > 5000) return "error";
  if (lag > 500) return "warn";
  if (lag > 0) return "ok";
  return "idle";
}

export function KafkaFlowGraph({ flow }: { flow: FlowConnectionData }) {
  const tables = flow.tables.slice(0, 10);
  const hidden = flow.tables.length - tables.length;
  const totalLag = tables.reduce((s, t) => s + t.kafka_consumer_lag, 0);
  const liveLag = tables.some((t) => t.kafka_consumer_lag > 0 || t.events_5m > 0);

  const nodeW = 80;
  const nodeH = 36;
  const topicW = 92;
  const topicH = 34;
  const col = { capture: 8, broker: 108, topic: 210, consumer: 330, apply: 430 };
  const rowGap = 6;
  const blockH = tables.length * (topicH + rowGap) - rowGap;
  const height = Math.max(96, blockH + 28);
  const width = col.apply + nodeW + 12;
  const midY = height / 2;
  const hubY = midY - nodeH / 2;

  const tableYs = tables.map((_, i) => {
    const start = midY - blockH / 2;
    return start + i * (topicH + rowGap);
  });

  const brokerPath = curvePath(col.capture + nodeW, hubY + nodeH / 2, col.broker, hubY + nodeH / 2);

  return (
    <FlowGraphShell
      title={`${flow.conn_id} · kafka`}
      meta={`lag ${totalLag}${hidden > 0 ? ` · +${hidden} tbl` : ""}`}
      width={width}
      height={height}
    >
      <FlowEdgePulse
        path={brokerPath}
        activityCount={totalLag}
        live={liveLag}
        edgePhase={0}
        color={CHART.accent}
      />

      {tables.map((table, i) => {
        const ty = tableYs[i] + topicH / 2;
        const lag = table.kafka_consumer_lag;
        const live = lag > 0 || table.events_5m > 0;
        const topicPath = curvePath(col.broker + nodeW, hubY + nodeH / 2, col.topic, ty);
        const consumerPath = curvePath(col.topic + topicW, ty, col.consumer, hubY + nodeH / 2);
        const applyPath = curvePath(col.consumer + nodeW, hubY + nodeH / 2, col.apply, hubY + nodeH / 2);

        return (
          <g key={table.source_table}>
            <FlowEdgePulse path={topicPath} activityCount={lag} live={live} edgePhase={i + 1} color={CHART.warning} strokeWidth={1} />
            <FlowEdgePulse path={consumerPath} activityCount={lag} live={live} edgePhase={i + 2} color={CHART.accent} strokeWidth={1} />
            <FlowEdgePulse path={applyPath} activityCount={table.events_5m} live={table.events_5m > 0} edgePhase={i + 4} color={CHART.success} strokeWidth={1} />
            <FlowNode
              x={col.topic}
              y={tableYs[i]}
              w={topicW}
              h={topicH}
              title={table.kafka_topic || table.source_table}
              sub={table.source_schema}
              status={lagStatus(lag)}
              lines={[`lag ${lag}`, table.kafka_partition != null ? `p${table.kafka_partition}` : "—"]}
            />
          </g>
        );
      })}

      <FlowNode
        x={col.capture}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title="Capture"
        sub="binlog"
        status={flow.capture_status === "healthy" ? "ok" : flow.capture_status ? "warn" : "idle"}
        lines={[flow.capture_status ?? "—"]}
      />
      <FlowNode
        x={col.broker}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title="Broker"
        sub="kafka"
        status={liveLag ? "ok" : "idle"}
        lines={[`${tables.length} topics`]}
      />
      <FlowNode
        x={col.consumer}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title="Consumer"
        sub="apply worker"
        status={totalLag > 500 ? "warn" : tables.some((t) => t.events_5m > 0) ? "ok" : "idle"}
        lines={[`Σ lag ${totalLag}`]}
      />
      <FlowNode
        x={col.apply}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title="Apply"
        sub="→ lake"
        status={tables.some((t) => t.events_5m > 0) ? "ok" : "idle"}
        lines={[`${tables.filter((t) => t.events_5m > 0).length} active`]}
      />
    </FlowGraphShell>
  );
}
