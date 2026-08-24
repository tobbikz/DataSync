"use client";

import { CHART } from "@/lib/chart-theme";
import {
  curvePath,
  FlowEdgePulse,
  FlowGraphShell,
  FlowNode,
  type FlowNodeStatus,
} from "./flow-graph-shared";
import type { FlowConnectionData } from "./KafkaFlowGraph";

function dmlStatus(ins: number, upd: number, del: number): FlowNodeStatus {
  const total = ins + upd + del;
  if (total > 0) return "ok";
  return "idle";
}

export function EventsFlowGraph({ flow }: { flow: FlowConnectionData }) {
  const tables = flow.tables.slice(0, 8);
  const hidden = flow.tables.length - tables.length;
  const totals = tables.reduce(
    (acc, t) => ({
      ins: acc.ins + (t.events_inserts_5m ?? 0),
      upd: acc.upd + (t.events_updates_5m ?? 0),
      del: acc.del + (t.events_deletes_5m ?? 0),
      ev: acc.ev + t.events_5m,
    }),
    { ins: 0, upd: 0, del: 0, ev: 0 },
  );

  const nodeW = 76;
  const nodeH = 34;
  const tableW = 88;
  const tableH = 32;
  const dmlW = 56;
  const dmlH = 28;
  const col = { source: 6, capture: 92, kafka: 178, apply: 264, dml: 352, lake: 430 };
  const rowGap = 5;
  const blockH = tables.length * (tableH + rowGap) - rowGap;
  const height = Math.max(108, blockH + 36);
  const width = col.lake + nodeW + 8;
  const midY = height / 2;
  const hubY = midY - nodeH / 2;

  const tableYs = tables.map((_, i) => {
    const start = midY - blockH / 2;
    return start + i * (tableH + rowGap);
  });

  const trunk = curvePath(col.source + nodeW, hubY + nodeH / 2, col.capture, hubY + nodeH / 2);
  const capKafka = curvePath(col.capture + nodeW, hubY + nodeH / 2, col.kafka, hubY + nodeH / 2);
  const kafkaApply = curvePath(col.kafka + nodeW, hubY + nodeH / 2, col.apply, hubY + nodeH / 2);
  const applyLake = curvePath(col.apply + nodeW, hubY + nodeH / 2, col.lake, hubY + nodeH / 2);

  const dmlNodes = [
    { id: "I", label: "Insert", count: totals.ins, color: CHART.dml.insert, yOff: -22 },
    { id: "U", label: "Update", count: totals.upd, color: CHART.dml.update, yOff: 0 },
    { id: "D", label: "Delete", count: totals.del, color: CHART.dml.delete, yOff: 22 },
  ];

  return (
    <FlowGraphShell
      title={`${flow.conn_id} · events`}
      meta={`${totals.ev} ev/5m · I${totals.ins} U${totals.upd} D${totals.del}${hidden > 0 ? ` · +${hidden}` : ""}`}
      width={width}
      height={height}
      minWidth={480}
    >
      <FlowEdgePulse path={trunk} activityCount={totals.ev} live={totals.ev > 0} edgePhase={0} color={CHART.accent} />
      <FlowEdgePulse path={capKafka} activityCount={totals.ev} live={totals.ev > 0} edgePhase={1} color={CHART.accent} />
      <FlowEdgePulse path={kafkaApply} activityCount={totals.ev} live={totals.ev > 0} edgePhase={2} color={CHART.accent} />
      <FlowEdgePulse path={applyLake} activityCount={totals.ev} live={totals.ev > 0} edgePhase={3} color={CHART.success} />

      {tables.map((table, i) => {
        const ty = tableYs[i] + tableH / 2;
        const live = table.events_5m > 0;
        const path = curvePath(col.apply + nodeW / 2, hubY + nodeH, col.apply + nodeW / 2, ty);
        return (
          <FlowEdgePulse
            key={table.source_table}
            path={path}
            activityCount={table.events_5m}
            live={live}
            edgePhase={i + 5}
            color={CHART.muted}
            strokeWidth={0.9}
          />
        );
      })}

      {dmlNodes.map((dml, i) => {
        const y = hubY + nodeH / 2 + dml.yOff;
        const path = curvePath(col.apply + nodeW, hubY + nodeH / 2, col.dml, y);
        return (
          <g key={dml.id}>
            <FlowEdgePulse
              path={path}
              activityCount={dml.count}
              live={dml.count > 0}
              edgePhase={i + 10}
              color={dml.color}
              strokeWidth={1}
            />
            <FlowNode
              x={col.dml}
              y={y - dmlH / 2}
              w={dmlW}
              h={dmlH}
              title={dml.label}
              sub={dml.id}
              status={dmlStatus(dml.count, 0, 0)}
              lines={[`${dml.count}/5m`]}
            />
          </g>
        );
      })}

      <FlowNode x={col.source} y={hubY} w={nodeW} h={nodeH} title="Source" sub={flow.db_engine} status={tables.length ? "ok" : "idle"} lines={[flow.conn_id]} />
      <FlowNode
        x={col.capture}
        y={hubY}
        w={nodeW}
        h={nodeH}
        title="Capture"
        sub="DML"
        status={totals.ev > 0 ? "ok" : "idle"}
        lines={[flow.capture_status ?? "—"]}
      />
      <FlowNode x={col.kafka} y={hubY} w={nodeW} h={nodeH} title="Kafka" sub="stream" status={totals.ev > 0 ? "ok" : "idle"} lines={[`${tables.length} tbl`]} />
      <FlowNode x={col.apply} y={hubY} w={nodeW} h={nodeH} title="Apply" sub="decode" status={totals.ev > 0 ? "ok" : "idle"} lines={[`${totals.ev} ev`]} />
      <FlowNode x={col.lake} y={hubY} w={nodeW} h={nodeH} title="Lake" sub="pg" status={tables.some((t) => t.status === "success") ? "ok" : "idle"} lines={["partitioned"]} />

      {tables.map((table, i) => (
        <FlowNode
          key={`tbl-${table.source_table}`}
          x={col.apply - tableW / 2 + nodeW / 2}
          y={tableYs[i]}
          w={tableW}
          h={tableH}
          title={table.source_table}
          sub={`I${table.events_inserts_5m ?? 0} U${table.events_updates_5m ?? 0}`}
          status={table.events_5m > 0 ? "ok" : "idle"}
          lines={[`D${table.events_deletes_5m ?? 0}`, `${table.events_5m}/5m`]}
        />
      ))}
    </FlowGraphShell>
  );
}
