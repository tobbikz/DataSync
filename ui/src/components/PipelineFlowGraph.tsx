"use client";

import { CHART } from "@/lib/chart-theme";
import { FlowEdgePulse } from "./FlowEdgePulse";

export interface PipelineStage {
  id: string;
  label: string;
  sublabel: string;
  status: "ok" | "warn" | "idle" | "error";
  metrics: { key: string; value: string }[];
}

interface PipelineFlowGraphProps {
  stages: PipelineStage[];
  recentEvents?: number;
  compact?: boolean;
  hideTitle?: boolean;
}

function statusColor(s: PipelineStage["status"]) {
  return CHART.pipeline[s];
}

function statusBg(s: PipelineStage["status"]) {
  const key = `${s}Bg` as keyof typeof CHART.pipeline;
  return CHART.pipeline[key];
}

function curvePath(x1: number, y1: number, x2: number, y2: number) {
  const dx = (x2 - x1) * 0.42;
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
}

export function PipelineFlowGraph({
  stages,
  recentEvents = 0,
  compact = false,
  hideTitle = false,
}: PipelineFlowGraphProps) {
  if (stages.length === 0) return null;

  const nodeW = compact ? 92 : 104;
  const nodeH = compact ? 54 : 62;
  const gap = compact ? 18 : 22;
  const padX = compact ? 12 : 16;
  const padY = compact ? 12 : 16;
  const width = padX * 2 + stages.length * nodeW + (stages.length - 1) * gap;
  const height = padY * 2 + nodeH + 8;
  const live = recentEvents > 0;

  const nodes = stages.map((stage, i) => ({
    stage,
    x: padX + i * (nodeW + gap),
    y: padY,
  }));

  return (
    <div className="rounded border border-border bg-surface p-3">
      {!hideTitle ? (
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
          Pipeline flow
        </p>
      ) : null}
      <div className={`w-full overflow-x-auto ${compact ? "h-[240px]" : "h-[320px]"}`}>
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="h-full w-full"
          preserveAspectRatio="xMidYMid meet"
          role="img"
          aria-label="CDC pipeline flow"
        >
        {nodes.slice(0, -1).map((node, i) => {
          const next = nodes[i + 1];
          const x1 = node.x + nodeW;
          const y1 = node.y + nodeH / 2;
          const x2 = next.x;
          const y2 = next.y + nodeH / 2;
          const d = curvePath(x1, y1, x2, y2);
          const edgeLive =
            live &&
            (node.stage.status === "ok" ||
              node.stage.status === "warn" ||
              next.stage.status === "ok" ||
              next.stage.status === "warn");
          const color = statusColor(
            node.stage.status === "error" || next.stage.status === "error"
              ? "error"
              : edgeLive
                ? "ok"
                : "idle",
          );

          return (
            <FlowEdgePulse
              key={`edge-${node.stage.id}`}
              path={d}
              activityCount={recentEvents}
              live={edgeLive}
              edgePhase={i}
              color={color}
              strokeWidth={1.2}
              dimmed={!edgeLive}
            />
          );
        })}

        {nodes.map(({ stage, x, y }) => {
          const color = statusColor(stage.status);
          const fill = statusBg(stage.status);
          return (
            <g key={stage.id} transform={`translate(${x}, ${y})`}>
              <rect width={nodeW} height={nodeH} rx={2} fill={fill} stroke={color} strokeWidth={1} />
              <rect width={nodeW} height={2} fill={color} />
              <text x={8} y={17} fill={CHART.foreground} fontSize={compact ? 9.5 : 10} fontWeight={600}>
                {stage.label}
              </text>
              <text x={8} y={29} fill={CHART.muted} fontSize={8} fontFamily="monospace">
                {stage.sublabel}
              </text>
              {stage.metrics.slice(0, 2).map((m, idx) => (
                <text
                  key={m.key}
                  x={8}
                  y={41 + idx * 11}
                  fill={CHART.foregroundSecondary}
                  fontSize={7.5}
                  fontFamily="monospace"
                >
                  {m.key}: {m.value}
                </text>
              ))}
            </g>
          );
        })}
        </svg>
      </div>
    </div>
  );
}

export function buildConnectionPipelineStages(
  connection: { db_engine: string; db_name: string; host: string; port: number },
  catalog: {
    tables_total: number;
    cdc_ready: number;
    success: number;
  },
  capture: { status: string; capture_lag_seconds: number | null } | null,
  hourly: { events: number; kafka_lag: number }[],
): PipelineStage[] {
  const engine =
    connection.db_engine === "mariadb"
      ? "MariaDB"
      : connection.db_engine === "mssql"
        ? "SQL Server"
        : "MongoDB";

  const events24h = hourly.reduce((s, h) => s + h.events, 0);
  const kafkaLag = hourly.length ? Math.max(...hourly.map((h) => h.kafka_lag)) : 0;

  const captureOk = capture?.status === "healthy";
  const captureWarn = capture && !captureOk;

  return [
    {
      id: "source",
      label: "Source",
      sublabel: `${engine} · ${connection.db_name}`,
      status: catalog.tables_total > 0 ? "ok" : "idle",
      metrics: [
        { key: "host", value: `${connection.host}:${connection.port}` },
        { key: "tables", value: String(catalog.tables_total) },
      ],
    },
    {
      id: "capture",
      label: "Capture",
      sublabel: "binlog → kafka",
      status: captureOk ? "ok" : captureWarn ? "warn" : "idle",
      metrics: [
        { key: "status", value: capture?.status ?? "—" },
        {
          key: "lag",
          value:
            capture?.capture_lag_seconds != null
              ? `${capture.capture_lag_seconds}s`
              : "—",
        },
      ],
    },
    {
      id: "kafka",
      label: "Kafka",
      sublabel: "topics",
      status: kafkaLag > 1000 ? "warn" : catalog.cdc_ready > 0 ? "ok" : "idle",
      metrics: [
        { key: "cdc", value: String(catalog.cdc_ready) },
        { key: "lag", value: String(kafkaLag) },
      ],
    },
    {
      id: "apply",
      label: "Apply",
      sublabel: "kafka → lake",
      status: events24h > 0 ? "ok" : catalog.success > 0 ? "ok" : "idle",
      metrics: [
        { key: "24h", value: String(events24h) },
        { key: "ok", value: String(catalog.success) },
      ],
    },
    {
      id: "lake",
      label: "Lake",
      sublabel: `pg · ${connection.db_name}`,
      status: catalog.success > 0 ? "ok" : "idle",
      metrics: [
        { key: "synced", value: String(catalog.success) },
        { key: "schema", value: connection.db_name },
      ],
    },
  ];
}
