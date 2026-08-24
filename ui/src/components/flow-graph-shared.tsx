"use client";

import type { ReactNode } from "react";
import { CHART } from "@/lib/chart-theme";
import { FlowEdgePulse } from "./FlowEdgePulse";

export type FlowNodeStatus = "ok" | "warn" | "idle" | "error";

export function flowStatusColor(s: FlowNodeStatus) {
  return CHART.pipeline[s];
}

export function flowStatusBg(s: FlowNodeStatus) {
  const key = `${s}Bg` as keyof typeof CHART.pipeline;
  return CHART.pipeline[key];
}

export function curvePath(x1: number, y1: number, x2: number, y2: number) {
  const dx = Math.max(24, (x2 - x1) * 0.42);
  return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
}

export function FlowNode({
  x,
  y,
  w,
  h,
  title,
  sub,
  status,
  lines,
}: {
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  sub: string;
  status: FlowNodeStatus;
  lines: string[];
}) {
  const large = h >= 68;
  const padX = large ? 10 : 7;
  const titleSize = large ? 12 : 10;
  const bodySize = large ? 10 : 8.5;
  const titleY = large ? 20 : 16;
  const subY = large ? 34 : 28;
  const lineStartY = large ? 48 : 40;
  const lineGap = large ? 14 : 12;
  const titleLimit = large ? 22 : 16;
  const subLimit = large ? 26 : 18;
  const stroke = flowStatusColor(status);
  const fill = flowStatusBg(status);
  return (
    <g transform={`translate(${x}, ${y})`}>
      <rect width={w} height={h} rx={2} fill={fill} stroke={stroke} strokeWidth={1} />
      <rect width={w} height={large ? 3 : 2} fill={stroke} />
      <text x={padX} y={titleY} fill={CHART.foreground} fontSize={titleSize} fontWeight={600}>
        {title.length > titleLimit ? `${title.slice(0, titleLimit - 1)}…` : title}
      </text>
      <text x={padX} y={subY} fill={CHART.muted} fontSize={bodySize} fontFamily="monospace">
        {sub.length > subLimit ? `${sub.slice(0, subLimit - 1)}…` : sub}
      </text>
      {lines.slice(0, 2).map((line, i) => (
        <text
          key={line}
          x={padX}
          y={lineStartY + i * lineGap}
          fill={CHART.foregroundSecondary}
          fontSize={bodySize}
          fontFamily="monospace"
        >
          {line}
        </text>
      ))}
    </g>
  );
}

export function FlowGraphShell({
  title,
  meta,
  width,
  height,
  minWidth,
  size = "large",
  children,
}: {
  title: string;
  meta?: string;
  width: number;
  height: number;
  minWidth?: number;
  size?: "compact" | "default" | "large";
  children: ReactNode;
}) {
  const shellHeight =
    size === "large" ? "h-[440px]" : size === "compact" ? "h-[200px]" : "h-[280px]";

  return (
    <div className="rounded border border-border bg-surface p-3">
      <div className="mb-2 flex items-baseline justify-between gap-2">
        {title ? (
          <p className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">{title}</p>
        ) : (
          <span />
        )}
        {meta ? <p className="font-mono text-[10px] text-foreground-muted">{meta}</p> : null}
      </div>
      <div
        className={`w-full overflow-x-auto ${shellHeight}`}
        style={minWidth ? { minWidth: `${minWidth}px` } : undefined}
      >
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="h-full w-full"
          preserveAspectRatio="xMidYMid meet"
          role="img"
        >
          {children}
        </svg>
      </div>
    </div>
  );
}

export { FlowEdgePulse };
