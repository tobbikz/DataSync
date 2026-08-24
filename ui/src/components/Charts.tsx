"use client";

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { CHART, chartAxisTick, chartGrid, chartTooltip } from "@/lib/chart-theme";

interface EventPoint {
  hour: string;
  events: number;
}

interface LagPoint {
  conn: string;
  lag: number;
}

function lagColor(lag: number): string {
  if (lag >= 5000) return CHART.error;
  if (lag >= 1000) return CHART.warning;
  return CHART.success;
}

export function EventsChart({ data }: { data: EventPoint[] }) {
  return (
    <div className="h-56 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid {...chartGrid} />
          <XAxis dataKey="hour" axisLine={false} tickLine={false} tick={chartAxisTick} />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={chartAxisTick}
            tickFormatter={(v) => `${Math.round(v / 1000)}k`}
          />
          <Tooltip {...chartTooltip} />
          <Area
            type="monotone"
            dataKey="events"
            stroke={CHART.accent}
            strokeWidth={1.5}
            fill={CHART.accent}
            fillOpacity={0.08}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

export function LagChart({ data }: { data: LagPoint[] }) {
  return (
    <div className="h-44 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid {...chartGrid} />
          <XAxis dataKey="conn" axisLine={false} tickLine={false} tick={chartAxisTick} />
          <YAxis axisLine={false} tickLine={false} tick={chartAxisTick} />
          <Tooltip {...chartTooltip} />
          <Bar dataKey="lag" radius={[0, 0, 0, 0]} maxBarSize={28}>
            {data.map((entry) => (
              <Cell key={entry.conn} fill={lagColor(entry.lag)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div className="mt-2 flex flex-wrap gap-3 font-mono text-[10px] text-foreground-muted">
        <span className="flex items-center gap-1">
          <span className="inline-block size-2 bg-success" />
          &lt;1k
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block size-2 bg-warning" />
          1k–5k
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block size-2 bg-error" />
          &gt;5k
        </span>
      </div>
    </div>
  );
}
