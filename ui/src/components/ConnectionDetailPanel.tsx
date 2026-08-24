"use client";

import Link from "next/link";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { usePolling } from "@/lib/use-polling";
import { CHART, chartAxisTick, chartGrid, chartTooltip } from "@/lib/chart-theme";

interface ConnectionDetail {
  connection: {
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
    username: string;
    active: boolean;
  };
  catalog: {
    tables_total: number;
    tables_active: number;
    cdc_ready: number;
    needs_full_load: number;
    success: number;
    failed: number;
  };
  capture: {
    status: string;
    capture_lag_seconds: number | null;
  } | null;
  hourly: {
    bucket: string;
    events: number;
    kafka_lag: number;
    apply_lag: number;
  }[];
}

function fmtHour(ts: string) {
  return new Date(ts).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function MetricTile({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="rounded border border-border bg-surface px-2.5 py-1.5">
      <p className="text-[9px] font-medium uppercase tracking-wide text-foreground-muted">{label}</p>
      <p className="mt-0.5 font-mono text-[15px] font-semibold leading-none text-foreground">{value}</p>
    </div>
  );
}

function MiniLineChart({
  data,
  dataKey,
  stroke,
  label,
  emptyLabel,
}: {
  data: { label: string; value: number }[];
  dataKey: string;
  stroke: string;
  label: string;
  emptyLabel: string;
}) {
  if (data.length === 0) {
    return (
      <div className="flex h-24 items-center justify-center font-mono text-[10px] text-foreground-muted">
        {emptyLabel}
      </div>
    );
  }

  return (
    <div className="h-24 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid {...chartGrid} />
          <XAxis
            dataKey="label"
            axisLine={false}
            tickLine={false}
            tick={{ ...chartAxisTick, fontSize: 8 }}
            interval="preserveStartEnd"
          />
          <YAxis
            axisLine={false}
            tickLine={false}
            tick={{ ...chartAxisTick, fontSize: 8 }}
            width={28}
          />
          <Tooltip {...chartTooltip} />
          <Line
            type="natural"
            dataKey={dataKey}
            stroke={stroke}
            strokeWidth={1.5}
            dot={{ r: 2, fill: stroke, strokeWidth: 0 }}
            activeDot={{ r: 3 }}
            name={label}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

export function ConnectionDetailPanel({ alias }: { alias: string }) {
  const { data, loading } = usePolling<ConnectionDetail>(
    () =>
      fetch(`/api/connections/${encodeURIComponent(alias)}/detail`).then((r) => {
        if (!r.ok) throw new Error("detail fetch failed");
        return r.json();
      }),
    15_000,
  );

  if (loading && !data) {
    return (
      <div className="resource-row-detail font-mono text-[11px] text-foreground-muted">
        loading detail…
      </div>
    );
  }

  if (!data) {
    return (
      <div className="resource-row-detail font-mono text-[11px] text-error">
        could not load connection detail
      </div>
    );
  }

  const { catalog, capture, hourly } = data;
  const eventsSeries = hourly.map((h) => ({
    label: fmtHour(h.bucket),
    value: h.events,
  }));
  const lagSeries = hourly.map((h) => ({
    label: fmtHour(h.bucket),
    value: h.kafka_lag,
  }));

  const captureLabel = capture
    ? capture.capture_lag_seconds != null
      ? `${capture.capture_lag_seconds}s`
      : capture.status
    : "—";

  return (
    <div className="resource-row-detail space-y-2.5 pt-2">
      <div className="flex flex-wrap items-center justify-end gap-3">
        <Link
          href={`/dashboard/flow?conn=${encodeURIComponent(alias)}&view=topology`}
          className="table-action text-[11px]"
        >
          View flow →
        </Link>
        <Link
          href={`/dashboard/catalog?conn=${encodeURIComponent(alias)}`}
          className="table-action text-[11px]"
        >
          Catalog →
        </Link>
      </div>

      <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-5">
        <MetricTile label="Tables" value={catalog.tables_total} />
        <MetricTile label="CDC ready" value={catalog.cdc_ready} />
        <MetricTile label="Success" value={catalog.success} />
        <MetricTile label="Needs load" value={catalog.needs_full_load} />
        <MetricTile label="Capture lag" value={captureLabel} />
      </div>

      <div className="grid gap-2 md:grid-cols-2">
        <section className="rounded border border-border bg-surface p-2">
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
            Events applied (24h)
          </p>
          <MiniLineChart
            data={eventsSeries}
            dataKey="value"
            stroke={CHART.accent}
            label="events"
            emptyLabel="no apply activity yet"
          />
        </section>
        <section className="rounded border border-border bg-surface p-2">
          <p className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
            Kafka lag (24h)
          </p>
          <MiniLineChart
            data={lagSeries}
            dataKey="value"
            stroke={CHART.accent}
            label="lag"
            emptyLabel="no lag samples yet"
          />
        </section>
      </div>
    </div>
  );
}
