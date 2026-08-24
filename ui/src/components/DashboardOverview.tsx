"use client";

import { EventsChart, LagChart } from "./Charts";
import { StatCard } from "./StatCard";
import { usePolling } from "@/lib/use-polling";

interface Overview {
  connections: number;
  tablesActive: number;
  tablesCdc: number;
  errors24h: number;
  eventSeries: { hour: string; events: number }[];
  lagByConn: { conn: string; lag: number }[];
}

function fmt(n: number) {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function DashboardOverview() {
  const { data, loading, lastUpdated, refresh } = usePolling<Overview>(
    () => fetch("/api/overview").then((r) => r.json()),
    30_000,
  );

  if (!data) {
    return (
      <div className="space-y-3">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="panel h-[72px] animate-pulse bg-surface-muted" />
          ))}
        </div>
        <div className="grid gap-3 xl:grid-cols-3">
          <div className="panel h-64 animate-pulse bg-surface-muted xl:col-span-2" />
          <div className="panel h-64 animate-pulse bg-surface-muted" />
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-end gap-2">
        {lastUpdated ? (
          <span className="font-mono text-[10px] text-foreground-muted">
            {lastUpdated.toLocaleTimeString()}
          </span>
        ) : null}
        <button
          type="button"
          onClick={() => refresh()}
          disabled={loading}
          className="btn-secondary py-1 text-[11px]"
        >
          Refresh
        </button>
      </div>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard label="Connections" value={String(data.connections)} />
        <StatCard
          label="Active tables"
          value={fmt(data.tablesActive)}
          delta="CDC"
          deltaTone="success"
        />
        <StatCard label="CDC enabled" value={fmt(data.tablesCdc)} />
        <StatCard
          label="Errors (24h)"
          value={String(data.errors24h)}
          delta={data.errors24h > 0 ? "review" : "ok"}
          deltaTone={data.errors24h > 0 ? "warning" : "success"}
        />
      </div>

      <div className="mt-4 grid gap-3 xl:grid-cols-3">
        <section className="panel xl:col-span-2">
          <div className="panel-header py-2">
            <div>
              <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                Events applied
              </p>
              <p className="text-[12px] text-foreground-secondary">Last 7 days</p>
            </div>
          </div>
          <div className="panel-body">
            {data.eventSeries.length > 0 ? (
              <EventsChart data={data.eventSeries} />
            ) : (
              <p className="flex h-56 items-center justify-center font-mono text-[11px] text-foreground-muted">
                no event data in range
              </p>
            )}
          </div>
        </section>

        <section className="panel">
          <div className="panel-header py-2">
            <div>
              <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                Kafka lag
              </p>
              <p className="text-[12px] text-foreground-secondary">Last hour</p>
            </div>
          </div>
          <div className="panel-body">
            {data.lagByConn.length > 0 ? (
              <LagChart data={data.lagByConn} />
            ) : (
              <p className="flex h-44 items-center justify-center font-mono text-[11px] text-foreground-muted">
                no lag samples
              </p>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
