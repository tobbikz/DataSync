"use client";

import { useState } from "react";
import Link from "next/link";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { StatCard } from "./StatCard";
import { Badge } from "./Badge";
import { usePolling } from "@/lib/use-polling";
import { PageMeta } from "./DashboardShell";
import { CHART, chartAxisTick, chartGrid, chartGridHorizontal, chartTooltip } from "@/lib/chart-theme";

interface OpsPayload {
  kpis: {
    connections: number;
    cdcReady: number;
    cdcInProgress: number;
    needsFullLoad: number;
    applyGreen: number;
    applyAmber: number;
    applyRed: number;
    errors24h: number;
    totalKafkaLag: number;
  };
  eventsHourly: {
    event_ts: string;
    conn_id: string;
    events_total: string;
    events_inserts: string;
    events_updates: string;
    events_deletes: string;
  }[];
  captureLag: {
    conn_id: string;
    capture_status: string;
    capture_lag_seconds: number;
    is_unhealthy: number;
  }[];
  catalogStatus: { status: string; count: number }[];
  engineMix: { db_engine: string; count: number }[];
  errorsByComponent: { component: string; error_count: number }[];
  recentErrors: {
    log_id: number;
    logged_at: string;
    level: string;
    component: string;
    conn_id: string | null;
    message: string;
  }[];
  applyTail: {
    logged_at: string;
    conn_id: string;
    source_schema: string;
    source_table: string;
    apply_health_rag: string;
    events_total: string;
    events_inserts: string;
    events_updates: string;
    events_deletes: string;
    duration_ms: string;
    kafka_consumer_lag: string;
    health_reason: string;
  }[];
}

const MAIN_TABS = [
  { id: "health", label: "Health" },
  { id: "catalog", label: "Catalog" },
  { id: "kafka", label: "Kafka" },
  { id: "events", label: "Events" },
  { id: "logs", label: "Logs" },
] as const;

const EVENTS_SUBTABS = [
  { id: "overview", label: "Overview" },
  { id: "slices", label: "Apply slices" },
] as const;

type MainTab = (typeof MAIN_TABS)[number]["id"];
type EventsSubTab = (typeof EVENTS_SUBTABS)[number]["id"];

function fmtTs(ts: string) {
  return new Date(ts).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function TabBar<T extends string>({
  tabs,
  active,
  onChange,
  nested,
}: {
  tabs: readonly { id: T; label: string }[];
  active: T;
  onChange: (id: T) => void;
  nested?: boolean;
}) {
  return (
    <div
      className={`flex flex-wrap gap-0 border-b border-border ${nested ? "mt-2" : ""}`}
      role="tablist"
    >
      {tabs.map((tab) => (
        <button
          key={tab.id}
          type="button"
          role="tab"
          aria-selected={active === tab.id}
          onClick={() => onChange(tab.id)}
          className={`border-b-2 px-4 py-2 text-[12px] font-medium transition-colors ${
            active === tab.id
              ? "border-accent text-accent"
              : "border-transparent text-foreground-muted hover:text-foreground-secondary"
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}

function EmptyChart({ label }: { label: string }) {
  return (
    <p className="flex h-full min-h-[10rem] items-center justify-center font-mono text-[11px] text-foreground-muted">
      {label}
    </p>
  );
}

function DmlPills({
  inserts,
  updates,
  deletes,
}: {
  inserts: number;
  updates: number;
  deletes: number;
}) {
  return (
    <div className="flex flex-wrap items-center gap-1">
      {inserts > 0 ? (
        <span className="inline-flex items-center gap-0.5 rounded border border-success/30 bg-success-subtle px-1.5 py-0.5 text-[10px] font-semibold text-success">
          +{inserts} I
        </span>
      ) : null}
      {updates > 0 ? (
        <span className="inline-flex items-center gap-0.5 rounded border border-accent/30 bg-accent-subtle px-1.5 py-0.5 text-[10px] font-semibold text-accent">
          ~{updates} U
        </span>
      ) : null}
      {deletes > 0 ? (
        <span className="inline-flex items-center gap-0.5 rounded border border-error/30 bg-error-subtle px-1.5 py-0.5 text-[10px] font-semibold text-error">
          −{deletes} D
        </span>
      ) : null}
      {inserts === 0 && updates === 0 && deletes === 0 ? (
        <span className="text-foreground-muted">—</span>
      ) : null}
    </div>
  );
}

function buildEventSeries(data: OpsPayload["eventsHourly"]) {
  const byHour = new Map<string, Record<string, number | string>>();
  for (const row of data) {
    const hour = fmtTs(row.event_ts);
    const bucket = byHour.get(hour) ?? { hour, total: 0 };
    bucket.total = Number(bucket.total) + Number(row.events_total ?? 0);
    bucket[row.conn_id] = Number(bucket[row.conn_id] ?? 0) + Number(row.events_total ?? 0);
    byHour.set(hour, bucket);
  }
  const series = Array.from(byHour.values()).slice(-48);
  const connIds = [...new Set(data.map((r) => r.conn_id))].slice(0, 6);
  return { series, connIds };
}

function buildIudSeries(data: OpsPayload["eventsHourly"]) {
  const acc = new Map<string, { hour: string; inserts: number; updates: number; deletes: number }>();
  for (const row of data) {
    const hour = fmtTs(row.event_ts);
    const cur = acc.get(hour) ?? { hour, inserts: 0, updates: 0, deletes: 0 };
    cur.inserts += Number(row.events_inserts ?? 0);
    cur.updates += Number(row.events_updates ?? 0);
    cur.deletes += Number(row.events_deletes ?? 0);
    acc.set(hour, cur);
  }
  return Array.from(acc.values()).slice(-24);
}

function buildEventsByConn(data: OpsPayload["eventsHourly"]) {
  const acc = new Map<string, number>();
  for (const row of data) {
    acc.set(row.conn_id, (acc.get(row.conn_id) ?? 0) + Number(row.events_total ?? 0));
  }
  return Array.from(acc.entries()).map(([conn, events]) => ({ conn, events }));
}

export function OpsDashboard() {
  const [mainTab, setMainTab] = useState<MainTab>("health");
  const [eventsSubTab, setEventsSubTab] = useState<EventsSubTab>("overview");
  const [expandedLogId, setExpandedLogId] = useState<number | null>(null);

  const { data, loading } = usePolling<OpsPayload>(
    () => fetch("/api/ops").then((r) => r.json()),
    60_000,
  );

  if (!data?.kpis) {
    return (
      <div className="space-y-3">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="panel h-[72px] animate-pulse bg-surface-muted" />
          ))}
        </div>
      </div>
    );
  }

  const { series: eventSeries, connIds } = buildEventSeries(data.eventsHourly);
  const stackedEvents = buildIudSeries(data.eventsHourly);
  const eventsByConn = buildEventsByConn(data.eventsHourly);

  const ragMix = [
    { name: "GREEN", value: data.kpis.applyGreen },
    { name: "AMBER", value: data.kpis.applyAmber },
    { name: "RED", value: data.kpis.applyRed },
  ].filter((r) => r.value > 0);

  const redSlices = data.applyTail.filter((r) => r.apply_health_rag === "RED").length;
  const eventsInView = data.applyTail.reduce(
    (sum, r) => sum + Number(r.events_total ?? 0),
    0,
  );
  const insertsInView = data.applyTail.reduce(
    (sum, r) => sum + Number(r.events_inserts ?? 0),
    0,
  );
  const updatesInView = data.applyTail.reduce(
    (sum, r) => sum + Number(r.events_updates ?? 0),
    0,
  );
  const deletesInView = data.applyTail.reduce(
    (sum, r) => sum + Number(r.events_deletes ?? 0),
    0,
  );

  return (
    <>
      <PageMeta label="DataSync Ops" />

      <div className="panel overflow-hidden">
        <TabBar tabs={MAIN_TABS} active={mainTab} onChange={setMainTab} />

        <div className="p-4">
          {mainTab === "health" ? (
            <div className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard label="CDC ready tables" value={String(data.kpis.cdcReady)} />
                <StatCard label="Apply GREEN" value={String(data.kpis.applyGreen)} />
                <StatCard label="Apply AMBER" value={String(data.kpis.applyAmber)} />
                <StatCard label="Apply RED" value={String(data.kpis.applyRed)} />
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Events / hour by conn
                    </p>
                  </div>
                  <div className="panel-body h-52">
                    {eventSeries.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={eventSeries}>
                          <CartesianGrid {...chartGrid} />
                          <XAxis dataKey="hour" tick={chartAxisTick} />
                          <YAxis tick={chartAxisTick} />
                          <Tooltip {...chartTooltip} />
                          <Legend wrapperStyle={{ fontSize: 10 }} />
                          {connIds.map((conn, i) => (
                            <Line
                              key={conn}
                              type="monotone"
                              dataKey={conn}
                              stroke={CHART.series[i % CHART.series.length]}
                              dot={false}
                              strokeWidth={1.5}
                            />
                          ))}
                        </LineChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no events in range" />
                    )}
                  </div>
                </section>

                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Events I/U/D stacked
                    </p>
                  </div>
                  <div className="panel-body h-52">
                    {stackedEvents.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={stackedEvents}>
                          <CartesianGrid {...chartGrid} />
                          <XAxis dataKey="hour" tick={chartAxisTick} />
                          <YAxis tick={chartAxisTick} />
                          <Tooltip {...chartTooltip} />
                          <Legend wrapperStyle={{ fontSize: 10 }} />
                          <Bar dataKey="inserts" stackId="dml" fill={CHART.dml.insert} />
                          <Bar dataKey="updates" stackId="dml" fill={CHART.dml.update} />
                          <Bar dataKey="deletes" stackId="dml" fill={CHART.dml.delete} />
                        </BarChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no DML volume" />
                    )}
                  </div>
                </section>
              </div>

              <div className="grid gap-3 xl:grid-cols-3">
                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      RAG mix now
                    </p>
                  </div>
                  <div className="panel-body h-44">
                    {ragMix.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie data={ragMix} dataKey="value" nameKey="name" innerRadius={36} outerRadius={64}>
                            {ragMix.map((entry) => (
                              <Cell
                                key={entry.name}
                                fill={
                                  CHART.ragChart[entry.name as keyof typeof CHART.ragChart] ??
                                  CHART.ragChart.UNKNOWN
                                }
                              />
                            ))}
                          </Pie>
                          <Tooltip {...chartTooltip} />
                        </PieChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no health samples" />
                    )}
                  </div>
                </section>

                <section className="panel xl:col-span-2">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Capture lag by conn
                    </p>
                  </div>
                  <div className="panel-body h-44">
                    {data.captureLag.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={data.captureLag} layout="vertical" margin={{ left: 8 }}>
                          <CartesianGrid {...chartGridHorizontal} />
                          <XAxis type="number" tick={chartAxisTick} />
                          <YAxis type="category" dataKey="conn_id" width={72} tick={chartAxisTick} />
                          <Tooltip {...chartTooltip} />
                          <Bar dataKey="capture_lag_seconds" fill={CHART.accent} />
                        </BarChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no capture positions" />
                    )}
                  </div>
                </section>
              </div>
            </div>
          ) : null}

          {mainTab === "catalog" ? (
            <div className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard label="Active CDC ready" value={String(data.kpis.cdcReady)} deltaTone="success" />
                <StatCard label="CDC in progress" value={String(data.kpis.cdcInProgress)} />
                <StatCard label="Needs full load" value={String(data.kpis.needsFullLoad)} deltaTone="warning" />
                <StatCard label="Connections" value={String(data.kpis.connections)} />
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Catalog status mix
                    </p>
                  </div>
                  <div className="panel-body h-52">
                    {data.catalogStatus.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie data={data.catalogStatus} dataKey="count" nameKey="status" innerRadius={40} outerRadius={72}>
                            {data.catalogStatus.map((_, i) => (
                              <Cell key={i} fill={CHART.blues[i % CHART.blues.length]} />
                            ))}
                          </Pie>
                          <Tooltip {...chartTooltip} />
                        </PieChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no catalog rows" />
                    )}
                  </div>
                </section>

                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Engine mix
                    </p>
                  </div>
                  <div className="panel-body h-52">
                    {data.engineMix.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={data.engineMix}>
                          <CartesianGrid {...chartGrid} />
                          <XAxis dataKey="db_engine" tick={chartAxisTick} />
                          <YAxis tick={chartAxisTick} />
                          <Tooltip {...chartTooltip} />
                          <Bar dataKey="count" fill={CHART.accent} />
                        </BarChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no engines" />
                    )}
                  </div>
                </section>
              </div>

              <section className="panel">
                <div className="panel-header py-2">
                  <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                    Catalog status funnel
                  </p>
                </div>
                <div className="panel-body h-48">
                  {data.catalogStatus.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={data.catalogStatus} layout="vertical" margin={{ left: 16 }}>
                        <CartesianGrid {...chartGridHorizontal} />
                        <XAxis type="number" tick={chartAxisTick} />
                        <YAxis type="category" dataKey="status" width={120} tick={chartAxisTick} />
                        <Tooltip {...chartTooltip} />
                        <Bar dataKey="count" fill={CHART.accent} />
                      </BarChart>
                    </ResponsiveContainer>
                  ) : (
                    <EmptyChart label="no catalog data" />
                  )}
                </div>
              </section>
            </div>
          ) : null}

          {mainTab === "kafka" ? (
            <div className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                <StatCard label="Total Kafka lag" value={String(data.kpis.totalKafkaLag)} deltaTone="warning" />
                <StatCard label="Apply RED" value={String(data.kpis.applyRed)} deltaTone="warning" />
                <StatCard label="Connections" value={String(data.kpis.connections)} />
              </div>

              <section className="panel">
                <div className="panel-header flex flex-wrap items-center justify-between gap-2 py-2">
                  <div>
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Kafka consumer lag by table
                    </p>
                    <p className="mt-0.5 text-[11px] text-foreground-secondary">
                      Latest apply slices — DML detail lives in Events.
                    </p>
                  </div>
                  <Link href="/dashboard/flow?view=kafka" className="table-action text-[11px]">
                    View Kafka flow →
                  </Link>
                </div>
                <div className="panel-body h-56">
                  {data.applyTail.length > 0 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={[...data.applyTail]
                          .sort((a, b) => Number(b.kafka_consumer_lag) - Number(a.kafka_consumer_lag))
                          .slice(0, 12)
                          .map((r) => ({
                            label: `${r.source_table}`,
                            lag: Number(r.kafka_consumer_lag),
                          }))}
                        layout="vertical"
                        margin={{ left: 8 }}
                      >
                        <CartesianGrid {...chartGridHorizontal} />
                        <XAxis type="number" tick={chartAxisTick} />
                        <YAxis type="category" dataKey="label" width={88} tick={chartAxisTick} />
                        <Tooltip {...chartTooltip} />
                        <Bar dataKey="lag" fill={CHART.accent} />
                      </BarChart>
                    </ResponsiveContainer>
                  ) : (
                    <EmptyChart label="no lag samples" />
                  )}
                </div>
              </section>
            </div>
          ) : null}

          {mainTab === "events" ? (
            <div className="space-y-4">
              <TabBar tabs={EVENTS_SUBTABS} active={eventsSubTab} onChange={setEventsSubTab} nested />

              {eventsSubTab === "overview" ? (
                <div className="space-y-4 pt-2">
                  <div className="grid gap-3 xl:grid-cols-2">
                    <section className="panel">
                      <div className="panel-header py-2">
                        <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                          Events total by conn
                        </p>
                      </div>
                      <div className="panel-body h-52">
                        {eventsByConn.length > 0 ? (
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={eventsByConn}>
                              <CartesianGrid {...chartGrid} />
                              <XAxis dataKey="conn" tick={chartAxisTick} />
                              <YAxis tick={chartAxisTick} />
                              <Tooltip {...chartTooltip} />
                              <Bar dataKey="events" fill={CHART.accent} />
                            </BarChart>
                          </ResponsiveContainer>
                        ) : (
                          <EmptyChart label="no events" />
                        )}
                      </div>
                    </section>

                    <section className="panel">
                      <div className="panel-header py-2">
                        <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                          Apply duration avg / hour
                        </p>
                      </div>
                      <div className="panel-body h-52">
                        {data.applyTail.length > 0 ? (
                          <ResponsiveContainer width="100%" height="100%">
                            <BarChart
                              data={data.applyTail.slice(0, 12).map((r) => ({
                                label: `${r.source_table}`,
                                ms: Number(r.duration_ms),
                              }))}
                            >
                              <CartesianGrid {...chartGrid} />
                              <XAxis dataKey="label" tick={chartAxisTick} />
                              <YAxis tick={chartAxisTick} />
                              <Tooltip {...chartTooltip} />
                              <Bar dataKey="ms" fill={CHART.accent} />
                            </BarChart>
                          </ResponsiveContainer>
                        ) : (
                          <EmptyChart label="no apply slices" />
                        )}
                      </div>
                    </section>
                  </div>
                </div>
              ) : null}

              {eventsSubTab === "slices" ? (
                <div className="space-y-4 pt-2">
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
                    <StatCard label="Events in view" value={String(eventsInView)} />
                    <StatCard label="Inserts" value={String(insertsInView)} deltaTone="success" />
                    <StatCard label="Updates" value={String(updatesInView)} />
                    <StatCard label="Deletes" value={String(deletesInView)} deltaTone="warning" />
                    <StatCard label="RED slices" value={String(redSlices)} deltaTone="warning" />
                    <StatCard label="Slices loaded" value={String(data.applyTail.length)} />
                  </div>

                  <section className="panel overflow-hidden">
                    <div className="panel-header py-2">
                      <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                        Apply slice tail
                      </p>
                      <p className="mt-0.5 text-[11px] text-foreground-secondary">
                        DML breakdown per apply batch — inserts, updates, deletes.
                      </p>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full font-mono text-[11px]">
                        <thead className="border-b border-border text-left text-foreground-muted">
                          <tr>
                            <th className="px-3 py-2">Time</th>
                            <th className="px-3 py-2">Table</th>
                            <th className="px-3 py-2">RAG</th>
                            <th className="px-3 py-2">Total</th>
                            <th className="px-3 py-2">DML</th>
                            <th className="px-3 py-2">Duration</th>
                            <th className="px-3 py-2">Lag</th>
                          </tr>
                        </thead>
                        <tbody>
                          {data.applyTail.map((row, i) => {
                            const ins = Number(row.events_inserts ?? 0);
                            const upd = Number(row.events_updates ?? 0);
                            const del = Number(row.events_deletes ?? 0);
                            return (
                              <tr key={`${row.logged_at}-${i}`} className="border-b border-border/60">
                                <td className="px-3 py-1.5 text-foreground-muted">{fmtTs(row.logged_at)}</td>
                                <td className="px-3 py-1.5">
                                  {row.conn_id} · {row.source_schema}.{row.source_table}
                                </td>
                                <td className="px-3 py-1.5" style={{ color: CHART.rag[row.apply_health_rag as keyof typeof CHART.rag] ?? CHART.rag.UNKNOWN }}>
                                  {row.apply_health_rag}
                                </td>
                                <td className="px-3 py-1.5 font-semibold">{row.events_total}</td>
                                <td className="px-3 py-1.5">
                                  <DmlPills inserts={ins} updates={upd} deletes={del} />
                                </td>
                                <td className="px-3 py-1.5">{row.duration_ms} ms</td>
                                <td className="px-3 py-1.5">{row.kafka_consumer_lag}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                      {data.applyTail.length === 0 ? (
                        <EmptyChart label="no apply slices in last 24h" />
                      ) : null}
                    </div>
                  </section>
                </div>
              ) : null}
            </div>
          ) : null}

          {mainTab === "logs" ? (
            <div className="space-y-4">
              <div className="grid gap-3 sm:grid-cols-2">
                <StatCard label="Errors (24h)" value={String(data.kpis.errors24h)} deltaTone="warning" />
                <StatCard label="Components with errors" value={String(data.errorsByComponent.length)} />
              </div>

              <div className="grid gap-3 xl:grid-cols-2">
                <section className="panel">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Errors by component
                    </p>
                  </div>
                  <div className="panel-body h-52">
                    {data.errorsByComponent.length > 0 ? (
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={data.errorsByComponent}>
                          <CartesianGrid {...chartGrid} />
                          <XAxis dataKey="component" tick={chartAxisTick} />
                          <YAxis tick={chartAxisTick} />
                          <Tooltip {...chartTooltip} />
                          <Bar dataKey="error_count" fill={CHART.accent} />
                        </BarChart>
                      </ResponsiveContainer>
                    ) : (
                      <EmptyChart label="no errors" />
                    )}
                  </div>
                </section>

                <section className="panel overflow-hidden">
                  <div className="panel-header py-2">
                    <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                      Recent errors
                    </p>
                  </div>
                  <div className="max-h-64 overflow-y-auto px-3 pb-3">
                    {data.recentErrors.length === 0 ? (
                      <EmptyChart label="no alerts" />
                    ) : (
                      data.recentErrors.map((log) => {
                        const open = expandedLogId === log.log_id;
                        return (
                          <div key={log.log_id} className="border-b border-border last:border-0">
                            <button
                              type="button"
                              onClick={() =>
                                setExpandedLogId((prev) =>
                                  prev === log.log_id ? null : log.log_id,
                                )
                              }
                              className="w-full py-2 text-left font-mono text-[11px] transition-colors hover:bg-surface-muted/60"
                            >
                              <span className="mr-1 text-foreground-muted">{open ? "▾" : "▸"}</span>
                              <span className="text-foreground-muted">{fmtTs(log.logged_at)}</span>{" "}
                              <Badge tone={log.level === "error" ? "error" : "warning"}>{log.level}</Badge>{" "}
                              <span className="text-foreground-secondary">{log.component}</span>
                              {!open ? (
                                <span className="text-foreground-muted"> — {log.message}</span>
                              ) : null}
                            </button>
                            {open ? (
                              <div className="space-y-1.5 border-l-2 border-border pb-2 pl-3 font-mono text-[11px]">
                                {log.conn_id ? (
                                  <p className="text-foreground-muted">conn {log.conn_id}</p>
                                ) : null}
                                <p className="whitespace-pre-wrap leading-relaxed text-foreground">
                                  {log.message}
                                </p>
                                <p className="text-[10px] text-foreground-muted">log_id {log.log_id}</p>
                              </div>
                            ) : null}
                          </div>
                        );
                      })
                    )}
                  </div>
                </section>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </>
  );
}
