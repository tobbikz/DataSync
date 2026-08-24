"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { PageMeta } from "./DashboardShell";
import { usePolling } from "@/lib/use-polling";
import {
  PipelineFlowGraph,
  buildConnectionPipelineStages,
} from "./PipelineFlowGraph";
import { PipelineTopologyGraph } from "./PipelineTopologyGraph";
import { KafkaFlowGraph } from "./KafkaFlowGraph";
import { EventsFlowGraph } from "./EventsFlowGraph";
import { CatalogFlowGraph, buildFlowConnections } from "./CatalogFlowGraph";

interface ConnectionItem {
  alias: string;
  db_engine: string;
  active: boolean;
}

interface ConnectionDetail {
  connection: {
    alias: string;
    db_engine: string;
    host: string;
    port: number;
    db_name: string;
  };
  catalog: {
    tables_total: number;
    cdc_ready: number;
    success: number;
  };
  capture: { status: string; capture_lag_seconds: number | null } | null;
  hourly: { events: number; kafka_lag: number }[];
  events_5m: number;
}

interface OpsFlowPayload {
  pipelineTables: {
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
    kafka_partition?: number | null;
    capture_status: string | null;
    capture_lag_seconds: number | null;
    kafka_consumer_lag: number | null;
  }[];
}

const FLOW_VIEWS = [
  { id: "topology", label: "Topology", desc: "Source → capture → tables → lake" },
  { id: "pipeline", label: "Pipeline", desc: "Linear 5-stage CDC path" },
  { id: "kafka", label: "Kafka", desc: "Broker, topics, consumer lag" },
  { id: "events", label: "Events", desc: "DML flow with I / U / D" },
  { id: "catalog", label: "Catalog", desc: "Table lifecycle stages" },
] as const;

type FlowView = (typeof FLOW_VIEWS)[number]["id"];

function LinearFlowCard({ alias }: { alias: string }) {
  const { data, loading } = usePolling<ConnectionDetail>(
    () =>
      fetch(`/api/connections/${encodeURIComponent(alias)}/detail`).then((r) => {
        if (!r.ok) throw new Error("detail fetch failed");
        return r.json();
      }),
    10_000,
  );

  if (loading && !data) {
    return (
      <div className="rounded border border-border bg-surface p-3 font-mono text-[11px] text-foreground-muted">
        loading {alias}…
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded border border-border bg-surface p-3 font-mono text-[11px] text-error">
        could not load {alias}
      </div>
    );
  }

  const stages = buildConnectionPipelineStages(
    data.connection,
    data.catalog,
    data.capture,
    data.hourly,
  );

  return (
    <div className="space-y-1.5">
      <div className="flex items-baseline justify-between gap-2 px-0.5">
        <p className="text-[11px] font-semibold text-foreground">{alias}</p>
        <Link
          href={`/dashboard/catalog?conn=${encodeURIComponent(alias)}`}
          className="table-action text-[10px]"
        >
          catalog →
        </Link>
      </div>
      <PipelineFlowGraph stages={stages} recentEvents={data.events_5m} compact hideTitle />
    </div>
  );
}

function topologyFromFlow(flow: ReturnType<typeof buildFlowConnections>[0]) {
  return {
    conn_id: flow.conn_id,
    db_engine: flow.db_engine,
    capture_status: flow.capture_status,
    capture_lag_seconds: flow.capture_lag_seconds,
    tables: flow.tables.map((t) => ({
      source_schema: t.source_schema,
      source_table: t.source_table,
      status: t.status,
      cdc_enabled: t.cdc_enabled,
      needs_full_load: t.needs_full_load,
      events_24h: t.events_24h,
      events_5m: t.events_5m,
      kafka_lag: t.kafka_consumer_lag,
    })),
  };
}

export function FlowDashboard() {
  const searchParams = useSearchParams();
  const initialView = (searchParams.get("view") as FlowView) ?? "topology";
  const initialConn = searchParams.get("conn") ?? "all";

  const [view, setView] = useState<FlowView>(
    FLOW_VIEWS.some((t) => t.id === initialView) ? initialView : "topology",
  );
  const [connFilter, setConnFilter] = useState(initialConn);

  const { data: ops, loading: opsLoading } = usePolling<OpsFlowPayload>(
    () => fetch("/api/ops").then((r) => r.json()),
    10_000,
  );

  const { data: connections } = usePolling<{ items: ConnectionItem[] }>(
    () => fetch("/api/connections?limit=100").then((r) => r.json()),
    60_000,
  );

  const flowConnections = useMemo(
    () => buildFlowConnections(ops?.pipelineTables ?? []),
    [ops?.pipelineTables],
  );

  const connIds = useMemo(() => {
    const fromOps = flowConnections.map((f) => f.conn_id);
    const fromList = (connections?.items ?? []).map((c) => c.alias);
    return Array.from(new Set([...fromOps, ...fromList])).sort();
  }, [flowConnections, connections?.items]);

  const visibleFlows =
    connFilter === "all"
      ? flowConnections
      : flowConnections.filter((f) => f.conn_id === connFilter);

  const visibleLinear =
    connFilter === "all" ? connIds : connIds.filter((id) => id === connFilter);

  const activeViewMeta = FLOW_VIEWS.find((v) => v.id === view);

  function renderGraphs() {
    if (view === "pipeline") {
      if (visibleLinear.length === 0) {
        return (
          <EmptyState
            message="No connections registered yet."
            href="/dashboard/connections"
            linkLabel="Add connection →"
          />
        );
      }
      return (
        <div className="space-y-4">
          {visibleLinear.map((alias) => (
            <LinearFlowCard key={alias} alias={alias} />
          ))}
        </div>
      );
    }

    if (opsLoading && !ops) {
      return <p className="font-mono text-[11px] text-foreground-muted">loading flow data…</p>;
    }

    if (visibleFlows.length === 0) {
      return (
        <EmptyState
          message="No active catalog tables to map."
          href="/dashboard/catalog"
          linkLabel="Open catalog →"
        />
      );
    }

    return (
      <div className="space-y-3">
        {visibleFlows.map((flow) => {
          if (view === "topology") {
            return <PipelineTopologyGraph key={flow.conn_id} flow={topologyFromFlow(flow)} compact={false} />;
          }
          if (view === "kafka") {
            return <KafkaFlowGraph key={flow.conn_id} flow={flow} />;
          }
          if (view === "events") {
            return <EventsFlowGraph key={flow.conn_id} flow={flow} />;
          }
          return <CatalogFlowGraph key={flow.conn_id} flow={flow} />;
        })}
      </div>
    );
  }

  return (
    <>
      <PageMeta label="DataSync Flow" />

      <div className="panel overflow-hidden">
        <div className="flex flex-wrap items-end justify-between gap-3 border-b border-border px-4 py-3">
          <div>
            <p className="text-[13px] font-semibold text-foreground">Flow</p>
            <p className="mt-0.5 text-[11px] text-foreground-muted">
              {activeViewMeta?.desc ?? "CDC pipeline visualizations"}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <label className="flex items-center gap-2 text-[11px] text-foreground-muted">
              View
              <select
                value={view}
                onChange={(e) => setView(e.target.value as FlowView)}
                className="input-field min-w-[148px] py-1 font-mono text-[11px]"
              >
                {FLOW_VIEWS.map((v) => (
                  <option key={v.id} value={v.id}>
                    {v.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex items-center gap-2 text-[11px] text-foreground-muted">
              Connection
              <select
                value={connFilter}
                onChange={(e) => setConnFilter(e.target.value)}
                className="input-field min-w-[140px] py-1 font-mono text-[11px]"
              >
                <option value="all">All connections</option>
                {connIds.map((id) => (
                  <option key={id} value={id}>
                    {id}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        <div className="p-4">{renderGraphs()}</div>
      </div>
    </>
  );
}

function EmptyState({
  message,
  href,
  linkLabel,
}: {
  message: string;
  href: string;
  linkLabel: string;
}) {
  return (
    <div className="rounded border border-dashed border-border px-4 py-8 text-center">
      <p className="text-[12px] text-foreground-muted">{message}</p>
      <Link href={href} className="table-action mt-2 inline-block text-[11px]">
        {linkLabel}
      </Link>
    </div>
  );
}
