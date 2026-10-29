"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "./Badge";
import { PageMeta, PanelSkeleton } from "./DashboardShell";
import { usePolling } from "@/lib/use-polling";
import type { CdcTuning } from "@/lib/cdc-tuning-types";

type Tab = "signals" | "notifications" | "lineage" | "cdc";

interface SignalRow {
  signal_id: number;
  requested_at: string;
  conn_id: string | null;
  catalog_id: number | null;
  action: string;
  status: string;
  last_error: string | null;
}

interface NotificationRow {
  notification_id: number;
  created_at: string;
  channel: string;
  severity: string;
  conn_id: string | null;
  title: string;
  body: string;
  acked_at: string | null;
}

interface LineageRow {
  lineage_id: number;
  created_at: string;
  event_type: string;
  conn_id: string | null;
  catalog_id: number | null;
  job_name: string;
  run_id: string | null;
}

const SIGNAL_ACTIONS = [
  "execute_incremental",
  "execute_blocking",
  "pause",
  "resume",
  "stop",
  "reload_config",
] as const;

const BOOL_KEYS: (keyof CdcTuning)[] = [
  "apply_dedup",
  "apply_audit",
  "mssql_replay_on_idle",
  "kafka_heartbeat",
  "kafka_tombstones",
  "reselect_on_apply",
  "cloudevents",
  "tracing",
  "openlineage",
  "include_source_query",
  "schema_events",
];

function fmtTs(ts: string) {
  return new Date(ts).toLocaleString();
}

export function ControlPanel() {
  const [tab, setTab] = useState<Tab>("signals");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  // Signals form
  const [action, setAction] = useState<(typeof SIGNAL_ACTIONS)[number]>(
    "execute_incremental",
  );
  const [connId, setConnId] = useState("");
  const [catalogId, setCatalogId] = useState("");
  const [connOptions, setConnOptions] = useState<string[]>([]);

  // CDC form
  const [cdc, setCdc] = useState<CdcTuning>({});
  const [routingText, setRoutingText] = useState("[]");
  const [excludeOpsText, setExcludeOpsText] = useState("[]");
  const [cdcPath, setCdcPath] = useState("");
  const [cdcLoaded, setCdcLoaded] = useState(false);

  const {
    data: signalsData,
    refresh: refreshSignals,
  } = usePolling<{ items: SignalRow[] }>(
    () => fetch("/api/signals?limit=40").then((r) => r.json()),
    8_000,
  );
  const {
    data: notificationsData,
    refresh: refreshNotifications,
  } = usePolling<{ items: NotificationRow[]; unacked: number }>(
    () => fetch("/api/notifications?limit=80").then((r) => r.json()),
    10_000,
  );
  const { data: lineageData } = usePolling<{ items: LineageRow[] }>(
    () => fetch("/api/lineage?limit=80").then((r) => r.json()),
    15_000,
  );

  useEffect(() => {
    fetch("/api/connections?page=1&limit=200")
      .then((r) => r.json())
      .then((d) => {
        const aliases = (d.items ?? []).map(
          (c: { alias: string }) => c.alias as string,
        );
        setConnOptions(aliases);
      })
      .catch(() => undefined);
  }, []);

  const loadCdc = useCallback(() => {
    fetch("/api/cdc-config")
      .then((r) => r.json())
      .then((d) => {
        if (d.error) {
          setError(d.error);
          return;
        }
        setCdc(d.cdc ?? {});
        setCdcPath(d.path ?? "");
        setRoutingText(JSON.stringify(d.cdc?.content_routing ?? [], null, 2));
        setExcludeOpsText(
          JSON.stringify(d.cdc?.message_filter_exclude_ops ?? [], null, 2),
        );
        setCdcLoaded(true);
      })
      .catch((e) => setError(String(e)));
  }, []);

  useEffect(() => {
    if (tab === "cdc" && !cdcLoaded) loadCdc();
  }, [tab, cdcLoaded, loadCdc]);

  const unacked = notificationsData?.unacked ?? 0;

  const tabs = useMemo(
    () =>
      [
        { id: "signals" as const, label: "Signals" },
        {
          id: "notifications" as const,
          label: unacked > 0 ? `Notifications (${unacked})` : "Notifications",
        },
        { id: "lineage" as const, label: "Lineage" },
        { id: "cdc" as const, label: "CDC config" },
      ] as const,
    [unacked],
  );

  async function postSignal() {
    setBusy(true);
    setError("");
    const res = await fetch("/api/signals", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action,
        connId: connId || undefined,
        catalogId: catalogId ? Number(catalogId) : undefined,
        confirm: true,
      }),
    });
    const data = await res.json();
    if (!res.ok) setError(data.error ?? "Signal failed");
    await refreshSignals();
    setBusy(false);
  }

  async function ackNotifications(ids?: number[]) {
    setBusy(true);
    setError("");
    const res = await fetch("/api/notifications", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ids ? { ids } : { ackAll: true }),
    });
    const data = await res.json();
    if (!res.ok) setError(data.error ?? "Ack failed");
    await refreshNotifications();
    setBusy(false);
  }

  async function saveCdc() {
    setBusy(true);
    setError("");
    let content_routing: unknown[];
    let message_filter_exclude_ops: unknown[];
    try {
      content_routing = JSON.parse(routingText) as unknown[];
      message_filter_exclude_ops = JSON.parse(excludeOpsText) as unknown[];
      if (!Array.isArray(content_routing) || !Array.isArray(message_filter_exclude_ops)) {
        throw new Error("routing / exclude_ops must be JSON arrays");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Invalid JSON");
      setBusy(false);
      return;
    }

    const res = await fetch("/api/cdc-config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        confirm: true,
        reload: true,
        cdc: {
          ...cdc,
          content_routing,
          message_filter_exclude_ops,
        },
      }),
    });
    const data = await res.json();
    if (!res.ok) {
      setError(data.error ?? "Save failed");
    } else {
      setCdc(data.cdc ?? cdc);
    }
    setBusy(false);
  }

  return (
    <>
      <PageMeta
        label="Control"
        meta={
          <span className="font-mono text-[11px] text-foreground-muted">
            signals · notifications · lineage · cdc knobs
          </span>
        }
      />

      {error ? (
        <pre className="whitespace-pre-wrap font-mono text-[11px] text-error">
          {error}
        </pre>
      ) : null}

      <div className="flex flex-wrap gap-1 border-b border-border pb-2">
        {tabs.map((t) => (
          <button
            key={t.id}
            type="button"
            onClick={() => setTab(t.id)}
            className={`px-2.5 py-1 text-[11px] font-medium ${
              tab === t.id
                ? "border-b-2 border-accent text-accent"
                : "text-foreground-secondary hover:text-foreground"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "signals" ? (
        <div className="space-y-3">
          <div className="panel p-3">
            <p className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
              Enqueue snapshot signal
            </p>
            <div className="flex flex-wrap items-end gap-2">
              <label className="text-[11px]">
                Action
                <select
                  className="mt-0.5 block min-w-[180px] border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                  value={action}
                  onChange={(e) =>
                    setAction(e.target.value as (typeof SIGNAL_ACTIONS)[number])
                  }
                >
                  {SIGNAL_ACTIONS.map((a) => (
                    <option key={a} value={a}>
                      {a}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-[11px]">
                Conn
                <select
                  className="mt-0.5 block min-w-[140px] border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                  value={connId}
                  onChange={(e) => setConnId(e.target.value)}
                >
                  <option value="">(optional)</option>
                  {connOptions.map((a) => (
                    <option key={a} value={a}>
                      {a}
                    </option>
                  ))}
                </select>
              </label>
              <label className="text-[11px]">
                Catalog id
                <input
                  className="mt-0.5 block w-28 border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                  value={catalogId}
                  onChange={(e) => setCatalogId(e.target.value)}
                  placeholder="optional"
                />
              </label>
              <button
                type="button"
                disabled={busy}
                onClick={postSignal}
                className="btn-primary py-1 text-[11px]"
              >
                Enqueue
              </button>
            </div>
          </div>

          {!signalsData ? (
            <PanelSkeleton rows={6} />
          ) : (
            <div className="panel overflow-hidden">
              <div className="panel-header py-2">
                <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                  Recent signals
                </p>
              </div>
              <div className="divide-y divide-border">
                {(signalsData.items ?? []).length === 0 ? (
                  <p className="px-3 py-6 text-center font-mono text-[11px] text-foreground-muted">
                    no signals yet
                  </p>
                ) : (
                  (signalsData.items ?? []).map((s) => (
                    <div
                      key={s.signal_id}
                      className="flex flex-wrap items-center gap-2 px-3 py-1.5 font-mono text-[11px]"
                    >
                      <span className="text-foreground-muted">
                        #{s.signal_id}
                      </span>
                      <Badge tone="outline">{s.action}</Badge>
                      <Badge
                        tone={
                          s.status === "failed"
                            ? "error"
                            : s.status === "completed"
                              ? "success"
                              : s.status === "paused"
                                ? "warning"
                                : "outline"
                        }
                      >
                        {s.status}
                      </Badge>
                      <span>{s.conn_id ?? "—"}</span>
                      {s.catalog_id != null ? (
                        <span>cat:{s.catalog_id}</span>
                      ) : null}
                      <span className="ml-auto text-foreground-muted">
                        {fmtTs(s.requested_at)}
                      </span>
                      {s.last_error ? (
                        <span className="w-full text-error">{s.last_error}</span>
                      ) : null}
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
        </div>
      ) : null}

      {tab === "notifications" ? (
        <div className="space-y-3">
          <div className="flex justify-end">
            <button
              type="button"
              disabled={busy || unacked === 0}
              onClick={() => ackNotifications()}
              className="btn-secondary py-1 text-[11px]"
            >
              Ack all unacked
            </button>
          </div>
          {!notificationsData ? (
            <PanelSkeleton rows={6} />
          ) : (
            <div className="panel overflow-hidden">
              <div className="divide-y divide-border">
                {(notificationsData.items ?? []).length === 0 ? (
                  <p className="px-3 py-6 text-center font-mono text-[11px] text-foreground-muted">
                    no notifications
                  </p>
                ) : (
                  (notificationsData.items ?? []).map((n) => (
                    <div
                      key={n.notification_id}
                      className="flex flex-wrap items-start gap-2 px-3 py-2"
                    >
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <Badge
                            tone={
                              n.severity === "error"
                                ? "error"
                                : n.severity === "warning"
                                  ? "warning"
                                  : "outline"
                            }
                          >
                            {n.severity}
                          </Badge>
                          <Badge tone="outline">{n.channel}</Badge>
                          {n.acked_at ? null : (
                            <Badge tone="warning">unacked</Badge>
                          )}
                          <span className="font-mono text-[11px] font-medium">
                            {n.title}
                          </span>
                        </div>
                        {n.body ? (
                          <p className="mt-0.5 font-mono text-[11px] text-foreground-secondary">
                            {n.body}
                          </p>
                        ) : null}
                        <p className="mt-0.5 font-mono text-[10px] text-foreground-muted">
                          {n.conn_id ?? "—"} · {fmtTs(n.created_at)}
                        </p>
                      </div>
                      {!n.acked_at ? (
                        <button
                          type="button"
                          disabled={busy}
                          className="btn-secondary py-0.5 text-[10px]"
                          onClick={() => ackNotifications([n.notification_id])}
                        >
                          Ack
                        </button>
                      ) : null}
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
        </div>
      ) : null}

      {tab === "lineage" ? (
        !lineageData ? (
          <PanelSkeleton rows={6} />
        ) : (
          <div className="panel overflow-hidden">
            <div className="panel-header py-2">
              <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                lineage_events (enable cdc.openlineage)
              </p>
            </div>
            <div className="divide-y divide-border">
              {(lineageData.items ?? []).length === 0 ? (
                <p className="px-3 py-6 text-center font-mono text-[11px] text-foreground-muted">
                  no lineage rows yet
                </p>
              ) : (
                (lineageData.items ?? []).map((row) => (
                  <div
                    key={row.lineage_id}
                    className="flex flex-wrap gap-2 px-3 py-1.5 font-mono text-[11px]"
                  >
                    <Badge tone="outline">{row.event_type}</Badge>
                    <span>{row.job_name}</span>
                    <span>{row.conn_id ?? "—"}</span>
                    {row.catalog_id != null ? (
                      <span>cat:{row.catalog_id}</span>
                    ) : null}
                    {row.run_id ? (
                      <span className="text-foreground-muted">{row.run_id}</span>
                    ) : null}
                    <span className="ml-auto text-foreground-muted">
                      {fmtTs(row.created_at)}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        )
      ) : null}

      {tab === "cdc" ? (
        <div className="space-y-3">
          <p className="font-mono text-[10px] text-foreground-muted">
            {cdcPath || "config.json"} · save writes `cdc` and enqueues reload_config
          </p>
          <div className="panel grid gap-3 p-3 md:grid-cols-2">
            {(
              [
                ["round_idle_seconds", "Round idle (s)"],
                ["slice_max_seconds", "Slice max (s)"],
                ["slice_max_events", "Slice max events"],
                ["capture_worker_count", "Capture workers"],
              ] as const
            ).map(([key, label]) => (
              <label key={key} className="text-[11px]">
                {label}
                <input
                  type="number"
                  className="mt-0.5 block w-full border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                  value={Number(cdc[key] ?? 0)}
                  onChange={(e) =>
                    setCdc((prev) => ({
                      ...prev,
                      [key]: Number(e.target.value),
                    }))
                  }
                />
              </label>
            ))}
            <label className="text-[11px]">
              Binlog mode
              <select
                className="mt-0.5 block w-full border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                value={String(cdc.binlog_mode ?? "auto")}
                onChange={(e) =>
                  setCdc((prev) => ({ ...prev, binlog_mode: e.target.value }))
                }
              >
                <option value="auto">auto</option>
                <option value="cli">cli</option>
                <option value="native">native</option>
              </select>
            </label>
            <div className="md:col-span-2 grid grid-cols-2 gap-2 sm:grid-cols-3">
              {BOOL_KEYS.map((key) => (
                <label
                  key={key}
                  className="flex items-center gap-2 font-mono text-[11px]"
                >
                  <input
                    type="checkbox"
                    checked={Boolean(cdc[key])}
                    onChange={(e) =>
                      setCdc((prev) => ({ ...prev, [key]: e.target.checked }))
                    }
                  />
                  {key}
                </label>
              ))}
            </div>
            <label className="md:col-span-2 text-[11px]">
              content_routing (JSON array)
              <textarea
                className="mt-0.5 h-28 w-full border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                value={routingText}
                onChange={(e) => setRoutingText(e.target.value)}
              />
            </label>
            <label className="md:col-span-2 text-[11px]">
              message_filter_exclude_ops (JSON array, e.g. [&quot;d&quot;])
              <textarea
                className="mt-0.5 h-16 w-full border border-border bg-surface px-2 py-1 font-mono text-[11px]"
                value={excludeOpsText}
                onChange={(e) => setExcludeOpsText(e.target.value)}
              />
            </label>
            <div className="md:col-span-2 flex gap-2">
              <button
                type="button"
                disabled={busy}
                onClick={saveCdc}
                className="btn-primary py-1 text-[11px]"
              >
                Save + reload
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => {
                  setCdcLoaded(false);
                  loadCdc();
                }}
                className="btn-secondary py-1 text-[11px]"
              >
                Reload from disk
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
