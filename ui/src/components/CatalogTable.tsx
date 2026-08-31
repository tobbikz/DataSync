"use client";

import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Badge } from "./Badge";
import { PageMeta } from "./DashboardShell";
import { Pagination } from "./Pagination";
import { fmtLag, fmtSeconds, HealthDot } from "./HealthDot";
import { usePolling } from "@/lib/use-polling";
import { PAGE_SIZE } from "@/lib/pagination";
import type { CatalogDetailResponse, CatalogRow } from "@/lib/catalog-types";

interface CatalogResponse {
  items: CatalogRow[];
  total: number;
}

interface ConnectionOption {
  alias: string;
}

const STATUSES = [
  "pending",
  "success",
  "syncing",
  "error",
  "needs_full_load",
  "full_load_in_progress",
  "quarantined",
];

const RAGS = ["RED", "AMBER", "GREEN", "UNKNOWN"];

function buildQuery(
  filters: {
    q: string;
    conn: string;
    status: string;
    rag: string;
    cdc: string;
    quarantined: string;
    needsFullLoad: string;
  },
  page: number,
) {
  const params = new URLSearchParams();
  if (filters.q) params.set("q", filters.q);
  if (filters.conn) params.set("conn", filters.conn);
  if (filters.status) params.set("status", filters.status);
  if (filters.rag) params.set("rag", filters.rag);
  if (filters.cdc) params.set("cdc", filters.cdc);
  if (filters.quarantined) params.set("quarantined", filters.quarantined);
  if (filters.needsFullLoad) params.set("needs_full_load", filters.needsFullLoad);
  params.set("page", String(page));
  params.set("limit", String(PAGE_SIZE));
  return params.toString();
}

function statusLabel(status: string) {
  return status.replaceAll("_", " ");
}

function CatalogDetailPanel({
  detail,
  busy,
  onEnable,
  onDisable,
  onReset,
  onUnquarantine,
  onSetScd2,
}: {
  detail: CatalogDetailResponse;
  busy: boolean;
  onEnable: () => void;
  onDisable: () => void;
  onReset: () => void;
  onUnquarantine: () => void;
  onSetScd2: (enabled: boolean) => void;
}) {
  const { catalog, apply_position, recent_stats, recent_logs } = detail;
  const replicationOn =
    catalog.active && catalog.cdc_enabled && catalog.capture_during_full_load;

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2 border-b border-border pb-3">
        <button
          type="button"
          disabled={busy || replicationOn}
          onClick={onEnable}
          className="btn-primary py-1 text-[11px]"
        >
          ENABLE REPLICATION
        </button>
        <button
          type="button"
          disabled={busy || !replicationOn}
          onClick={onDisable}
          className="btn-secondary py-1 text-[11px]"
        >
          DISABLE REPLICATION
        </button>
        {catalog.needs_full_load ? (
          <button
            type="button"
            disabled={busy}
            onClick={onReset}
            className="btn-secondary py-1 text-[11px] border-warning/40 text-warning"
            title="Re-run enable replication (full-load reboot)"
          >
            RESET
          </button>
        ) : null}
        {catalog.quarantined ? (
          <button
            type="button"
            disabled={busy}
            onClick={onUnquarantine}
            className="btn-secondary py-1 text-[11px] border-error/40 text-error"
            title="Clear apply quarantine and resume Kafka apply for this table"
          >
            UNQUARANTINE
          </button>
        ) : null}
        {/* Its own switch on purpose: history is independent of whether the table
            replicates, so it can be turned on or off at any time without touching CDC. */}
        <button
          type="button"
          disabled={busy}
          onClick={() => onSetScd2(!catalog.scd2_enabled)}
          className="btn-secondary py-1 text-[11px]"
          title={
            catalog.scd2_enabled
              ? `Stop recording versions. ${catalog.source_table}_history keeps what it already has.`
              : `Record every version of each row in ${catalog.source_table}_history, so a query can ask what a row looked like at any past instant.`
          }
        >
          {catalog.scd2_enabled ? "DISABLE SCD2 HISTORY" : "ENABLE SCD2 HISTORY"}
        </button>
        <span className="font-mono text-[10px] text-foreground-muted">
          active={catalog.active ? "true" : "false"} · cdc=
          {catalog.cdc_enabled ? "on" : "off"} · cap-during-fl=
          {catalog.capture_during_full_load ? "true" : "false"} · scd2=
          {catalog.scd2_enabled ? "on" : "off"}
        </span>
      </div>

      <dl className="grid gap-2 border-l-2 border-accent/30 py-1 pl-3 font-mono text-[11px]">
      <div className="flex gap-2">
        <dt className="w-24 shrink-0 text-foreground-muted">health</dt>
        <dd className="flex items-center gap-1.5">
          <HealthDot rag={catalog.health_rag} />
          {catalog.health_rag}
          {catalog.health_reason ? ` · ${catalog.health_reason}` : ""}
        </dd>
      </div>
      {catalog.quarantined && catalog.quarantine_reason ? (
        <div className="flex gap-2">
          <dt className="w-24 shrink-0 text-foreground-muted">quarantine</dt>
          <dd className="text-error">{catalog.quarantine_reason}</dd>
        </div>
      ) : null}
      <div className="flex gap-2">
        <dt className="w-24 shrink-0 text-foreground-muted">capture</dt>
        <dd>{fmtSeconds(catalog.capture_lag_seconds)}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="w-24 shrink-0 text-foreground-muted">apply</dt>
        <dd>{fmtSeconds(catalog.apply_lag_seconds)}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="w-24 shrink-0 text-foreground-muted">kafka lag</dt>
        <dd>{fmtLag(catalog.kafka_lag)} msgs</dd>
      </div>
      <div className="flex gap-2">
        <dt className="w-24 shrink-0 text-foreground-muted">full load</dt>
        <dd>
          {catalog.last_full_load_at
            ? new Date(catalog.last_full_load_at).toLocaleString()
            : "never"}
          {catalog.needs_full_load ? " · needs reload" : ""}
        </dd>
      </div>
      {apply_position ? (
        <>
          <div className="flex gap-2">
            <dt className="w-24 shrink-0 text-foreground-muted">position</dt>
            <dd>
              {apply_position.kafka_topic ?? "—"}
              {apply_position.kafka_partition != null
                ? ` p${apply_position.kafka_partition}`
                : ""}
              {apply_position.kafka_offset != null
                ? `@${apply_position.kafka_offset}`
                : ""}
            </dd>
          </div>
          <div className="flex gap-2">
            <dt className="w-24 shrink-0 text-foreground-muted">last apply</dt>
            <dd>
              {apply_position.last_applied_at
                ? new Date(apply_position.last_applied_at).toLocaleString()
                : "—"}
            </dd>
          </div>
        </>
      ) : null}
      {recent_stats.length > 0 ? (
        <div className="flex gap-2">
          <dt className="w-24 shrink-0 text-foreground-muted">last slice</dt>
          <dd>
            {recent_stats[0].events_total ?? 0} events · lag{" "}
            {fmtLag(recent_stats[0].kafka_consumer_lag)}
          </dd>
        </div>
      ) : null}
      {recent_logs.length > 0 ? (
        <div className="mt-1 space-y-1 border-t border-border pt-2">
          <p className="text-[10px] uppercase tracking-wide text-foreground-muted">
            recent logs
          </p>
          {recent_logs.slice(0, 5).map((log, i) => (
            <p key={`${log.logged_at}-${i}`} className="leading-snug">
              <span className="text-foreground-muted">
                {new Date(log.logged_at).toLocaleTimeString()}
              </span>{" "}
              <Badge
                tone={
                  log.level === "error"
                    ? "error"
                    : log.level === "warning"
                      ? "warning"
                      : "outline"
                }
              >
                {log.level}
              </Badge>{" "}
              {log.message}
            </p>
          ))}
        </div>
      ) : null}
      </dl>
    </div>
  );
}

export function CatalogTable() {
  const searchParams = useSearchParams();
  const [connections, setConnections] = useState<ConnectionOption[]>([]);
  const [q, setQ] = useState("");
  const [conn, setConn] = useState("");
  const [status, setStatus] = useState("");
  const [rag, setRag] = useState("");
  const [cdc, setCdc] = useState("");
  const [quarantined, setQuarantined] = useState("");
  const [needsFullLoad, setNeedsFullLoad] = useState("");
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [detail, setDetail] = useState<CatalogDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [replicationBusy, setReplicationBusy] = useState<number | null>(null);
  const [actionError, setActionError] = useState("");

  useEffect(() => {
    setRag(searchParams.get("rag") ?? "");
    setQuarantined(searchParams.get("quarantined") ?? "");
    setNeedsFullLoad(searchParams.get("needs_full_load") ?? "");
    setConn(searchParams.get("conn") ?? "");
    const catalogId = searchParams.get("catalog_id");
    if (catalogId) {
      const id = Number(catalogId);
      if (Number.isFinite(id)) {
        setExpanded(id);
        setDetailLoading(true);
        fetch(`/api/catalog/${id}`)
          .then((r) => (r.ok ? r.json() : null))
          .then((d) => setDetail(d))
          .finally(() => setDetailLoading(false));
      }
    }
  }, [searchParams]);

  const queryString = useMemo(
    () =>
      buildQuery(
        { q, conn, status, rag, cdc, quarantined, needsFullLoad },
        page,
      ),
    [q, conn, status, rag, cdc, quarantined, needsFullLoad, page],
  );

  const filterKey = `${q}|${conn}|${status}|${rag}|${cdc}|${quarantined}|${needsFullLoad}`;

  useEffect(() => {
    setPage(1);
  }, [filterKey]);

  const fetchCatalog = useCallback(
    () =>
      fetch(`/api/catalog?${queryString}`).then((r) => r.json()) as Promise<CatalogResponse>,
    [queryString],
  );

  const { data, refresh } = usePolling(fetchCatalog, 30_000);

  const items = data?.items ?? [];
  const total = data?.total ?? 0;

  useEffect(() => {
    fetch("/api/connections?limit=100&page=1")
      .then((r) => r.json())
      .then((d) => setConnections(d.items ?? []));
  }, []);

  useEffect(() => {
    setExpanded(null);
    setDetail(null);
  }, [queryString]);

  async function loadDetail(catalogId: number) {
    setDetailLoading(true);
    setDetail(null);
    try {
      const res = await fetch(`/api/catalog/${catalogId}`);
      if (res.ok) {
        setDetail((await res.json()) as CatalogDetailResponse);
      }
    } finally {
      setDetailLoading(false);
    }
  }

  async function setReplication(
    catalogId: number,
    mode:
      | "enable"
      | "disable"
      | "reset"
      | "unquarantine"
      | "scd2-enable"
      | "scd2-disable",
    reloadDetail = false,
  ) {
    setReplicationBusy(catalogId);
    setActionError("");
    try {
      const res = await fetch(`/api/catalog/${catalogId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode }),
      });
      const body = await res.json();
      if (!res.ok) {
        setActionError(body.error ?? "Failed to update catalog");
        return;
      }
      await refresh();
      if (reloadDetail && expanded === catalogId) {
        await loadDetail(catalogId);
      }
    } finally {
      setReplicationBusy(null);
    }
  }

  async function toggleRow(catalogId: number) {
    if (expanded === catalogId) {
      setExpanded(null);
      setDetail(null);
      return;
    }
    setExpanded(catalogId);
    await loadDetail(catalogId);
  }

  const selectClass = "input-field max-w-[9rem] py-1 font-mono text-[11px]";

  return (
    <>
      <PageMeta label="Catalog" />

      <div className="panel overflow-hidden">
        <div className="panel-header py-2">
          <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
            Replicated tables
          </p>
          <p className="mt-0.5 text-[11px] text-foreground-secondary">
            Click a row to expand health, lag, and recent logs.
          </p>
        </div>

        <div className="catalog-filters">
          <input
            type="search"
            placeholder="Search…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            className="input-field w-full font-mono text-[12px]"
          />
          <select
            value={conn}
            onChange={(e) => setConn(e.target.value)}
            className={selectClass}
          >
            <option value="">all conn</option>
            {connections.map((c) => (
              <option key={c.alias} value={c.alias}>
                {c.alias}
              </option>
            ))}
          </select>
          <select
            value={status}
            onChange={(e) => setStatus(e.target.value)}
            className={selectClass}
          >
            <option value="">all status</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
          <select
            value={rag}
            onChange={(e) => setRag(e.target.value)}
            className={selectClass}
          >
            <option value="">all health</option>
            {RAGS.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
          <select
            value={cdc}
            onChange={(e) => setCdc(e.target.value)}
            className={selectClass}
          >
            <option value="">cdc any</option>
            <option value="true">cdc on</option>
            <option value="false">cdc off</option>
          </select>
          <select
            value={quarantined}
            onChange={(e) => setQuarantined(e.target.value)}
            className={selectClass}
          >
            <option value="">quarantine any</option>
            <option value="true">quarantined</option>
            <option value="false">ok</option>
          </select>
          <select
            value={needsFullLoad}
            onChange={(e) => setNeedsFullLoad(e.target.value)}
            className={selectClass}
          >
            <option value="">full-load any</option>
            <option value="true">needs load</option>
            <option value="false">loaded</option>
          </select>
        </div>

        {actionError ? (
          <p className="px-4 py-2 font-mono text-[11px] text-error">{actionError}</p>
        ) : null}

        {items.length === 0 ? (
          <p className="py-10 text-center font-mono text-[11px] text-foreground-muted">
            no catalog entries
          </p>
        ) : (
          <div className="resource-list resource-list--catalog max-h-[calc(100vh-14rem)] overflow-y-auto">
            <div className="resource-list-head">
              <span>Table</span>
              <span>Health</span>
              <span>Lag</span>
              <span>Flags</span>
            </div>
            {items.map((row) => {
              const isOpen = expanded === row.catalog_id;
              const replicationOn =
                row.active && row.cdc_enabled && row.capture_during_full_load;
              const busy = replicationBusy === row.catalog_id;

              return (
                <Fragment key={row.catalog_id}>
                  <div
                    role="button"
                    tabIndex={0}
                    className={`resource-row group cursor-pointer ${
                      isOpen ? "is-expanded" : ""
                    } ${row.quarantined && !isOpen ? "is-quarantined" : ""}`}
                    onClick={() => toggleRow(row.catalog_id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        toggleRow(row.catalog_id);
                      }
                    }}
                  >
                    <div>
                      <div className="resource-row__title">
                        <span className="mr-1.5 font-mono text-[10px] text-foreground-muted">
                          {isOpen ? "▾" : "▸"}
                        </span>
                        {row.source_schema}.{row.source_table}
                      </div>
                      <div className="resource-row__sub">{row.conn_id}</div>
                    </div>

                    <div>
                      <span className="resource-row__status">
                        <HealthDot rag={row.health_rag} />
                        {row.health_rag}
                      </span>
                      <div className="resource-row__mono-muted mt-0.5">
                        {statusLabel(row.status)}
                      </div>
                    </div>

                    <div>
                      <div className="resource-row__mono">
                        cap {fmtSeconds(row.capture_lag_seconds)}
                      </div>
                      <div className="resource-row__mono-muted">
                        apply {fmtSeconds(row.apply_lag_seconds)} · kafka{" "}
                        {fmtLag(row.kafka_lag)}
                      </div>
                    </div>

                    <div className="text-right">
                      <div className="resource-row__mono text-[10px]">
                        cdc {row.cdc_enabled ? "on" : "off"}
                        {row.needs_full_load ? " · needs load" : ""}
                      </div>
                      <div
                        className="mt-1 flex justify-end gap-1"
                        onClick={(e) => e.stopPropagation()}
                        onKeyDown={(e) => e.stopPropagation()}
                      >
                        <button
                          type="button"
                          disabled={busy || replicationOn}
                          onClick={() => setReplication(row.catalog_id, "enable", isOpen)}
                          className="table-action text-[10px]"
                          title="active + cdc_enabled + capture_during_full_load"
                        >
                          ENABLE REPLICATION
                        </button>
                        <span className="table-action-sep">·</span>
                        <button
                          type="button"
                          disabled={busy || !replicationOn}
                          onClick={() => setReplication(row.catalog_id, "disable", isOpen)}
                          className="table-action table-action--muted text-[10px]"
                        >
                          DISABLE REPLICATION
                        </button>
                        {row.needs_full_load ? (
                          <>
                            <span className="table-action-sep">·</span>
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => setReplication(row.catalog_id, "reset", isOpen)}
                              className="table-action text-[10px] text-warning"
                              title="Re-run enable replication (full-load reboot)"
                            >
                              RESET
                            </button>
                          </>
                        ) : null}
                        {row.quarantined ? (
                          <>
                            <span className="table-action-sep">·</span>
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() =>
                                setReplication(row.catalog_id, "unquarantine", isOpen)
                              }
                              className="table-action text-[10px] text-error"
                              title="Clear apply quarantine and resume Kafka apply"
                            >
                              UNQUARANTINE
                            </button>
                          </>
                        ) : null}
                      </div>
                    </div>
                  </div>

                  {isOpen ? (
                    <div className="resource-row-detail">
                      {detailLoading ? (
                        <p className="font-mono text-[11px] text-foreground-muted">
                          loading…
                        </p>
                      ) : detail ? (
                        <CatalogDetailPanel
                          detail={detail}
                          busy={busy}
                          onEnable={() =>
                            setReplication(row.catalog_id, "enable", true)
                          }
                          onDisable={() =>
                            setReplication(row.catalog_id, "disable", true)
                          }
                          onReset={() =>
                            setReplication(row.catalog_id, "reset", true)
                          }
                          onUnquarantine={() =>
                            setReplication(row.catalog_id, "unquarantine", true)
                          }
                          onSetScd2={(enabled) =>
                            setReplication(
                              row.catalog_id,
                              enabled ? "scd2-enable" : "scd2-disable",
                              true,
                            )
                          }
                        />
                      ) : (
                        <p className="font-mono text-[11px] text-foreground-muted">
                          detail unavailable
                        </p>
                      )}
                    </div>
                  ) : null}
                </Fragment>
              );
            })}
          </div>
        )}
        <Pagination
          page={page}
          total={total}
          pageSize={PAGE_SIZE}
          onPageChange={setPage}
        />
      </div>
    </>
  );
}
