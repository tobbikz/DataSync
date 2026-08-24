"use client";

import Link from "next/link";
import { Fragment, useState } from "react";
import { PageMeta, PanelSkeleton } from "./DashboardShell";
import { fmtLag, fmtSeconds, HealthDot } from "./HealthDot";
import { usePolling } from "@/lib/use-polling";
import type { CatalogDetailResponse, CatalogRow, OverviewResponse } from "@/lib/catalog-types";

function deriveIssue(row: CatalogRow) {
  if (row.quarantined) {
    return row.quarantine_reason ?? "quarantined";
  }
  if (row.needs_full_load || row.status === "needs_full_load") {
    return "needs full load";
  }
  if (row.status === "full_load_in_progress") {
    return "full load in progress";
  }
  if (row.status === "pending") {
    return "pending onboard";
  }
  if (row.status === "error") {
    return "replication error";
  }
  if (row.health_reason) {
    return row.health_reason;
  }
  if (row.kafka_lag != null && row.kafka_lag >= 50000) {
    return "kafka lag critical";
  }
  if (row.kafka_lag != null && row.kafka_lag >= 1000) {
    return "kafka lag elevated";
  }
  if (row.capture_lag_seconds != null && row.capture_lag_seconds > 300) {
    return "capture lag high";
  }
  if (row.apply_lag_seconds != null && row.apply_lag_seconds > 300) {
    return "apply lag high";
  }
  return row.status;
}

function SummaryTag({
  label,
  value,
  tone,
  href,
}: {
  label: string;
  value: number;
  tone?: "error" | "warning" | "success" | "neutral";
  href?: string;
}) {
  const toneClass =
    tone === "error"
      ? "bg-error-subtle text-error"
      : tone === "warning"
        ? "bg-warning-subtle text-warning"
        : tone === "success"
          ? "bg-success-subtle text-success"
          : "";

  const body = (
    <span className={`tag ${toneClass}`}>
      {label} {value}
    </span>
  );

  if (href && value > 0) {
    return (
      <Link href={href} className="hover:opacity-80">
        {body}
      </Link>
    );
  }

  return body;
}

function QueueDetail({ detail }: { detail: CatalogDetailResponse }) {
  const { catalog, recent_logs } = detail;
  return (
    <dl className="ml-4 grid gap-1.5 border-l-2 border-accent/30 py-2 pl-3 font-mono text-[11px]">
      <div className="flex gap-2">
        <dt className="w-20 shrink-0 text-foreground-muted">capture</dt>
        <dd>{fmtSeconds(catalog.capture_lag_seconds)}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="w-20 shrink-0 text-foreground-muted">apply</dt>
        <dd>{fmtSeconds(catalog.apply_lag_seconds)}</dd>
      </div>
      <div className="flex gap-2">
        <dt className="w-20 shrink-0 text-foreground-muted">kafka</dt>
        <dd>{fmtLag(catalog.kafka_lag)} msgs</dd>
      </div>
      {catalog.quarantine_reason ? (
        <div className="flex gap-2">
          <dt className="w-20 shrink-0 text-foreground-muted">detail</dt>
          <dd className="text-error">{catalog.quarantine_reason}</dd>
        </div>
      ) : null}
      {recent_logs[0] ? (
        <div className="flex gap-2">
          <dt className="w-20 shrink-0 text-foreground-muted">last log</dt>
          <dd className="line-clamp-2">{recent_logs[0].message}</dd>
        </div>
      ) : null}
      <div>
        <Link
          href={`/dashboard/catalog?catalog_id=${catalog.catalog_id}`}
          className="text-accent hover:underline"
        >
          open in catalog →
        </Link>
      </div>
    </dl>
  );
}

export function OverviewPanel() {
  const { data } = usePolling<OverviewResponse>(
    () => fetch("/api/overview").then((r) => r.json()),
    30_000,
  );

  const [expanded, setExpanded] = useState<number | null>(null);
  const [detail, setDetail] = useState<CatalogDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const summary = data?.summary;
  const queue = data?.queue ?? [];
  if (!data) {
    return (
      <>
        <PageMeta label="Pipeline" />
        <PanelSkeleton rows={4} />
      </>
    );
  }

  async function toggleRow(catalogId: number) {
    if (expanded === catalogId) {
      setExpanded(null);
      setDetail(null);
      return;
    }
    setExpanded(catalogId);
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

  return (
    <>
      <PageMeta
        label="Pipeline"
        meta={
          summary ? (
            <>
              <SummaryTag label="conn" value={summary.connections} />
              <SummaryTag label="tables" value={summary.tables_active} />
              <SummaryTag label="cdc" value={summary.tables_cdc} />
              <SummaryTag
                label="red"
                value={summary.rag_red}
                tone="error"
                href="/dashboard/catalog?rag=RED"
              />
              <SummaryTag
                label="amber"
                value={summary.rag_amber}
                tone="warning"
                href="/dashboard/catalog?rag=AMBER"
              />
              <SummaryTag
                label="green"
                value={summary.rag_green}
                tone="success"
                href="/dashboard/catalog?rag=GREEN"
              />
              <SummaryTag
                label="quarantine"
                value={summary.quarantined}
                tone="error"
                href="/dashboard/catalog?quarantined=true"
              />
              <SummaryTag
                label="needs load"
                value={summary.needs_full_load}
                tone="warning"
                href="/dashboard/catalog?needs_full_load=true"
              />
              {summary.errors_24h > 0 ? (
                <Link href="/dashboard/logs?level=error">
                  <span className="tag bg-error-subtle text-error">
                    errors 24h {summary.errors_24h}
                  </span>
                </Link>
              ) : null}
            </>
          ) : null
        }
      />

      <section className="panel overflow-hidden">
        <div className="panel-header py-2">
          <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
            Needs attention
          </p>
          <span className="font-mono text-[10px] text-foreground-muted">
            {queue.length} tables
          </span>
        </div>
        <div className="table-scroll max-h-96">
          <table className="data-table">
            <thead>
              <tr>
                <th className="w-8" />
                <th className="w-8">RAG</th>
                <th>Connection</th>
                <th>Table</th>
                <th>Issue</th>
                <th>Capture</th>
                <th>Apply</th>
                <th>Kafka</th>
              </tr>
            </thead>
            <tbody>
              {queue.length === 0 ? (
                <tr>
                  <td
                    colSpan={8}
                    className="py-8 text-center font-mono text-[11px] text-foreground-muted"
                  >
                    {data ? "all clear" : "—"}
                  </td>
                </tr>
              ) : (
                queue.map((row) => {
                  const isOpen = expanded === row.catalog_id;
                  return (
                    <Fragment key={row.catalog_id}>
                      <tr
                        className={`cursor-pointer ${isOpen ? "bg-accent-subtle/50" : row.health_rag === "RED" ? "bg-error-subtle/20" : ""}`}
                        onClick={() => toggleRow(row.catalog_id)}
                      >
                        <td className="font-mono text-[10px] text-foreground-muted">
                          {isOpen ? "▾" : "▸"}
                        </td>
                        <td>
                          <HealthDot rag={row.health_rag} />
                        </td>
                        <td className="font-mono text-[11px]">{row.conn_id}</td>
                        <td className="font-mono text-[11px]">
                          {row.source_schema}.{row.source_table}
                        </td>
                        <td className="max-w-xs truncate text-[12px] text-foreground-secondary">
                          {deriveIssue(row)}
                        </td>
                        <td className="font-mono text-[11px] text-foreground-secondary">
                          {fmtSeconds(row.capture_lag_seconds)}
                        </td>
                        <td className="font-mono text-[11px] text-foreground-secondary">
                          {fmtSeconds(row.apply_lag_seconds)}
                        </td>
                        <td className="font-mono text-[11px] text-foreground-secondary">
                          {fmtLag(row.kafka_lag)}
                        </td>
                      </tr>
                      {isOpen ? (
                        <tr>
                          <td colSpan={8} className="!pt-0 !pb-3">
                            {detailLoading ? (
                              <p className="ml-4 py-2 font-mono text-[11px] text-foreground-muted">
                                loading…
                              </p>
                            ) : detail ? (
                              <QueueDetail detail={detail} />
                            ) : null}
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
