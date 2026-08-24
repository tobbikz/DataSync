"use client";

import { Fragment, Suspense, useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { PageMeta } from "./DashboardShell";
import { Pagination } from "./Pagination";
import { usePolling } from "@/lib/use-polling";
import { PAGE_SIZE } from "@/lib/pagination";

interface LogRow {
  log_id: number;
  logged_at: string;
  level: string;
  component: string;
  message: string;
  conn_id: string | null;
}

interface LogsResponse {
  items: LogRow[];
  total: number;
  page: number;
  limit: number;
}

function levelDotClass(level: string) {
  if (level === "error") return "level-dot--error";
  if (level === "warning") return "level-dot--warning";
  if (level === "info") return "level-dot--info";
  return "level-dot--debug";
}

function rowStateClass(level: string, expanded: boolean) {
  if (expanded) return "is-expanded";
  if (level === "error") return "is-error";
  if (level === "warning") return "is-warning";
  return "";
}

function formatTime(iso: string) {
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatSource(log: LogRow) {
  return log.conn_id ? `${log.component} · ${log.conn_id}` : log.component;
}

function truncate(text: string, max = 120) {
  if (text.length <= max) return text;
  return `${text.slice(0, max)}…`;
}

function LogsTableInner() {
  const searchParams = useSearchParams();
  const highlightId = searchParams.get("log_id");
  const [level, setLevel] = useState("");
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState<string | null>(null);
  const highlightRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    setPage(1);
    setExpanded(null);
  }, [level]);

  const fetchLogs = useCallback(() => {
    const params = new URLSearchParams({
      page: String(page),
      limit: String(PAGE_SIZE),
    });
    if (level) params.set("level", level);
    if (highlightId) params.set("log_id", highlightId);
    return fetch(`/api/logs?${params}`).then((r) => r.json()) as Promise<LogsResponse>;
  }, [level, page, highlightId]);

  const { data, refresh } = usePolling(fetchLogs, 10_000);

  const items = data?.items ?? [];
  const total = data?.total ?? 0;

  useEffect(() => {
    refresh();
  }, [level, page, highlightId, refresh]);

  useEffect(() => {
    if (!highlightId || items.length === 0) return;
    const match = items.find((log) => String(log.log_id) === highlightId);
    if (match) {
      setExpanded(String(match.log_id));
      requestAnimationFrame(() => {
        highlightRef.current?.scrollIntoView({ behavior: "smooth", block: "center" });
      });
    }
  }, [highlightId, items]);

  function rowKey(log: LogRow) {
    return String(log.log_id);
  }

  function toggleRow(key: string) {
    setExpanded((prev) => (prev === key ? null : key));
  }

  return (
    <>
      <PageMeta label="Logs" />

      <div className="panel overflow-hidden">
        <div className="panel-header py-2">
          <div>
            <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
              Control plane events
            </p>
            <p className="mt-0.5 text-[11px] text-foreground-secondary">
              Click a row to expand the full message and metadata.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            {["", "info", "warning", "error"].map((l) => (
              <button
                key={l || "all"}
                type="button"
                onClick={() => setLevel(l)}
                className={
                  level === l
                    ? "btn-primary py-1 text-[10px]"
                    : "btn-secondary py-1 text-[10px]"
                }
              >
                {l || "all"}
              </button>
            ))}
          </div>
        </div>

        {items.length === 0 ? (
          <p className="py-10 text-center font-mono text-[11px] text-foreground-muted">
            no log entries
          </p>
        ) : (
          <div className="resource-list resource-list--logs max-h-[calc(100vh-14rem)] overflow-y-auto">
            <div className="resource-list-head">
              <span>When</span>
              <span>Level</span>
              <span>Event</span>
            </div>
            {items.map((log) => {
              const key = rowKey(log);
              const isOpen = expanded === key;
              const isHighlighted = highlightId === key;

              return (
                <Fragment key={key}>
                  <div
                    ref={isHighlighted ? highlightRef : undefined}
                    role="button"
                    tabIndex={0}
                    className={`resource-row group cursor-pointer ${rowStateClass(log.level, isOpen)} ${isHighlighted ? "ring-1 ring-inset ring-accent/40 bg-accent-subtle/30" : ""}`}
                    onClick={() => toggleRow(key)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        toggleRow(key);
                      }
                    }}
                  >
                    <div className="resource-row__mono text-[11px] leading-none">
                      <span className="mr-1.5 font-mono text-[10px] text-foreground-muted">
                        {isOpen ? "▾" : "▸"}
                      </span>
                      <time>{formatTime(log.logged_at)}</time>
                    </div>

                    <div className="flex items-start gap-1.5 pt-px">
                      <span
                        className={`status-dot mt-0.5 ${levelDotClass(log.level)}`}
                      />
                      <span className="text-[10px] uppercase tracking-wide text-foreground-muted">
                        {log.level}
                      </span>
                    </div>

                    <div className="min-w-0">
                      <div className="resource-row__sub text-[11px] leading-none">
                        {formatSource(log)}
                      </div>
                      <div className="mt-1 text-[12px] leading-snug text-foreground">
                        {isOpen ? log.message : truncate(log.message)}
                      </div>
                    </div>
                  </div>

                  {isOpen ? (
                    <div className="resource-row-detail border-b border-border/60">
                      <dl className="grid gap-2 font-mono text-[11px] sm:grid-cols-2">
                        <div>
                          <dt className="text-foreground-muted">log_id</dt>
                          <dd>{log.log_id}</dd>
                        </div>
                        <div>
                          <dt className="text-foreground-muted">logged_at</dt>
                          <dd>{log.logged_at}</dd>
                        </div>
                        <div>
                          <dt className="text-foreground-muted">component</dt>
                          <dd>{log.component}</dd>
                        </div>
                        <div>
                          <dt className="text-foreground-muted">conn_id</dt>
                          <dd>{log.conn_id ?? "—"}</dd>
                        </div>
                      </dl>
                      <p className="mt-3 whitespace-pre-wrap font-mono text-[11px] leading-relaxed text-foreground">
                        {log.message}
                      </p>
                    </div>
                  ) : null}
                </Fragment>
              );
            })}
          </div>
        )}

        {total > PAGE_SIZE ? (
          <div className="border-t border-border px-3 py-2">
            <Pagination page={page} total={total} pageSize={PAGE_SIZE} onPageChange={setPage} />
          </div>
        ) : null}
      </div>
    </>
  );
}

export function LogsTable() {
  return (
    <Suspense fallback={<p className="p-4 font-mono text-[11px] text-foreground-muted">loading logs…</p>}>
      <LogsTableInner />
    </Suspense>
  );
}
