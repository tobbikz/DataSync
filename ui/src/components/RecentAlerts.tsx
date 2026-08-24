"use client";

import Link from "next/link";
import { usePolling } from "@/lib/use-polling";
import { Badge } from "./Badge";

interface LogRow {
  logged_at: string;
  level: string;
  component: string;
  message: string;
  conn_id: string | null;
}

interface LogsResponse {
  items: LogRow[];
}

export function RecentAlerts() {
  const { data, lastUpdated } = usePolling<LogsResponse>(
    () => fetch("/api/logs?level=error&limit=6").then((r) => r.json()),
    15_000,
  );

  const items = data?.items ?? [];

  return (
    <section className="panel flex flex-col">
      <div className="panel-header py-2">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-wide text-foreground-secondary">
            Recent errors
          </p>
          <p className="text-[11px] text-foreground-muted">Last 6 · auto-refresh</p>
        </div>
        <Link href="/dashboard/logs" className="btn-secondary py-1 text-[11px]">
          View all
        </Link>
      </div>

      <div className="flex-1 divide-y divide-border">
        {items.length === 0 ? (
          <p className="px-4 py-6 text-center font-mono text-[11px] text-foreground-muted">
            {data ? "no errors" : "loading…"}
          </p>
        ) : (
          items.map((log, i) => (
            <article key={`${log.logged_at}-${i}`} className="px-4 py-3">
              <div className="flex items-center gap-2">
                <Badge tone="error">{log.level}</Badge>
                <span className="truncate text-[11px] text-foreground-muted">
                  {log.component}
                </span>
                <time className="ml-auto shrink-0 font-mono text-[10px] text-foreground-muted">
                  {new Date(log.logged_at).toLocaleTimeString()}
                </time>
              </div>
              <p className="mt-1.5 line-clamp-2 text-[12px] leading-snug text-foreground-secondary">
                {log.conn_id ? (
                  <span className="font-mono text-accent">{log.conn_id} · </span>
                ) : null}
                {log.message}
              </p>
            </article>
          ))
        )}
      </div>

      {lastUpdated ? (
        <div className="border-t border-border px-4 py-2">
          <p className="font-mono text-[10px] text-foreground-muted">
            updated {lastUpdated.toLocaleTimeString()}
          </p>
        </div>
      ) : null}
    </section>
  );
}
