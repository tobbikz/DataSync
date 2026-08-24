"use client";

import { usePolling } from "@/lib/use-polling";
import type { HealthStatus } from "@/lib/health";

export function SystemHealth() {
  const { data: health } = usePolling<HealthStatus>(
    () => fetch("/api/health").then((r) => r.json()),
    30_000,
  );

  const allOk = health
    ? health.datasync.ok &&
      health.datalake.ok &&
      health.kafka.ok &&
      health.cli.ok
    : false;

  return (
    <div className="panel inline-flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2">
      <span
        className={`status-dot ${!health ? "status-dot--idle" : allOk ? "status-dot--ok" : "status-dot--warn"}`}
      />
      <span className="text-[11px] font-semibold uppercase tracking-wide text-foreground-secondary">
        System status
      </span>
      <span
        className={`tag ${
          !health
            ? "text-foreground-muted"
            : allOk
              ? "bg-success-subtle text-success"
              : "bg-warning-subtle text-warning"
        }`}
      >
        {!health ? "…" : allOk ? "operational" : "degraded"}
      </span>
    </div>
  );
}
