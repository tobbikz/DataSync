"use client";

import { SystemHealth } from "./SystemHealth";

/** Shared page chrome — keeps top anchor consistent across routes. */
export function DashboardShell({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <main className="flex min-h-0 flex-1 flex-col px-6 py-4">
      <div className="shrink-0 pb-4">
        <SystemHealth />
      </div>
      <div className="flex min-h-0 flex-1 flex-col space-y-4">{children}</div>
    </main>
  );
}

export function PageMeta({
  label,
  meta,
  actions,
}: {
  label: string;
  meta?: React.ReactNode;
  actions?: React.ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-2">
      <div className="flex flex-wrap items-baseline gap-x-2.5 gap-y-1">
        <span className="text-[11px] font-semibold uppercase tracking-wide text-foreground-secondary">
          {label}
        </span>
        {meta}
      </div>
      {actions}
    </div>
  );
}

export function PanelSkeleton({ rows = 4 }: { rows?: number }) {
  return (
    <div className="panel overflow-hidden">
      <div className="border-b border-border px-4 py-2">
        <div className="h-3 w-24 animate-pulse bg-surface-muted" />
      </div>
      <div className="space-y-0 divide-y divide-border">
        {Array.from({ length: rows }, (_, i) => (
          <div key={i} className="h-9 animate-pulse bg-surface-muted/60" />
        ))}
      </div>
    </div>
  );
}
