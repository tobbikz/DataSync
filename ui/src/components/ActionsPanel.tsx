"use client";

import { useCallback, useEffect, useState } from "react";
import { Badge } from "./Badge";

interface ActionJob {
  id: string;
  action: string;
  status: "running" | "completed" | "failed";
  startedAt: string;
  finishedAt?: string;
  connId?: string;
  command: string;
  output?: string;
}

interface ConfirmState {
  action: "discover" | "full-load" | "onboard-pending";
  title: string;
  description: string;
  connId?: string;
  hotOnly?: boolean;
}

export function ActionsPanel({ connId }: { connId?: string }) {
  const [jobs, setJobs] = useState<ActionJob[]>([]);
  const [runnerOk, setRunnerOk] = useState(true);
  const [busy, setBusy] = useState(false);
  const [confirm, setConfirm] = useState<ConfirmState | null>(null);
  const [error, setError] = useState("");

  const refresh = useCallback(() => {
    fetch("/api/actions")
      .then((r) => r.json())
      .then((d) => {
        setJobs(d.jobs ?? []);
        setRunnerOk(!!d.runnerAvailable);
      });
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 4000);
    return () => clearInterval(t);
  }, [refresh]);

  async function runAction(state: ConfirmState) {
    setBusy(true);
    setError("");
    setConfirm(null);

    const res = await fetch("/api/actions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: state.action,
        connId: state.connId,
        hotOnly: state.hotOnly,
        confirm: true,
      }),
    });

    const data = await res.json();
    if (!res.ok) {
      setError(data.error ?? "Action failed to start");
    }
    refresh();
    setBusy(false);
  }

  const actions = connId
    ? [
        {
          label: "Full load",
          action: "full-load" as const,
          title: `Full load — ${connId}`,
          description:
            "Runs DataSync full-load for this connection. May take a long time and reload lake tables.",
        },
        {
          label: "Onboard hot",
          action: "onboard-pending" as const,
          title: `Onboard pending (hot) — ${connId}`,
          description: "Onboards hot-tier pending tables for this connection.",
          hotOnly: true,
        },
      ]
    : [
        {
          label: "Discover",
          action: "discover" as const,
          title: "Run discover",
          description:
            "Scans all active connections and upserts catalog entries. Safe to run periodically.",
        },
        {
          label: "Onboard hot",
          action: "onboard-pending" as const,
          title: "Onboard pending (hot)",
          description: "Onboards all hot-tier tables waiting for CDC setup.",
          hotOnly: true,
        },
      ];

  function statusTone(status: ActionJob["status"]) {
    if (status === "completed") return "success";
    if (status === "failed") return "error";
    return "accent";
  }

  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
            Pipeline actions
          </p>
          {!runnerOk ? (
            <p className="mt-0.5 text-[11px] text-warning">
              CLI unavailable — build DataSync or run via Docker.
            </p>
          ) : null}
        </div>
        <div className="flex flex-wrap gap-1.5">
          {actions.map((a) => (
            <button
              key={a.label}
              type="button"
              disabled={busy || !runnerOk}
              onClick={() =>
                setConfirm({
                  action: a.action,
                  title: a.title,
                  description: a.description,
                  connId,
                  hotOnly: a.hotOnly,
                })
              }
              className="btn-primary"
            >
              {a.label}
            </button>
          ))}
        </div>
      </div>

      {error ? (
        <p className="border-b border-border px-4 py-2 text-[12px] text-error">
          {error}
        </p>
      ) : null}

      {jobs.length > 0 ? (
        <div className="overflow-x-auto">
          <table className="data-table">
            <thead>
              <tr>
                <th>Action</th>
                <th>Connection</th>
                <th>Status</th>
                <th>Started</th>
                <th>Output</th>
              </tr>
            </thead>
            <tbody>
              {jobs.slice(0, 5).map((job) => (
                <tr key={job.id}>
                  <td className="font-medium text-foreground">{job.action}</td>
                  <td className="font-mono text-[11px]">
                    {job.connId ?? "—"}
                  </td>
                  <td>
                    <Badge tone={statusTone(job.status)}>{job.status}</Badge>
                  </td>
                  <td className="font-mono text-[11px] text-foreground-muted">
                    {new Date(job.startedAt).toLocaleTimeString()}
                  </td>
                  <td className="max-w-xs truncate font-mono text-[11px] text-foreground-secondary">
                    {job.output?.slice(-120) ?? "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="px-4 py-3 text-[12px] text-foreground-muted">
          No recent jobs
        </p>
      )}

      {confirm ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/30 p-4">
          <div className="panel w-full max-w-md border-border-strong p-5">
            <h3 className="text-[14px] font-semibold text-foreground">
              {confirm.title}
            </h3>
            <p className="mt-2 text-[12px] leading-relaxed text-foreground-secondary">
              {confirm.description}
            </p>
            <div className="mt-4 flex gap-2">
              <button
                type="button"
                onClick={() => setConfirm(null)}
                className="btn-secondary flex-1"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => runAction(confirm)}
                className="btn-primary flex-1"
              >
                Confirm
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
