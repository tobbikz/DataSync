"use client";

import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "./Badge";
import { PageMeta, PanelSkeleton } from "./DashboardShell";
import { Pagination } from "./Pagination";
import {
  PIPELINE_ACTIONS,
  type OnboardTier,
  type PipelineActionDef,
} from "@/lib/pipeline-actions";
import type { ActionKind } from "@/lib/actions";
import { PAGE_SIZE, paginateSlice } from "@/lib/pagination";

interface ActionJob {
  id: string;
  action: ActionKind;
  status: "running" | "completed" | "failed";
  startedAt: string;
  finishedAt?: string;
  connId?: string;
  command: string;
  output?: string;
  skipOnboard?: boolean;
  hotOnly?: boolean;
  coldOnly?: boolean;
  schema?: string;
  table?: string;
}

interface ConnectionOption {
  alias: string;
}

interface RunForm {
  connId: string;
  onboardTier: OnboardTier;
  skipOnboard: boolean;
  schema: string;
  table: string;
}

const defaultForm: RunForm = {
  connId: "",
  onboardTier: "hot",
  skipOnboard: false,
  schema: "",
  table: "",
};

function dangerTone(d: PipelineActionDef["danger"]) {
  if (d === "high") return "error";
  if (d === "medium") return "warning";
  return "outline";
}

function statusTone(status: ActionJob["status"]) {
  if (status === "completed") return "success";
  if (status === "failed") return "error";
  return "accent";
}

function jobTarget(job: ActionJob) {
  const parts: string[] = [];
  if (job.connId) parts.push(job.connId);
  if (job.hotOnly) parts.push("hot");
  if (job.coldOnly) parts.push("cold");
  if (job.skipOnboard) parts.push("skip onboard");
  if (job.schema) parts.push(job.schema);
  if (job.table) parts.push(job.table);
  return parts.length ? parts.join(" · ") : "all connections";
}

export function PipelinesPanel() {
  const [jobs, setJobs] = useState<ActionJob[]>([]);
  const [runnerOk, setRunnerOk] = useState(true);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [connections, setConnections] = useState<ConnectionOption[]>([]);
  const [pending, setPending] = useState<PipelineActionDef | null>(null);
  const [form, setForm] = useState<RunForm>(defaultForm);
  const [jobsPage, setJobsPage] = useState(1);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);

  const refresh = useCallback(() => {
    return fetch("/api/actions")
      .then((r) => r.json())
      .then((d) => {
        setJobs(d.jobs ?? []);
        setRunnerOk(!!d.runnerAvailable);
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 4000);
    return () => clearInterval(t);
  }, [refresh]);

  useEffect(() => {
    fetch("/api/connections?limit=100&page=1")
      .then((r) => r.json())
      .then((d) => setConnections(d.items ?? []));
  }, []);

  const jobSlice = useMemo(
    () => paginateSlice(jobs, (jobsPage - 1) * PAGE_SIZE, PAGE_SIZE),
    [jobs, jobsPage],
  );

  function openRun(action: PipelineActionDef) {
    setPending(action);
    setForm(defaultForm);
    setError("");
  }

  async function confirmRun() {
    if (!pending) return;
    setBusy(true);
    setError("");

    const payload: Record<string, unknown> = {
      action: pending.id,
      confirm: true,
    };

    if (pending.connOptional || pending.requiresConn) {
      if (form.connId) payload.connId = form.connId;
    }
    if (pending.supportsSkipOnboard && form.skipOnboard) {
      payload.skipOnboard = true;
    }
    if (pending.supportsOnboardTier) {
      payload.onboardTier = form.onboardTier;
    }
    if (pending.supportsSchemaTable) {
      if (form.schema) payload.schema = form.schema;
      if (form.table) payload.table = form.table;
    }

    const res = await fetch("/api/actions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await res.json();
    if (!res.ok) {
      setError(data.error ?? "Action failed to start");
      setBusy(false);
      return;
    }

    setPending(null);
    setJobsPage(1);
    await refresh();
    setBusy(false);
  }

  return (
    <>
      <PageMeta
        label="Pipelines"
        meta={
          <span className="font-mono text-[11px] text-foreground-muted">
            {loading
              ? "loading…"
              : runnerOk
                ? `${PIPELINE_ACTIONS.length} CLI commands`
                : "CLI unavailable"}
          </span>
        }
      />

      {!runnerOk && !loading ? (
        <p className="flex items-center gap-2 font-mono text-[11px] text-warning">
          <span className="status-dot status-dot--warn" />
          Build DataSync (`cpp/build/DataSync`) or use Docker compose to run
          pipeline actions.
        </p>
      ) : null}

      {loading ? (
        <PanelSkeleton rows={6} />
      ) : (
        <>
          <div className="panel overflow-hidden">
            <div className="panel-header py-2">
              <div>
                <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                  DataSync CLI actions
                </p>
                <p className="mt-0.5 text-[11px] text-foreground-secondary">
                  Same commands as{" "}
                  <code className="font-mono">DataSync &lt;command&gt;</code>.
                  Configure scope, then confirm before run.
                </p>
              </div>
            </div>

            <div className="resource-list">
              <div
                className="resource-list-head"
                style={{
                  gridTemplateColumns: "minmax(0,1fr) minmax(0,1.4fr) auto",
                }}
              >
                <span>Command</span>
                <span>Description</span>
                <span>Run</span>
              </div>
              {PIPELINE_ACTIONS.map((action) => (
                <div
                  key={action.id}
                  className="resource-row group"
                  style={{
                    gridTemplateColumns: "minmax(0,1fr) minmax(0,1.4fr) auto",
                  }}
                >
                  <div>
                    <div className="resource-row__title">{action.label}</div>
                    <div className="resource-row__mono-muted mt-0.5">
                      {action.cli}
                    </div>
                  </div>
                  <div>
                    <p className="text-[11px] leading-snug text-foreground-secondary">
                      {action.description}
                    </p>
                    <p className="mt-1 text-[10px] text-foreground-muted">
                      {action.impact}
                    </p>
                  </div>
                  <div className="resource-row__actions justify-end">
                    <button
                      type="button"
                      className="table-action"
                      disabled={!runnerOk || busy}
                      onClick={() => openRun(action)}
                    >
                      Run
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="panel overflow-hidden">
            <div className="panel-header py-2">
              <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
                Recent jobs
              </p>
            </div>

            {jobs.length === 0 ? (
              <p className="py-8 text-center font-mono text-[11px] text-foreground-muted">
                no jobs yet
              </p>
            ) : (
              <div className="resource-list max-h-[calc(100vh-22rem)] overflow-y-auto">
                <div
                  className="resource-list-head"
                  style={{
                    gridTemplateColumns:
                      "minmax(0,0.9fr) minmax(0,1fr) minmax(0,0.7fr) minmax(0,0.8fr)",
                  }}
                >
                  <span>Action</span>
                  <span>Target</span>
                  <span>Status</span>
                  <span>Started</span>
                </div>
                {jobSlice.items.map((job) => {
                  const isOpen = expandedJob === job.id;
                  return (
                    <Fragment key={job.id}>
                      <div
                        role="button"
                        tabIndex={0}
                        className={`resource-row group cursor-pointer ${isOpen ? "is-expanded" : ""}`}
                        style={{
                          gridTemplateColumns:
                            "minmax(0,0.9fr) minmax(0,1fr) minmax(0,0.7fr) minmax(0,0.8fr)",
                        }}
                        onClick={() =>
                          setExpandedJob(isOpen ? null : job.id)
                        }
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            setExpandedJob(isOpen ? null : job.id);
                          }
                        }}
                      >
                        <div className="resource-row__title">{job.action}</div>
                        <div className="resource-row__mono text-[10px]">
                          {jobTarget(job)}
                        </div>
                        <div>
                          <Badge tone={statusTone(job.status)}>
                            {job.status}
                          </Badge>
                        </div>
                        <div className="resource-row__mono-muted">
                          {new Date(job.startedAt).toLocaleString()}
                        </div>
                      </div>
                      {isOpen ? (
                        <div className="resource-row-detail space-y-2">
                          <p className="font-mono text-[10px] text-foreground-muted">
                            {job.command}
                          </p>
                          <pre className="max-h-48 overflow-auto whitespace-pre-wrap font-mono text-[11px] text-foreground-secondary">
                            {job.output?.trim() || "—"}
                          </pre>
                        </div>
                      ) : null}
                    </Fragment>
                  );
                })}
              </div>
            )}
            <Pagination
              page={jobsPage}
              total={jobs.length}
              pageSize={PAGE_SIZE}
              onPageChange={setJobsPage}
            />
          </div>
        </>
      )}

      {pending ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/30 p-4">
          <div className="panel w-full max-w-lg p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="text-[13px] font-semibold text-foreground">
                  Run {pending.label}
                </h3>
                <p className="mt-1 font-mono text-[10px] text-foreground-muted">
                  {pending.cli}
                </p>
              </div>
              <Badge tone={dangerTone(pending.danger)}>
                {pending.danger} impact
              </Badge>
            </div>
            <p className="mt-3 text-[12px] text-foreground-secondary">
              {pending.impact}
            </p>

            <div className="mt-4 space-y-3">
              {(pending.requiresConn || pending.connOptional) && (
                <label className="block space-y-1">
                  <span className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
                    Connection
                    {pending.connOptional ? " (optional — all if empty)" : ""}
                  </span>
                  <select
                    className="input-field font-mono text-[11px]"
                    value={form.connId}
                    onChange={(e) =>
                      setForm((f) => ({ ...f, connId: e.target.value }))
                    }
                  >
                    <option value="">
                      {pending.connOptional ? "all active connections" : "select…"}
                    </option>
                    {connections.map((c) => (
                      <option key={c.alias} value={c.alias}>
                        {c.alias}
                      </option>
                    ))}
                  </select>
                </label>
              )}

              {pending.supportsOnboardTier ? (
                <label className="block space-y-1">
                  <span className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
                    Tier
                  </span>
                  <select
                    className="input-field font-mono text-[11px]"
                    value={form.onboardTier}
                    onChange={(e) =>
                      setForm((f) => ({
                        ...f,
                        onboardTier: e.target.value as OnboardTier,
                      }))
                    }
                  >
                    <option value="hot">hot only</option>
                    <option value="cold">cold only</option>
                    <option value="all">all pending</option>
                  </select>
                </label>
              ) : null}

              {pending.supportsSkipOnboard ? (
                <label className="flex items-center gap-2 text-[12px]">
                  <input
                    type="checkbox"
                    checked={form.skipOnboard}
                    onChange={(e) =>
                      setForm((f) => ({
                        ...f,
                        skipOnboard: e.target.checked,
                      }))
                    }
                  />
                  Skip onboard after full load
                </label>
              ) : null}

              {pending.supportsSchemaTable ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="block space-y-1">
                    <span className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
                      Schema (optional)
                    </span>
                    <input
                      className="input-field font-mono text-[11px]"
                      value={form.schema}
                      onChange={(e) =>
                        setForm((f) => ({ ...f, schema: e.target.value }))
                      }
                      placeholder="public"
                    />
                  </label>
                  <label className="block space-y-1">
                    <span className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
                      Table (optional)
                    </span>
                    <input
                      className="input-field font-mono text-[11px]"
                      value={form.table}
                      onChange={(e) =>
                        setForm((f) => ({ ...f, table: e.target.value }))
                      }
                      placeholder="orders"
                    />
                  </label>
                </div>
              ) : null}
            </div>

            {error ? (
              <p className="mt-3 font-mono text-[11px] text-error">{error}</p>
            ) : null}

            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                className="btn-secondary"
                onClick={() => setPending(null)}
                disabled={busy}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn-primary"
                onClick={confirmRun}
                disabled={
                  busy || (pending.requiresConn && !form.connId.trim())
                }
              >
                {busy ? "Starting…" : "Confirm & run"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
