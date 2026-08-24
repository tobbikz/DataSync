"use client";

import { useEffect, useMemo, useState } from "react";
import {
  CDC_REQUIREMENTS,
  defaultForm,
  ENGINE_OPTIONS,
  type ConnectionForm,
  type DbEngine,
  validateForm,
  WIZARD_STEPS,
  type WizardStepId,
} from "@/lib/connection-types";

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
        {label}
      </span>
      {children}
    </label>
  );
}

function CdcRequirements({ engine }: { engine: DbEngine }) {
  return (
    <div className="border border-warning/40 bg-warning-subtle px-3 py-2.5">
      <p className="text-[11px] font-semibold uppercase tracking-wide text-warning">
        CDC required on source
      </p>
      <ul className="mt-1.5 list-inside list-disc space-y-0.5 text-[12px] text-foreground-secondary">
        {CDC_REQUIREMENTS[engine].map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
      <p className="mt-2 text-[11px] text-foreground-muted">
        DataSync validates CDC readiness before saving. Connections that fail
        preflight are rejected with the exact reason.
      </p>
    </div>
  );
}

function formToPayload(form: ConnectionForm, isEdit: boolean) {
  const extras =
    form.db_engine === "mongodb" && form.replica_set.trim()
      ? { replica_set: form.replica_set.trim() }
      : {};

  const payload: Record<string, unknown> = {
    alias: form.alias.trim().toUpperCase(),
    db_engine: form.db_engine,
    host: form.host.trim(),
    port: form.port,
    db_name: form.db_name.trim(),
    username: form.username.trim(),
    extras,
    active: true,
  };

  if (!isEdit || form.password) {
    payload.password = form.password;
  }

  return payload;
}

export function AddConnectionWizard({
  open,
  firstTime = false,
  editAlias = null,
  onClose,
  onCreated,
}: {
  open: boolean;
  firstTime?: boolean;
  editAlias?: string | null;
  onClose: () => void;
  onCreated: () => void;
}) {
  const isEdit = !!editAlias;
  const steps = useMemo(
    () => (isEdit ? WIZARD_STEPS.filter((s) => s.id !== "type") : WIZARD_STEPS),
    [isEdit],
  );

  const [stepIndex, setStepIndex] = useState(0);
  const [form, setForm] = useState<ConnectionForm>(() => defaultForm());
  const [error, setError] = useState("");
  const [testPassed, setTestPassed] = useState(false);
  const [testOk, setTestOk] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [loading, setLoading] = useState(false);

  const step = steps[stepIndex].id;
  const testStep: WizardStepId | null =
    form.db_engine === "mongodb" ? "additional" : "properties";
  const mustTestBeforeNext = step === testStep;
  const canAdvance = !mustTestBeforeNext || testPassed;
  const engineMeta = useMemo(
    () => ENGINE_OPTIONS.find((e) => e.id === form.db_engine),
    [form.db_engine],
  );

  useEffect(() => {
    if (!open) return;

    setError("");
    setTestPassed(false);
    setTestOk(null);
    setStepIndex(0);

    if (editAlias) {
      setLoading(true);
      fetch(`/api/connections/${encodeURIComponent(editAlias)}`)
        .then((r) => r.json())
        .then((d) => {
          if (d.error) {
            setError(d.error);
            return;
          }
          setForm({
            alias: d.alias,
            db_engine: d.db_engine,
            host: d.host,
            port: d.port,
            db_name: d.db_name,
            username: d.username,
            password: "",
            replica_set:
              typeof d.extras?.replica_set === "string" ? d.extras.replica_set : "",
            active: d.active,
          });
        })
        .finally(() => setLoading(false));
    } else {
      setForm(defaultForm());
      setLoading(false);
    }
  }, [open, editAlias]);

  if (!open) return null;

  function patch(values: Partial<ConnectionForm>) {
    setForm((prev) => ({ ...prev, ...values }));
    setError("");
    const invalidatesTest =
      "alias" in values ||
      "host" in values ||
      "port" in values ||
      "db_name" in values ||
      "username" in values ||
      "password" in values ||
      "db_engine" in values ||
      "replica_set" in values;
    if (invalidatesTest) {
      setTestPassed(false);
      setTestOk(null);
    }
  }

  function selectEngine(engine: DbEngine) {
    const opt = ENGINE_OPTIONS.find((e) => e.id === engine)!;
    patch({ db_engine: engine, port: opt.defaultPort });
  }

  function goNext() {
    const err = validateForm(form, step);
    if (err) {
      setError(err);
      return;
    }
    if (mustTestBeforeNext && !testPassed) {
      setError("Run Test connection successfully before continuing");
      return;
    }
    setStepIndex((i) => Math.min(i + 1, steps.length - 1));
  }

  function goBack() {
    setError("");
    setStepIndex((i) => Math.max(i - 1, 0));
  }

  async function testConnection() {
    const validateStep =
      form.db_engine === "mongodb" ? ("additional" as const) : ("properties" as const);
    const err = validateForm(form, validateStep);
    if (err) {
      setError(err);
      return;
    }
    setTesting(true);
    setError("");
    setTestPassed(false);
    setTestOk(null);
    try {
      const res = await fetch("/api/connections/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formToPayload(form, isEdit)),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error ?? "Test failed");
        return;
      }
      if (data.ok) {
        setTestPassed(true);
        setTestOk("CDC preflight passed");
      } else if (Array.isArray(data.reasons)) {
        setError(data.reasons.join("\n"));
      } else {
        setError("CDC preflight failed");
      }
    } finally {
      setTesting(false);
    }
  }

  async function submit() {
    const err = validateForm(form, "review");
    if (err) {
      setError(err);
      return;
    }
    setSaving(true);
    setError("");
    try {
      const payload = formToPayload(form, isEdit);
      const res = await fetch(
        isEdit
          ? `/api/connections/${encodeURIComponent(editAlias!)}`
          : "/api/connections",
        {
          method: isEdit ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        },
      );
      const data = await res.json();
      if (!res.ok) {
        if (Array.isArray(data.reasons) && data.reasons.length > 0) {
          setError(data.reasons.join("\n"));
        } else {
          setError(data.error ?? "Failed to save connection");
        }
        return;
      }
      setStepIndex(0);
      setForm(defaultForm());
      onCreated();
      onClose();
    } finally {
      setSaving(false);
    }
  }

  const shellClass = firstTime
    ? "absolute inset-0 z-10 flex flex-col"
    : "fixed inset-0 z-50 flex items-center justify-center bg-foreground/30 p-4";

  const panelClass = firstTime
    ? "panel flex min-h-[min(640px,calc(100vh-12rem))] w-full flex-1 flex-col overflow-hidden"
    : "panel flex max-h-[90vh] w-full max-w-4xl flex-col overflow-hidden";

  return (
    <div className={shellClass}>
      <div className={panelClass}>
        <div className={firstTime ? "hidden" : "border-b border-border px-4 py-2"}>
          <p className="font-mono text-[10px] text-foreground-muted">
            Connections / {isEdit ? "Edit connection" : "Create connection"}
          </p>
          <h2 className="text-[13px] font-semibold text-foreground">
            {isEdit ? editAlias : "Add connection"}
          </h2>
        </div>

        <div className="flex min-h-0 flex-1">
          <aside className="w-52 shrink-0 border-r border-border bg-surface-raised p-4">
            <ol className="space-y-3">
              {steps.map((s, i) => {
                const active = i === stepIndex;
                const done = i < stepIndex;
                return (
                  <li
                    key={s.id}
                    className={`flex items-start gap-2 border-l-2 pl-2 ${
                      active
                        ? "border-accent text-accent"
                        : done
                          ? "border-success text-foreground-secondary"
                          : "border-transparent text-foreground-muted"
                    }`}
                  >
                    <span className="font-mono text-[10px]">{i + 1}</span>
                    <span className="text-[12px] font-medium leading-snug">
                      {s.label}
                    </span>
                  </li>
                );
              })}
            </ol>
          </aside>

          <div className="flex min-h-0 min-w-0 flex-1 flex-col">
            <div className="flex-1 overflow-y-auto p-6">
              {loading ? (
                <p className="font-mono text-[11px] text-foreground-muted">
                  loading…
                </p>
              ) : null}

              {!loading && step === "type" ? (
                <div>
                  <h3 className="text-[13px] font-semibold text-foreground">
                    Configure a connector
                  </h3>
                  <p className="mt-1 text-[12px] text-foreground-muted">
                    Select the source database engine.
                  </p>
                  <div className="mt-6 grid gap-2 sm:grid-cols-3">
                    {ENGINE_OPTIONS.map((opt) => {
                      const selected = form.db_engine === opt.id;
                      return (
                        <button
                          key={opt.id}
                          type="button"
                          onClick={() => selectEngine(opt.id)}
                          className={`border px-4 py-3 text-left transition-colors ${
                            selected
                              ? "border-accent bg-accent-subtle text-accent"
                              : "border-border bg-surface hover:bg-surface-muted text-foreground"
                          }`}
                        >
                          <span className="text-[13px] font-semibold">
                            {opt.label}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}

              {!loading && step === "properties" ? (
                <div className="max-w-lg space-y-4">
                  <h3 className="text-[13px] font-semibold text-foreground">
                    Connection properties
                  </h3>
                  <CdcRequirements engine={form.db_engine} />
                  <p className="text-[12px] text-foreground-muted">
                    {engineMeta?.label} — alias becomes{" "}
                    <code className="font-mono">conn_id</code> in the catalog.
                  </p>
                  <Field label="Alias">
                    <input
                      className="input-field font-mono uppercase"
                      value={form.alias}
                      onChange={(e) =>
                        patch({ alias: e.target.value.toUpperCase() })
                      }
                      placeholder="MARIADB_PROD"
                      disabled={isEdit}
                    />
                  </Field>
                  <div className="grid gap-3 sm:grid-cols-[1fr_120px]">
                    <Field label="Host">
                      <input
                        className="input-field font-mono"
                        value={form.host}
                        onChange={(e) => patch({ host: e.target.value })}
                        placeholder="127.0.0.1"
                      />
                    </Field>
                    <Field label="Port">
                      <input
                        className="input-field font-mono"
                        type="number"
                        value={form.port}
                        onChange={(e) =>
                          patch({ port: Number(e.target.value) })
                        }
                      />
                    </Field>
                  </div>
                  <Field label="Database">
                    <input
                      className="input-field font-mono"
                      value={form.db_name}
                      onChange={(e) => patch({ db_name: e.target.value })}
                      placeholder="testdb"
                    />
                  </Field>
                  <div className="grid gap-3 sm:grid-cols-2">
                    <Field label="Username">
                      <input
                        className="input-field font-mono"
                        value={form.username}
                        onChange={(e) => patch({ username: e.target.value })}
                        autoComplete="off"
                      />
                    </Field>
                    <Field label="Password">
                      <input
                        className="input-field font-mono"
                        type="password"
                        value={form.password}
                        onChange={(e) => patch({ password: e.target.value })}
                        autoComplete="new-password"
                        placeholder={isEdit ? "leave blank to keep" : undefined}
                      />
                    </Field>
                  </div>
                  {mustTestBeforeNext && step === "properties" && !testPassed ? (
                    <p className="text-[11px] text-foreground-muted">
                      Test the connection to enable Next.
                    </p>
                  ) : null}
                  {testOk ? (
                    <p className="font-mono text-[11px] text-[var(--success)]">
                      {testOk}
                    </p>
                  ) : null}
                </div>
              ) : null}

              {!loading && step === "additional" ? (
                <div className="max-w-lg space-y-4">
                  <h3 className="text-[13px] font-semibold text-foreground">
                    Additional properties
                  </h3>
                  {form.db_engine === "mongodb" ? (
                    <Field label="Replica set">
                      <input
                        className="input-field font-mono"
                        value={form.replica_set}
                        onChange={(e) =>
                          patch({ replica_set: e.target.value })
                        }
                        placeholder="rs0"
                      />
                    </Field>
                  ) : (
                    <p className="text-[12px] text-foreground-muted">
                      No extra properties for {engineMeta?.label}. Replication is always
                      enabled — DataSync will discover, capture CDC, and run full-load jobs
                      for this source after saving.
                    </p>
                  )}
                  {mustTestBeforeNext && step === "additional" && !testPassed ? (
                    <p className="text-[11px] text-foreground-muted">
                      Test the connection to enable Next.
                    </p>
                  ) : null}
                  {testOk && step === "additional" ? (
                    <p className="font-mono text-[11px] text-[var(--success)]">
                      {testOk}
                    </p>
                  ) : null}
                </div>
              ) : null}

              {!loading && step === "review" ? (
                <div className="max-w-lg space-y-4">
                  <h3 className="text-[13px] font-semibold text-foreground">
                    Review
                  </h3>
                  <p className="text-[12px] text-foreground-muted">
                    Saving runs a live CDC preflight against the source when the
                    connection is active.
                  </p>
                  <dl className="space-y-2 border border-border bg-surface-muted p-4 font-mono text-[12px]">
                    <div className="flex gap-3">
                      <dt className="w-28 text-foreground-muted">alias</dt>
                      <dd>{form.alias.trim().toUpperCase() || "—"}</dd>
                    </div>
                    <div className="flex gap-3">
                      <dt className="w-28 text-foreground-muted">engine</dt>
                      <dd>{form.db_engine}</dd>
                    </div>
                    <div className="flex gap-3">
                      <dt className="w-28 text-foreground-muted">endpoint</dt>
                      <dd>
                        {form.host}:{form.port}/{form.db_name}
                      </dd>
                    </div>
                    <div className="flex gap-3">
                      <dt className="w-28 text-foreground-muted">user</dt>
                      <dd>{form.username || "—"}</dd>
                    </div>
                    {form.db_engine === "mongodb" && form.replica_set ? (
                      <div className="flex gap-3">
                        <dt className="w-28 text-foreground-muted">
                          replica set
                        </dt>
                        <dd>{form.replica_set}</dd>
                      </div>
                    ) : null}
                    <div className="flex gap-3">
                      <dt className="w-28 text-foreground-muted">replication</dt>
                      <dd>enabled — discover, CDC, and jobs will run</dd>
                    </div>
                  </dl>
                </div>
              ) : null}

              {error ? (
                <pre className="mt-4 whitespace-pre-wrap font-mono text-[12px] text-error">
                  {error}
                </pre>
              ) : null}
            </div>

            <div className="flex items-center justify-between border-t border-border px-6 py-3">
              <button
                type="button"
                onClick={goBack}
                disabled={stepIndex === 0 || loading}
                className="btn-secondary"
              >
                Back
              </button>
              <div className="flex items-center gap-2">
                {!firstTime ? (
                  <button type="button" onClick={onClose} className="btn-secondary">
                    Cancel
                  </button>
                ) : null}
                {mustTestBeforeNext ? (
                  <button
                    type="button"
                    onClick={testConnection}
                    disabled={testing || loading}
                    className="btn-secondary text-[11px]"
                  >
                    {testing ? "Testing…" : "Test connection"}
                  </button>
                ) : null}
                {step === "review" ? (
                  <button
                    type="button"
                    onClick={submit}
                    disabled={saving || loading}
                    className="btn-primary"
                  >
                    {saving
                      ? "Validating…"
                      : isEdit
                        ? "Save changes"
                        : "Create connection"}
                  </button>
                ) : (
                  <button
                    type="button"
                    onClick={goNext}
                    disabled={loading || !canAdvance}
                    title={
                      !canAdvance
                        ? "Run Test connection successfully first"
                        : undefined
                    }
                    className="btn-primary"
                  >
                    Next
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
