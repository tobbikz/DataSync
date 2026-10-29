"use client";

import { useCallback, useEffect, useState } from "react";

function linesToList(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function listToLines(list: unknown): string {
  if (!Array.isArray(list)) return "";
  return list.map(String).join("\n");
}

/** U4 + U5: pause/resume capture + table include/exclude filters. */
export function ConnectionCaptureControls({
  alias,
  active,
  onChanged,
}: {
  alias: string;
  active: boolean;
  onChanged?: () => void;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [includeText, setIncludeText] = useState("");
  const [excludeText, setExcludeText] = useState("");
  const [onlineDdl, setOnlineDdl] = useState(false);
  const [loaded, setLoaded] = useState(false);

  const loadFilters = useCallback(() => {
    fetch(`/api/connections/${encodeURIComponent(alias)}/filters`)
      .then((r) => r.json())
      .then((d) => {
        if (d.error) {
          setError(d.error);
          return;
        }
        setIncludeText(listToLines(d.table_include));
        setExcludeText(listToLines(d.table_exclude));
        setOnlineDdl(!!d.capture_online_ddl_helpers);
        setLoaded(true);
      })
      .catch((e) => setError(String(e)));
  }, [alias]);

  useEffect(() => {
    loadFilters();
  }, [loadFilters]);

  async function setCapture(action: "pause" | "resume") {
    setBusy(true);
    setError("");
    const res = await fetch(
      `/api/connections/${encodeURIComponent(alias)}/capture`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, confirm: true }),
      },
    );
    const data = await res.json();
    if (!res.ok) setError(data.error ?? "Capture action failed");
    else onChanged?.();
    setBusy(false);
  }

  async function saveFilters() {
    setBusy(true);
    setError("");
    const res = await fetch(
      `/api/connections/${encodeURIComponent(alias)}/filters`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          confirm: true,
          table_include: linesToList(includeText),
          table_exclude: linesToList(excludeText),
          capture_online_ddl_helpers: onlineDdl,
        }),
      },
    );
    const data = await res.json();
    if (!res.ok) setError(data.error ?? "Filter save failed");
    setBusy(false);
  }

  return (
    <section className="rounded border border-border bg-surface p-2 space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <p className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
          Capture
        </p>
        <span className="font-mono text-[10px] text-foreground-muted">
          {active ? "active" : "paused"}
        </span>
        <div className="ml-auto flex gap-1.5">
          <button
            type="button"
            disabled={busy || !active}
            onClick={() => setCapture("pause")}
            className="btn-secondary py-0.5 text-[10px]"
          >
            Pause
          </button>
          <button
            type="button"
            disabled={busy || active}
            onClick={() => setCapture("resume")}
            className="btn-primary py-0.5 text-[10px]"
          >
            Resume
          </button>
        </div>
      </div>

      <p className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
        Table filters (regex, one per line)
      </p>
      {!loaded ? (
        <p className="font-mono text-[10px] text-foreground-muted">loading…</p>
      ) : (
        <div className="grid gap-2 md:grid-cols-2">
          <label className="text-[10px]">
            table_include
            <textarea
              className="mt-0.5 h-20 w-full border border-border bg-surface-muted px-1.5 py-1 font-mono text-[11px]"
              value={includeText}
              onChange={(e) => setIncludeText(e.target.value)}
              placeholder="schema\.orders|billing\..*"
            />
          </label>
          <label className="text-[10px]">
            table_exclude
            <textarea
              className="mt-0.5 h-20 w-full border border-border bg-surface-muted px-1.5 py-1 font-mono text-[11px]"
              value={excludeText}
              onChange={(e) => setExcludeText(e.target.value)}
              placeholder=".*_tmp|audit\..*"
            />
          </label>
        </div>
      )}
      <label className="flex items-center gap-2 font-mono text-[11px]">
        <input
          type="checkbox"
          checked={onlineDdl}
          onChange={(e) => setOnlineDdl(e.target.checked)}
        />
        capture_online_ddl_helpers (gh-ost / pt-osc)
      </label>
      <button
        type="button"
        disabled={busy || !loaded}
        onClick={saveFilters}
        className="btn-secondary py-0.5 text-[10px]"
      >
        Save filters
      </button>
      {error ? (
        <p className="font-mono text-[10px] text-error">{error}</p>
      ) : null}
    </section>
  );
}
