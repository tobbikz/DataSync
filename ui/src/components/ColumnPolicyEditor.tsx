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

function maskToText(mask: unknown): string {
  if (!mask || typeof mask !== "object") return "";
  return Object.entries(mask as Record<string, string>)
    .map(([k, v]) => `${k}:${v}`)
    .join("\n");
}

function textToMask(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of linesToList(text)) {
    const idx = line.indexOf(":");
    if (idx <= 0) {
      out[line] = "hash";
    } else {
      out[line.slice(0, idx).trim()] = line.slice(idx + 1).trim() || "hash";
    }
  }
  return out;
}

/** U4: per-table column include/exclude/mask on engine_meta. */
export function ColumnPolicyEditor({ catalogId }: { catalogId: number }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [includeText, setIncludeText] = useState("");
  const [excludeText, setExcludeText] = useState("");
  const [maskText, setMaskText] = useState("");
  const [loaded, setLoaded] = useState(false);

  const load = useCallback(() => {
    fetch(`/api/catalog/${catalogId}/policy`)
      .then((r) => r.json())
      .then((d) => {
        if (d.error) {
          setError(d.error);
          return;
        }
        setIncludeText(listToLines(d.column_include));
        setExcludeText(listToLines(d.column_exclude));
        setMaskText(maskToText(d.column_mask));
        setLoaded(true);
      })
      .catch((e) => setError(String(e)));
  }, [catalogId]);

  useEffect(() => {
    load();
  }, [load]);

  async function save() {
    setBusy(true);
    setError("");
    const res = await fetch(`/api/catalog/${catalogId}/policy`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        confirm: true,
        column_include: linesToList(includeText),
        column_exclude: linesToList(excludeText),
        column_mask: textToMask(maskText),
      }),
    });
    const data = await res.json();
    if (!res.ok) setError(data.error ?? "Policy save failed");
    setBusy(false);
  }

  return (
    <section className="rounded border border-border bg-surface-muted/40 p-2 space-y-2">
      <p className="text-[10px] font-semibold uppercase tracking-wide text-foreground-muted">
        Column policy (engine_meta)
      </p>
      {!loaded ? (
        <p className="font-mono text-[10px] text-foreground-muted">loading…</p>
      ) : (
        <div className="grid gap-2 md:grid-cols-3">
          <label className="text-[10px]">
            column_include
            <textarea
              className="mt-0.5 h-16 w-full border border-border bg-surface px-1.5 py-1 font-mono text-[11px]"
              value={includeText}
              onChange={(e) => setIncludeText(e.target.value)}
              placeholder="id&#10;email"
            />
          </label>
          <label className="text-[10px]">
            column_exclude
            <textarea
              className="mt-0.5 h-16 w-full border border-border bg-surface px-1.5 py-1 font-mono text-[11px]"
              value={excludeText}
              onChange={(e) => setExcludeText(e.target.value)}
              placeholder="password_hash"
            />
          </label>
          <label className="text-[10px]">
            column_mask (col:hash)
            <textarea
              className="mt-0.5 h-16 w-full border border-border bg-surface px-1.5 py-1 font-mono text-[11px]"
              value={maskText}
              onChange={(e) => setMaskText(e.target.value)}
              placeholder="email:hash&#10;ssn:hash"
            />
          </label>
        </div>
      )}
      <button
        type="button"
        disabled={busy || !loaded}
        onClick={save}
        className="btn-secondary py-0.5 text-[10px]"
      >
        Save column policy
      </button>
      {error ? (
        <p className="font-mono text-[10px] text-error">{error}</p>
      ) : null}
    </section>
  );
}
