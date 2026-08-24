export function HealthDot({ rag }: { rag: string }) {
  const tone =
    rag === "GREEN"
      ? "status-dot--ok"
      : rag === "AMBER"
        ? "status-dot--warn"
        : rag === "RED"
          ? "status-dot--error"
          : "status-dot--idle";

  return (
    <span
      className={`status-dot ${tone}`}
      title={rag}
      aria-label={`health ${rag}`}
    />
  );
}

function fmtLag(n: number | null) {
  if (n == null) return "—";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtSeconds(n: number | null) {
  if (n == null) return "—";
  if (n >= 3600) return `${Math.round(n / 3600)}h`;
  if (n >= 60) return `${Math.round(n / 60)}m`;
  return `${n}s`;
}

export { fmtLag, fmtSeconds };
