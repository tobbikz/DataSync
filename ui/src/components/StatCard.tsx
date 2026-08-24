interface StatCardProps {
  label: string;
  value: string;
  delta?: string;
  deltaTone?: "success" | "warning" | "neutral";
}

export function StatCard({
  label,
  value,
  delta,
  deltaTone = "neutral",
}: StatCardProps) {
  const stripe = {
    success: "border-l-success",
    warning: "border-l-warning",
    neutral: "border-l-border-strong",
  };

  const deltaStyles = {
    success: "text-success",
    warning: "text-warning",
    neutral: "text-foreground-muted",
  };

  return (
    <div className={`panel border-l-[3px] ${stripe[deltaTone]} p-4`}>
      <p className="text-[11px] font-medium uppercase tracking-wide text-foreground-muted">
        {label}
      </p>
      <div className="mt-1 flex items-baseline justify-between gap-2">
        <p className="font-mono text-2xl font-semibold tabular-nums tracking-tight text-foreground">
          {value}
        </p>
        {delta ? (
          <span className={`text-[11px] font-medium ${deltaStyles[deltaTone]}`}>
            {delta}
          </span>
        ) : null}
      </div>
    </div>
  );
}
