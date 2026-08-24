interface BadgeProps {
  children: React.ReactNode;
  tone?: "neutral" | "accent" | "success" | "warning" | "error" | "outline";
  title?: string;
}

export function Badge({ children, tone = "neutral", title }: BadgeProps) {
  const styles = {
    neutral: "bg-surface-muted text-foreground-secondary",
    accent: "bg-accent-subtle text-accent",
    success: "bg-success-subtle text-success",
    warning: "bg-warning-subtle text-warning",
    error: "bg-error-subtle text-error",
    outline: "border-border bg-surface text-foreground-muted",
  };

  return (
    <span className={`tag ${styles[tone]}`} title={title}>
      {children}
    </span>
  );
}
