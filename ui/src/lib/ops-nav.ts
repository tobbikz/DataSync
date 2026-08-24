export function isMainOpsTab(value: string | null): value is "health" | "catalog" | "kafka" | "events" | "logs" {
  return value === "health" || value === "catalog" || value === "kafka" || value === "events" || value === "logs";
}
