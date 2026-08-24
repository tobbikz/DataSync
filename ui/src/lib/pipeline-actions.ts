import type { ActionKind } from "./actions";

export interface PipelineActionDef {
  id: ActionKind;
  label: string;
  cli: string;
  description: string;
  impact: string;
  requiresConn: boolean;
  connOptional?: boolean;
  supportsSkipOnboard?: boolean;
  supportsSchemaTable?: boolean;
  daemonOnce?: boolean;
  danger: "low" | "medium" | "high";
}

export const PIPELINE_ACTIONS: PipelineActionDef[] = [
  {
    id: "discover",
    label: "Discover",
    cli: "discover",
    description: "Scan active connections and upsert catalog entries.",
    impact: "Safe to run periodically. No data reload.",
    requiresConn: false,
    danger: "low",
  },
  {
    id: "full-load",
    label: "Full load",
    cli: "full-load [--conn-id ID] [--skip-onboard]",
    description: "Bulk copy source tables into the lake.",
    impact: "Heavy — may reload large tables. Can run for one connection or all.",
    requiresConn: false,
    connOptional: true,
    supportsSkipOnboard: true,
    danger: "high",
  },
  {
    id: "onboard-pending",
    label: "Onboard pending",
    cli: "onboard-pending [--conn-id ID]",
    description: "Enable CDC capture for catalog tables waiting onboarding.",
    impact: "Prepares all pending tables for capture (Kafka reset + cdc enable).",
    requiresConn: false,
    connOptional: true,
    danger: "medium",
  },
  {
    id: "ddl-sync",
    label: "DDL sync",
    cli: "ddl-sync --conn-id ID [--schema S] [--table T]",
    description: "Apply schema DDL changes from source to lake.",
    impact: "Runs DDL alignment for one connection, optionally scoped to schema/table.",
    requiresConn: true,
    supportsSchemaTable: true,
    danger: "medium",
  },
  {
    id: "kafka-apply",
    label: "Kafka apply",
    cli: "kafka-apply --conn-id ID",
    description: "Consume Kafka topics and apply events to the lake for one connection.",
    impact: "Runs the apply worker pool once for the selected source.",
    requiresConn: true,
    danger: "medium",
  },
  {
    id: "capture",
    label: "Capture",
    cli: "capture --conn-id ID",
    description: "Run one CDC capture slice for a connection (binlog / LSN / change streams).",
    impact: "Publishes change events to Kafka for the selected source.",
    requiresConn: true,
    danger: "medium",
  },
  {
    id: "daemon",
    label: "Daemon cycle",
    cli: "daemon --once",
    description: "Run a single daemon orchestration cycle (discover + capture + apply loop).",
    impact: "One-shot daemon tick. Continuous daemon runs via systemd/Docker, not the UI.",
    requiresConn: false,
    daemonOnce: true,
    danger: "medium",
  },
];

export function pipelineActionById(id: ActionKind) {
  return PIPELINE_ACTIONS.find((a) => a.id === id);
}
