export type CdcTuning = {
  round_idle_seconds?: number;
  slice_max_seconds?: number;
  slice_max_events?: number;
  capture_worker_count?: number;
  apply_dedup?: boolean;
  apply_audit?: boolean;
  mssql_replay_on_idle?: boolean;
  kafka_heartbeat?: boolean;
  binlog_mode?: string;
  kafka_tombstones?: boolean;
  reselect_on_apply?: boolean;
  cloudevents?: boolean;
  tracing?: boolean;
  openlineage?: boolean;
  include_source_query?: boolean;
  schema_events?: boolean;
  content_routing?: unknown[];
  message_filter_exclude_ops?: unknown[];
};

export const CDC_TUNING_KEYS: (keyof CdcTuning)[] = [
  "round_idle_seconds",
  "slice_max_seconds",
  "slice_max_events",
  "capture_worker_count",
  "apply_dedup",
  "apply_audit",
  "mssql_replay_on_idle",
  "kafka_heartbeat",
  "binlog_mode",
  "kafka_tombstones",
  "reselect_on_apply",
  "cloudevents",
  "tracing",
  "openlineage",
  "include_source_query",
  "schema_events",
  "content_routing",
  "message_filter_exclude_ops",
];
