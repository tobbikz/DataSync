export interface CatalogRow {
  catalog_id: number;
  conn_id: string;
  db_engine: string;
  source_schema: string;
  source_table: string;
  active: boolean;
  cdc_enabled: boolean;
  capture_during_full_load: boolean;
  status: string;
  needs_full_load: boolean;
  last_full_load_at: string | null;
  health_rag: string;
  kafka_lag: number | null;
  capture_lag_seconds: number | null;
  apply_lag_seconds: number | null;
  quarantined: boolean;
  quarantine_reason: string | null;
  last_apply_at: string | null;
  health_reason: string | null;
}

export interface CatalogListResponse {
  items: CatalogRow[];
  total: number;
  page: number;
  limit: number;
}

export interface CatalogDetailResponse {
  catalog: CatalogRow;
  apply_position: {
    status: string;
    kafka_topic: string | null;
    kafka_partition: number | null;
    kafka_offset: number | null;
    apply_lag_seconds: number | null;
    last_applied_at: string | null;
    last_error: string | null;
    quarantine_reason: string | null;
    quarantined_at: string | null;
  } | null;
  recent_stats: {
    logged_at: string;
    events_total: number | null;
    kafka_consumer_lag: number | null;
    apply_health_rag: string | null;
    health_reason: string | null;
  }[];
  recent_logs: {
    logged_at: string;
    level: string;
    component: string;
    message: string;
  }[];
}

export interface CatalogFilters {
  conn?: string;
  status?: string;
  cdc?: boolean;
  rag?: string;
  quarantined?: boolean;
  needsFullLoad?: boolean;
  q?: string;
  limit?: number;
  offset?: number;
}

export interface OverviewSummary {
  connections: number;
  tables_active: number;
  tables_cdc: number;
  errors_24h: number;
  rag_red: number;
  rag_amber: number;
  rag_green: number;
  quarantined: number;
  needs_full_load: number;
}

export interface OverviewResponse {
  summary: OverviewSummary;
  queue: CatalogRow[];
}
