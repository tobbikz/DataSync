import net from "net";
import { Pool } from "pg";
import { runnerAvailable } from "./actions";
import { type DbConfig, getDatasyncDbConfig, getDatalakeDbConfig } from "./config";

function checkTcp(host: string, port: number, timeoutMs = 2000): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.connect({ host, port }, () => {
      socket.destroy();
      resolve(true);
    });
    socket.on("error", () => resolve(false));
    socket.setTimeout(timeoutMs, () => {
      socket.destroy();
      resolve(false);
    });
  });
}

async function pingDatabase(
  cfg: DbConfig,
): Promise<{ ok: boolean; latencyMs: number | null; database: string }> {
  const started = Date.now();
  const pool = new Pool({
    host: cfg.host,
    port: cfg.port,
    database: cfg.database,
    user: cfg.user,
    password: cfg.password,
    ssl: cfg.sslmode === "require" ? { rejectUnauthorized: false } : undefined,
    max: 1,
    connectionTimeoutMillis: 2000,
  });

  try {
    await pool.query("SELECT 1");
    return {
      ok: true,
      latencyMs: Date.now() - started,
      database: cfg.database,
    };
  } catch {
    return { ok: false, latencyMs: null, database: cfg.database };
  } finally {
    await pool.end().catch(() => {});
  }
}

async function fetchPipelineSignals(cfg: DbConfig): Promise<{
  needsFullLoad: number;
  quarantined: number;
  applyRed: number;
}> {
  const pool = new Pool({
    host: cfg.host,
    port: cfg.port,
    database: cfg.database,
    user: cfg.user,
    password: cfg.password,
    ssl: cfg.sslmode === "require" ? { rejectUnauthorized: false } : undefined,
    max: 1,
    connectionTimeoutMillis: 3000,
  });

  try {
    const { rows } = await pool.query<{
      needs_full_load: number;
      quarantined: number;
      apply_red: number;
    }>(`
      SELECT
        (SELECT COUNT(*)::int FROM cdc_catalog.catalog
           WHERE active AND needs_full_load) AS needs_full_load,
        (SELECT COUNT(*)::int FROM cdc_catalog.catalog
           WHERE active AND status = 'quarantined'::cdc_catalog.replication_status) AS quarantined,
        (SELECT COUNT(*)::int FROM (
           SELECT DISTINCT ON (s.catalog_id) s.apply_health_rag
           FROM cdc_catalog.apply_batch_stats s
           WHERE s.logged_at >= now() - interval '24 hours'
           ORDER BY s.catalog_id, s.logged_at DESC
         ) latest WHERE apply_health_rag = 'RED') AS apply_red
    `);
    const row = rows[0] ?? {};
    return {
      needsFullLoad: Number(row.needs_full_load ?? 0),
      quarantined: Number(row.quarantined ?? 0),
      applyRed: Number(row.apply_red ?? 0),
    };
  } catch {
    return { needsFullLoad: 0, quarantined: 0, applyRed: 0 };
  } finally {
    await pool.end().catch(() => {});
  }
}

export async function checkKafka(): Promise<{
  ok: boolean;
  host: string;
  port: number;
}> {
  const bootstrap = process.env.KAFKA_BOOTSTRAP ?? "127.0.0.1:9092";
  const [host, portStr] = bootstrap.split(":");
  const port = Number(portStr ?? 9092);
  const ok = await checkTcp(host, port);
  return { ok, host, port };
}

export async function getHealthStatus() {
  const datasyncCfg = getDatasyncDbConfig();
  const [datasync, datalake, kafka, pipeline] = await Promise.all([
    pingDatabase(datasyncCfg),
    pingDatabase(getDatalakeDbConfig()),
    checkKafka(),
    fetchPipelineSignals(datasyncCfg),
  ]);

  const pipelineOk =
    pipeline.needsFullLoad === 0 &&
    pipeline.quarantined === 0 &&
    pipeline.applyRed === 0;

  return {
    datasync,
    datalake,
    kafka,
    cli: { ok: runnerAvailable() },
    pipeline,
    pipelineOk,
    checkedAt: new Date().toISOString(),
  };
}

export type HealthStatus = Awaited<ReturnType<typeof getHealthStatus>>;
