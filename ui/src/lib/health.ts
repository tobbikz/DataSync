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
  const [datasync, datalake, kafka] = await Promise.all([
    pingDatabase(getDatasyncDbConfig()),
    pingDatabase(getDatalakeDbConfig()),
    checkKafka(),
  ]);

  return {
    datasync,
    datalake,
    kafka,
    cli: { ok: runnerAvailable() },
    checkedAt: new Date().toISOString(),
  };
}

export type HealthStatus = Awaited<ReturnType<typeof getHealthStatus>>;
