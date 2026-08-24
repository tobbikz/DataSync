import { Pool, type QueryResultRow } from "pg";
import { getDatasyncDbConfig } from "./config";

let pool: Pool | null = null;

function getPool(): Pool {
  if (!pool) {
    const cfg = getDatasyncDbConfig();
    pool = new Pool({
      host: cfg.host,
      port: cfg.port,
      database: cfg.database,
      user: cfg.user,
      password: cfg.password,
      ssl: cfg.sslmode === "require" ? { rejectUnauthorized: false } : undefined,
      max: 8,
      idleTimeoutMillis: 30_000,
    });
  }
  return pool;
}

export type QueryResult<T extends QueryResultRow> = {
  rows: T[];
  ok: boolean;
  error?: string;
};

export async function query<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
): Promise<QueryResult<T>> {
  try {
    const result = await getPool().query<T>(text, params);
    return { rows: result.rows, ok: true };
  } catch (err) {
    console.error("[db]", err);
    const message = err instanceof Error ? err.message : "Database error";
    return { rows: [] as T[], ok: false, error: message };
  }
}

export async function mutate<T extends QueryResultRow = QueryResultRow>(
  text: string,
  params?: unknown[],
): Promise<{ ok: true; rows: T[] } | { ok: false; error: string }> {
  try {
    const result = await getPool().query<T>(text, params);
    return { ok: true, rows: result.rows };
  } catch (err) {
    console.error("[db]", err);
    const message = err instanceof Error ? err.message : "Database error";
    return { ok: false, error: message };
  }
}

export async function pingDb(): Promise<boolean> {
  try {
    await getPool().query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}
