import type { DbEngine } from "./connection-types";
import { validateConnectionCdc } from "./connection-preflight";

export const CONNECTION_ENGINES = new Set<string>(["mariadb", "mssql", "mongodb"]);

export interface ConnectionPayload {
  alias: string;
  db_engine: DbEngine;
  host: string;
  port: number;
  db_name: string;
  username: string;
  password: string;
  extras: Record<string, unknown>;
  active: boolean;
}

export function normalizeAlias(raw: string): string {
  return raw.trim().toUpperCase();
}

export function parseConnectionBody(body: {
  alias?: string;
  db_engine?: string;
  host?: string;
  port?: number;
  db_name?: string;
  username?: string;
  password?: string;
  extras?: Record<string, unknown>;
  active?: boolean;
}): { ok: true; value: ConnectionPayload } | { ok: false; error: string } {
  const alias = normalizeAlias(body.alias ?? "");
  const db_engine = body.db_engine ?? "";
  const host = body.host?.trim() ?? "";
  const port = Number(body.port);
  const db_name = body.db_name?.trim() ?? "";
  const username = body.username?.trim() ?? "";
  const password = body.password ?? "";
  const active = true;
  const extras = body.extras ?? {};

  if (!alias || !/^[A-Z0-9_]+$/.test(alias)) {
    return { ok: false, error: "Alias required (A-Z, 0-9, underscore)" };
  }
  if (!CONNECTION_ENGINES.has(db_engine)) {
    return { ok: false, error: "Invalid db_engine" };
  }
  if (!host || !db_name) {
    return { ok: false, error: "Host and database are required" };
  }
  if (!Number.isFinite(port) || port <= 0 || port > 65535) {
    return { ok: false, error: "Invalid port" };
  }
  if (db_engine === "mongodb") {
    const rs = extras.replica_set;
    if (typeof rs !== "string" || !rs.trim()) {
      return { ok: false, error: "MongoDB requires extras.replica_set" };
    }
  }

  return {
    ok: true,
    value: {
      alias,
      db_engine: db_engine as DbEngine,
      host,
      port,
      db_name,
      username,
      password,
      extras,
      active,
    },
  };
}

export async function runConnectionPreflight(payload: {
  db_engine: DbEngine;
  host: string;
  port: number;
  db_name: string;
  username: string;
  password: string;
  extras?: Record<string, unknown>;
}) {
  return validateConnectionCdc({
    db_engine: payload.db_engine,
    host: payload.host,
    port: payload.port,
    db_name: payload.db_name,
    username: payload.username,
    password: payload.password,
    extras: payload.extras as { replica_set?: string },
  });
}

export async function logConnectionEvent(message: string, alias: string) {
  const { mutate } = await import("./db");
  await mutate(
    `
    INSERT INTO cdc_catalog.logs (level, component, message, conn_id)
    VALUES ('info', 'control_plane', $1, $2)
  `,
    [message, alias],
  ).catch(() => {});
}
