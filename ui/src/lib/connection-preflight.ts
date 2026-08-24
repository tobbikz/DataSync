import type { DbEngine } from "./connection-types";

export interface PreflightInput {
  db_engine: DbEngine;
  host: string;
  port: number;
  db_name: string;
  username: string;
  password: string;
  extras?: { replica_set?: string };
}

export interface PreflightResult {
  ok: boolean;
  reasons: string[];
}

function enabledFlag(value: string | undefined) {
  return (value ?? "").toUpperCase() === "ON" || value === "1";
}

async function checkMariaDb(input: PreflightInput): Promise<PreflightResult> {
  const mysql = await import("mysql2/promise");
  const reasons: string[] = [];

  let conn;
  try {
    conn = await mysql.createConnection({
      host: input.host,
      port: input.port,
      user: input.username || undefined,
      password: input.password || undefined,
      database: input.db_name,
      connectTimeout: 8_000,
    });
  } catch (err) {
    return {
      ok: false,
      reasons: [
        `Cannot connect to MariaDB/MySQL: ${err instanceof Error ? err.message : "connection failed"}`,
      ],
    };
  }

  try {
    const [rows] = (await conn.query(
      "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format')",
    )) as [Array<{ Variable_name: string; Value: string }>, unknown];

    const vars = Object.fromEntries(
      rows.map((r) => [r.Variable_name, r.Value]),
    );

    if (!enabledFlag(vars.log_bin)) {
      reasons.push(`log_bin=${vars.log_bin ?? "?"} (required ON)`);
    }
    if ((vars.binlog_format ?? "").toUpperCase() !== "ROW") {
      reasons.push(
        `binlog_format=${vars.binlog_format ?? "?"} (required ROW)`,
      );
    }

    try {
      const [master] = (await conn.query("SHOW MASTER STATUS")) as [
        unknown[],
        unknown,
      ];
      if (!Array.isArray(master) || master.length === 0) {
        reasons.push(
          "SHOW MASTER STATUS returned no rows — binlog not available to this user",
        );
      }
    } catch (err) {
      reasons.push(
        `SHOW MASTER STATUS failed: ${err instanceof Error ? err.message : "error"}`,
      );
    }
  } finally {
    await conn.end().catch(() => {});
  }

  return { ok: reasons.length === 0, reasons };
}

async function checkMssql(input: PreflightInput): Promise<PreflightResult> {
  const mssql = await import("mssql");
  const reasons: string[] = [];

  let pool: import("mssql").ConnectionPool | undefined;
  try {
    pool = await mssql.connect({
      server: input.host,
      port: input.port,
      database: input.db_name,
      user: input.username || undefined,
      password: input.password || undefined,
      options: {
        encrypt: false,
        trustServerCertificate: true,
      },
      connectionTimeout: 8_000,
      requestTimeout: 8_000,
    });
  } catch (err) {
    return {
      ok: false,
      reasons: [
        `Cannot connect to SQL Server: ${err instanceof Error ? err.message : "connection failed"}`,
      ],
    };
  }

  try {
    const cdc = await pool.request().query(
      "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()",
    );

    const row = cdc.recordset[0] as
      | { is_cdc_enabled?: boolean | number | string }
      | undefined;
    if (!row) {
      reasons.push("is_cdc_enabled not found for target database");
    } else {
      const enabled =
        row.is_cdc_enabled === true ||
        row.is_cdc_enabled === 1 ||
        row.is_cdc_enabled === "1";
      if (!enabled) {
        reasons.push(
          `CDC disabled on database ${input.db_name} (is_cdc_enabled=0) — enable CDC before adding this source`,
        );
      }
    }
  } catch (err) {
    reasons.push(
      `CDC check failed: ${err instanceof Error ? err.message : "query error"}`,
    );
  } finally {
    await pool?.close().catch(() => {});
  }

  return { ok: reasons.length === 0, reasons };
}

async function checkMongoDb(input: PreflightInput): Promise<PreflightResult> {
  const { MongoClient } = await import("mongodb");
  const reasons: string[] = [];
  const replicaSet = input.extras?.replica_set?.trim() ?? "";

  if (!replicaSet) {
    return {
      ok: false,
      reasons: ["replica_set is required for MongoDB change streams"],
    };
  }

  const auth =
    input.username && input.password
      ? `${encodeURIComponent(input.username)}:${encodeURIComponent(input.password)}@`
      : input.username
        ? `${encodeURIComponent(input.username)}@`
        : "";

  const uri = `mongodb://${auth}${input.host}:${input.port}/${input.db_name}?replicaSet=${encodeURIComponent(replicaSet)}`;

  let client: InstanceType<typeof MongoClient> | undefined;
  try {
    client = new MongoClient(uri, {
      serverSelectionTimeoutMS: 8_000,
      connectTimeoutMS: 8_000,
    });
    await client.connect();
    await client.db("admin").command({ ping: 1 });

    const hello = (await client
      .db("admin")
      .command({ hello: 1 })) as { setName?: string; msg?: string };

    if (!hello.setName) {
      reasons.push(
        "MongoDB is not a replica set — change streams require replSet/sharded cluster",
      );
    } else if (hello.setName !== replicaSet) {
      reasons.push(
        `replica_set mismatch: expected ${replicaSet}, server reports ${hello.setName}`,
      );
    }
  } catch (err) {
    return {
      ok: false,
      reasons: [
        `Cannot connect to MongoDB or validate replica set: ${err instanceof Error ? err.message : "connection failed"}`,
      ],
    };
  } finally {
    await client?.close().catch(() => {});
  }

  return { ok: reasons.length === 0, reasons };
}

export async function validateConnectionCdc(
  input: PreflightInput,
): Promise<PreflightResult> {
  if (input.db_engine === "mariadb") {
    return checkMariaDb(input);
  }
  if (input.db_engine === "mssql") {
    return checkMssql(input);
  }
  return checkMongoDb(input);
}
