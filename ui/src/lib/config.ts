import fs from "fs";
import path from "path";

export interface DbConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  sslmode?: string;
}

export function getDatasyncDbConfig(): DbConfig {
  if (process.env.DATASYNC_PG_HOST) {
    return {
      host: process.env.DATASYNC_PG_HOST,
      port: Number(process.env.DATASYNC_PG_PORT ?? "5432"),
      database: process.env.DATASYNC_PG_DATABASE ?? "datasync",
      user: process.env.DATASYNC_PG_USER ?? "",
      password: process.env.DATASYNC_PG_PASSWORD ?? "",
      sslmode: process.env.DATASYNC_PG_SSLMODE,
    };
  }

  const configPath =
    process.env.DATASYNC_CONFIG ??
    path.resolve(process.cwd(), "..", "config.json");

  const raw = JSON.parse(fs.readFileSync(configPath, "utf-8")) as {
    datasync: DbConfig;
  };

  return raw.datasync;
}

export function getDatalakeDbConfig(): DbConfig {
  if (process.env.DATALAKE_PG_HOST) {
    return {
      host: process.env.DATALAKE_PG_HOST,
      port: Number(process.env.DATALAKE_PG_PORT ?? "5432"),
      database: process.env.DATALAKE_PG_DATABASE ?? "datalake",
      user: process.env.DATALAKE_PG_USER ?? "",
      password: process.env.DATALAKE_PG_PASSWORD ?? "",
      sslmode: process.env.DATALAKE_PG_SSLMODE,
    };
  }

  const configPath =
    process.env.DATASYNC_CONFIG ??
    path.resolve(process.cwd(), "..", "config.json");

  const raw = JSON.parse(fs.readFileSync(configPath, "utf-8")) as {
    datalake: DbConfig;
  };

  return raw.datalake;
}
