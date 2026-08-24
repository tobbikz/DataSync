import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";

export type ActionKind =
  | "discover"
  | "full-load"
  | "onboard-pending"
  | "ddl-sync"
  | "kafka-apply"
  | "capture"
  | "daemon";

export interface ActionJob {
  id: string;
  action: ActionKind;
  status: "running" | "completed" | "failed";
  startedAt: string;
  finishedAt?: string;
  exitCode?: number;
  connId?: string;
  skipOnboard?: boolean;
  schema?: string;
  table?: string;
  once?: boolean;
  command: string;
  output: string;
}

const jobs = new Map<string, ActionJob>();
const MAX_JOBS = 50;

function trimJobs() {
  if (jobs.size <= MAX_JOBS) return;
  const sorted = [...jobs.values()].sort(
    (a, b) => new Date(a.startedAt).getTime() - new Date(b.startedAt).getTime(),
  );
  while (jobs.size > MAX_JOBS) {
    const oldest = sorted.shift();
    if (oldest) jobs.delete(oldest.id);
  }
}

function repoRoot(): string {
  return process.env.DATASYNC_ROOT ?? path.resolve(process.cwd(), "..");
}

function configPath(): string {
  return process.env.DATASYNC_CONFIG ?? path.join(repoRoot(), "config.json");
}

function containerCli(): string | null {
  const mounted = "/usr/local/bin/container";
  if (fs.existsSync(mounted)) {
    return mounted;
  }
  const fromEnv = process.env.DATASYNC_CONTAINER_CMD?.trim();
  if (fromEnv) {
    return fromEnv;
  }
  for (const candidate of ["docker", "podman"]) {
    const resolved = path.join("/usr/bin", candidate);
    if (fs.existsSync(resolved)) {
      return candidate;
    }
  }
  return null;
}

function resolveRunner(): { executable: string; prefix: string[]; cwd?: string } | null {
  if (process.env.DATASYNC_BIN && fs.existsSync(process.env.DATASYNC_BIN)) {
    return { executable: process.env.DATASYNC_BIN, prefix: [] };
  }

  const localBin = path.join(repoRoot(), "cpp/build/DataSync");
  if (fs.existsSync(localBin)) {
    return { executable: localBin, prefix: [] };
  }

  const composeFile =
    process.env.DATASYNC_COMPOSE_FILE ?? path.join(repoRoot(), "docker-compose.yml");
  const cli = containerCli();
  if (cli && fs.existsSync(composeFile)) {
    return {
      executable: cli,
      prefix: ["compose", "-f", composeFile, "run", "--rm", "--no-deps", "datasync"],
      cwd: path.dirname(composeFile),
    };
  }

  return null;
}

export function listJobs(): ActionJob[] {
  return [...jobs.values()].sort(
    (a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime(),
  );
}

export function getJob(id: string): ActionJob | undefined {
  return jobs.get(id);
}

export function validateConnId(connId: string): boolean {
  return /^[A-Za-z0-9_-]+$/.test(connId);
}

export function validateSchemaTable(value: string): boolean {
  return /^[A-Za-z0-9_]+$/.test(value);
}

export function startAction(input: {
  action: ActionKind;
  connId?: string;
  skipOnboard?: boolean;
  schema?: string;
  table?: string;
}): { job: ActionJob } | { error: string } {
  const runner = resolveRunner();
  if (!runner) {
    return {
      error:
        "DataSync binary not found. Set DATASYNC_BIN or build cpp/build/DataSync, or use Docker.",
    };
  }

  if (!fs.existsSync(configPath())) {
    return { error: `Config not found: ${configPath()}` };
  }

  const connRequired = ["ddl-sync", "kafka-apply", "capture"].includes(input.action);
  if (connRequired && !input.connId) {
    return { error: `${input.action} requires connId` };
  }

  if (input.connId && !validateConnId(input.connId)) {
    return { error: "Invalid connId" };
  }

  if (input.schema && !validateSchemaTable(input.schema)) {
    return { error: "Invalid schema" };
  }

  if (input.table && !validateSchemaTable(input.table)) {
    return { error: "Invalid table" };
  }

  const args = [...runner.prefix];

  if (runner.prefix.length === 0) {
    args.push("--config", configPath());
  }

  args.push(input.action);

  if (input.connId) {
    args.push("--conn-id", input.connId);
  }

  if (input.action === "full-load" && input.skipOnboard) {
    args.push("--skip-onboard");
  }

  if (input.action === "ddl-sync") {
    if (input.schema) args.push("--schema", input.schema);
    if (input.table) args.push("--table", input.table);
  }

  if (input.action === "daemon") {
    args.push("--once");
  }

  const command = `${runner.executable} ${args.join(" ")}`;
  const job: ActionJob = {
    id: randomUUID(),
    action: input.action,
    status: "running",
    startedAt: new Date().toISOString(),
    connId: input.connId,
    skipOnboard: input.skipOnboard,
    schema: input.schema,
    table: input.table,
    once: input.action === "daemon" ? true : undefined,
    command,
    output: "",
  };

  jobs.set(job.id, job);
  trimJobs();

  const child = spawn(runner.executable, args, {
    cwd: runner.cwd ?? repoRoot(),
    env: { ...process.env, DATASYNC_CONFIG: configPath() },
    detached: false,
  });

  let output = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    output = (output + chunk.toString()).slice(-12000);
    job.output = output;
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    output = (output + chunk.toString()).slice(-12000);
    job.output = output;
  });

  child.on("close", (code) => {
    job.exitCode = code ?? 1;
    job.status = code === 0 ? "completed" : "failed";
    job.finishedAt = new Date().toISOString();
    job.output = output;
  });

  child.on("error", (err) => {
    job.status = "failed";
    job.finishedAt = new Date().toISOString();
    job.output = `${output}\n${err.message}`.trim();
  });

  return { job };
}

export function runnerAvailable(): boolean {
  return resolveRunner() !== null && fs.existsSync(configPath());
}
