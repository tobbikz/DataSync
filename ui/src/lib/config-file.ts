import fs from "fs";
import path from "path";
import { CDC_TUNING_KEYS, type CdcTuning } from "./cdc-tuning-types";

export type { CdcTuning } from "./cdc-tuning-types";

export function configFilePath(): string {
  return (
    process.env.DATASYNC_CONFIG ??
    path.resolve(process.cwd(), "..", "config.json")
  );
}

export function readConfigFile(): Record<string, unknown> {
  const raw = fs.readFileSync(configFilePath(), "utf-8");
  return JSON.parse(raw) as Record<string, unknown>;
}

export function readCdcTuning(): CdcTuning {
  const cfg = readConfigFile();
  const cdc = (cfg.cdc ?? {}) as CdcTuning;
  return { ...cdc };
}

export function writeCdcTuning(next: CdcTuning): CdcTuning {
  const filePath = configFilePath();
  const cfg = readConfigFile();
  const merged: CdcTuning = {
    ...((cfg.cdc as CdcTuning) ?? {}),
  };
  for (const key of CDC_TUNING_KEYS) {
    if (key in next) {
      (merged as Record<string, unknown>)[key] = next[key];
    }
  }
  cfg.cdc = merged;
  fs.writeFileSync(filePath, `${JSON.stringify(cfg, null, 2)}\n`, "utf-8");
  return merged;
}
