export type DbEngine = "mariadb" | "mssql" | "mongodb";

export interface ConnectionForm {
  alias: string;
  db_engine: DbEngine;
  host: string;
  port: number;
  db_name: string;
  username: string;
  password: string;
  replica_set: string;
  active: boolean;
}

export const ENGINE_OPTIONS: {
  id: DbEngine;
  label: string;
  defaultPort: number;
}[] = [
  {
    id: "mariadb",
    label: "MariaDB / MySQL",
    defaultPort: 3306,
  },
  {
    id: "mssql",
    label: "SQL Server",
    defaultPort: 1433,
  },
  {
    id: "mongodb",
    label: "MongoDB",
    defaultPort: 27017,
  },
];

export const CDC_REQUIREMENTS: Record<DbEngine, string[]> = {
  mariadb: [
    "log_bin = ON",
    "binlog_format = ROW",
    "SHOW MASTER STATUS must succeed (binlog readable)",
  ],
  mssql: [
    "Database CDC enabled (sys.databases.is_cdc_enabled = 1)",
    "Connect user can read sys views / cdc schema",
  ],
  mongodb: [
    "Replica set deployed (change streams)",
    "replica_set name in connection extras",
  ],
};

export const WIZARD_STEPS = [
  { id: "type", label: "Connector type" },
  { id: "properties", label: "Properties" },
  { id: "additional", label: "Additional properties" },
  { id: "review", label: "Review" },
] as const;

export type WizardStepId = (typeof WIZARD_STEPS)[number]["id"];

export function defaultForm(engine: DbEngine = "mariadb"): ConnectionForm {
  const opt = ENGINE_OPTIONS.find((e) => e.id === engine)!;
  return {
    alias: "",
    db_engine: engine,
    host: "127.0.0.1",
    port: opt.defaultPort,
    db_name: "",
    username: "",
    password: "",
    replica_set: "",
    active: true,
  };
}

export function validateForm(form: ConnectionForm, step: WizardStepId): string | null {
  if (step === "type" && !form.db_engine) {
    return "Select a connector type";
  }
  if (step === "properties" || step === "review") {
    if (!form.alias.trim()) return "Connection alias is required";
    if (!/^[A-Za-z0-9_]+$/.test(form.alias.trim())) {
      return "Alias: letters, numbers, underscore only";
    }
    if (!form.host.trim()) return "Host is required";
    if (!form.db_name.trim()) return "Database name is required";
    if (form.port <= 0 || form.port > 65535) return "Invalid port";
  }
  if (step === "additional" || step === "review") {
    if (form.db_engine === "mongodb" && !form.replica_set.trim()) {
      return "MongoDB replica set name is required for change streams";
    }
  }
  return null;
}
