declare module "mssql" {
  export interface ConnectionPool {
    request(): {
      query(sql: string): Promise<{ recordset: unknown[] }>;
    };
    close(): Promise<void>;
  }

  export interface config {
    server?: string;
    port?: number;
    database?: string;
    user?: string;
    password?: string;
    options?: Record<string, unknown>;
    connectionTimeout?: number;
    requestTimeout?: number;
  }

  export function connect(config: config): Promise<ConnectionPool>;
}
