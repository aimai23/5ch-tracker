export type Env = {
  DB: D1Database;
  /**
   * Secret token for /internal/ingest
   * Set this as a Worker secret: INGEST_TOKEN
   */
  INGEST_TOKEN: string;
  GEMINI_API_KEY: string;
  /**
   * Comma-separated allowlist for frontend origins.
   * Example: https://example.com,https://app.example.com
   */
  FRONTEND_ORIGINS?: string;
  /**
   * Optional per-IP limit for public GET APIs within one window.
   */
  PUBLIC_API_RATE_LIMIT?: string;
  /**
   * Optional window length (seconds) for PUBLIC_API_RATE_LIMIT.
   */
  PUBLIC_API_RATE_WINDOW_SEC?: string;
};

export interface D1Result<T = unknown> {
  results: T[];
  success: boolean;
  meta: any;
  error?: string;
}

export interface D1PreparedStatement {
  bind(...values: any[]): D1PreparedStatement;
  first<T = unknown>(colName?: string): Promise<T | null>;
  run<T = unknown>(): Promise<D1Result<T>>;
  all<T = unknown>(): Promise<D1Result<T>>;
  raw<T = unknown>(): Promise<T[]>;
}

export interface D1Database {
  prepare(query: string): D1PreparedStatement;
  dump(): Promise<ArrayBuffer>;
  batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]>;
  exec(query: string): Promise<D1Result>;
}

export interface ExecutionContext {
  waitUntil(promise: Promise<any>): void;
  passThroughOnException(): void;
}

export interface ScheduledEvent {
  cron: string;
  type: string;
  scheduledTime: number;
}

// Global Worker Types Stubs (if not picked up by tsconfig)
export interface Request {
  url: string;
  method: string;
  headers: Headers;
  json(): Promise<any>;
  text(): Promise<string>;
}

export interface Response {
  status: number;
  headers: Headers;
  json(): Promise<any>;
  text(): Promise<string>;
}
