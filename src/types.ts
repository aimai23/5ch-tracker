export type Env = {
  DB: D1Database;
  /**
   * Secret token for /internal/ingest
   * Set this as a Worker secret: INGEST_TOKEN
   */
  INGEST_TOKEN?: string;
};
