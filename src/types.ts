export type Env = {
  KV: KVNamespace;
  /**
   * Secret token for /internal/ingest
   * Set this as a Worker secret: INGEST_TOKEN
   */
  INGEST_TOKEN?: string;
};
