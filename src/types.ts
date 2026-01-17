/**
 * Environment bindings available to the Worker. At runtime the `KV` binding
 * should be configured to point at your Cloudflare KV namespace. See README
 * and wrangler.jsonc for setup details.
 */
export interface Env {
  KV: KVNamespace;
}