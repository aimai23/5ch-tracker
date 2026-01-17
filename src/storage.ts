import type { Env } from "./types";

/**
 * Retrieve a ranking object for a given window from KV. Returns null if not found.
 */
export async function getRanking(env: Env, window: string): Promise<any | null> {
  const key = `ranking:${window}`;
  try {
    return await env.KV.get(key, { type: "json" });
  } catch {
    return null;
  }
}

/**
 * Persist a ranking object for a given window to KV.
 */
export async function putRanking(env: Env, window: string, data: unknown): Promise<void> {
  const key = `ranking:${window}`;
  await env.KV.put(key, JSON.stringify(data));
}

/**
 * Retrieve metadata about the last cron run.
 */
export async function getMeta(env: Env): Promise<any | null> {
  try {
    return await env.KV.get("meta", { type: "json" });
  } catch {
    return null;
  }
}

/**
 * Persist metadata about the cron run.
 */
export async function putMeta(env: Env, meta: unknown): Promise<void> {
  await env.KV.put("meta", JSON.stringify(meta));
}