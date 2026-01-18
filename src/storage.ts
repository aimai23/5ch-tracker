import type { Env } from "./types";

export type RankingItem = { ticker: string; count: number };
export type RankingPayload = {
  updatedAt: string | null;
  window: string;
  items: RankingItem[];
  sources: Array<{ name: string; url: string }>;
};

export type MetaPayload = {
  lastRunAt: string | null;
  lastStatus: "success" | "error" | "noop" | null;
  lastError: string | null;
};

export function rankingKey(window: string): string {
  // allow: 24h, 1h
  const w = (window || "24h").toLowerCase();
  return `ranking:${w}`;
}

export async function getRanking(env: Env, window: string): Promise<RankingPayload | null> {
  const raw = await env.KV.get(rankingKey(window));
  if (!raw) return null;
  try {
    return JSON.parse(raw) as RankingPayload;
  } catch {
    return null;
  }
}

export async function putRanking(env: Env, window: string, payload: RankingPayload): Promise<void> {
  await env.KV.put(rankingKey(window), JSON.stringify(payload));
}

export async function getMeta(env: Env): Promise<MetaPayload | null> {
  const raw = await env.KV.get("meta");
  if (!raw) return null;
  try {
    return JSON.parse(raw) as MetaPayload;
  } catch {
    return null;
  }
}

export async function putMeta(env: Env, payload: MetaPayload): Promise<void> {
  await env.KV.put("meta", JSON.stringify(payload));
}
