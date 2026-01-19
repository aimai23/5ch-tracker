import type { Env } from "./types";

export interface RankingItem {
  ticker: string;
  count: number;
  sentiment?: number;
}

// PriceItem Removed

export interface RadarData {
  hype: number;
  panic: number;
  faith: number;
  gamble: number;
  iq: number;
}

export type TopicItem = { word: string; count: number };
export type RankingPayload = {
  updatedAt: string | null;
  window: string;
  items: RankingItem[];
  topics: TopicItem[];
  overview?: string;
  ongi_comment?: string; // New separate comment for Ongi tab
  fear_greed?: number;
  radar?: RadarData;
  sources: Array<{ name: string; url: string }>;
};

export type MetaPayload = {
  lastRunAt: string | null;
  lastStatus: "success" | "error" | "noop" | null;
  lastError: string | null;
};

// Helper to get meta key for ranking aux data
function rankingMetaKey(window: string): string {
  return `ranking_meta_${window}`;
}

export async function getRanking(env: Env, window: string): Promise<RankingPayload | null> {
  // 1. Get items
  const { results } = await env.DB.prepare(
    "SELECT ticker, count, sentiment FROM rankings WHERE term = ? ORDER BY count DESC"
  )
    .bind(window)
    .all<{ ticker: string; count: number; sentiment: number }>();

  // 2. Get meta (sources, updatedAt)
  const metaRaw = await env.DB.prepare("SELECT value FROM meta WHERE key = ?")
    .bind(rankingMetaKey(window))
    .first<string>("value");

  if (!results && !metaRaw) return null;

  let meta: Partial<RankingPayload> = {};
  if (metaRaw) {
    try {
      meta = JSON.parse(metaRaw);
    } catch { }
  }

  return {
    window,
    updatedAt: meta.updatedAt ?? null,
    items: results || [],
    topics: meta.topics || [],
    overview: meta.overview || null,
    ongi_comment: meta.ongi_comment || null,
    fear_greed: meta.fear_greed,
    radar: meta.radar,
    sources: meta.sources || [],
  };
}

export async function putRanking(env: Env, window: string, payload: RankingPayload): Promise<void> {
  const statements: D1PreparedStatement[] = [];

  // 1. Delete old ranking for window
  // Use 'term' column
  statements.push(env.DB.prepare("DELETE FROM rankings WHERE term = ?").bind(window));

  // 2. Insert new items
  for (const item of payload.items) {
    statements.push(
      env.DB.prepare("INSERT INTO rankings (term, ticker, count, sentiment) VALUES (?, ?, ?, ?)")
        .bind(window, item.ticker, item.count, item.sentiment ?? 0)
    );
  }

  // 3. Save meta
  const meta: Partial<RankingPayload> = {
    updatedAt: payload.updatedAt,
    sources: payload.sources,
    topics: payload.topics,
    overview: payload.overview,
    ongi_comment: payload.ongi_comment,
    fear_greed: payload.fear_greed,
    radar: payload.radar,
  };
  statements.push(
    env.DB.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)")
      .bind(rankingMetaKey(window), JSON.stringify(meta))
  );

  await env.DB.batch(statements);
}

// --- Price Functions ---

// Prices Logic Removed

export async function getMeta(env: Env): Promise<MetaPayload | null> {
  const val = await env.DB.prepare("SELECT value FROM meta WHERE key = 'global_meta'")
    .first<string>("value");

  if (!val) return null;
  try {
    return JSON.parse(val) as MetaPayload;
  } catch {
    return null;
  }
}

export async function putMeta(env: Env, payload: MetaPayload): Promise<void> {
  await env.DB.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES ('global_meta', ?)")
    .bind(JSON.stringify(payload))
    .run();
}
