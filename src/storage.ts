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
  breaking_news?: string[];
  polymarket?: Array<{ title: string; title_ja?: string; outcomes: string; url: string; volume: number }>;
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
    "SELECT ticker, count, sentiment FROM rankings WHERE term = ? ORDER BY count DESC LIMIT 20"
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
    breaking_news: meta.breaking_news || [],
    polymarket: meta.polymarket || [],
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
    breaking_news: payload.breaking_news,
    polymarket: payload.polymarket,
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

// --- Ongi History ---

export interface OngiHistoryItem {
  timestamp: number;
  score: number;
  label: string;
  metrics: RadarData;
}

export async function saveOngiHistory(env: Env, score: number, label: string, metrics: RadarData): Promise<void> {
  const now = Math.floor(Date.now() / 1000);

  // Cleanup old data (older than 30 days) to keep DB healthy
  // Fire and forget (await but don't block logic if possible, though await is safer for D1)
  await env.DB.prepare("DELETE FROM ongi_history WHERE timestamp < ?").bind(now - 30 * 86400).run();

  // 1. Check last entry
  const lastEntry = await env.DB.prepare("SELECT id, timestamp FROM ongi_history ORDER BY timestamp DESC LIMIT 1")
    .first<{ id: number; timestamp: number }>();

  // 2. Logic: If last entry is within 1 hour (3600s), OVERWRITE it.
  if (lastEntry && (now - lastEntry.timestamp) < 3600) {
    await env.DB.prepare(
      "UPDATE ongi_history SET timestamp = ?, score = ?, label = ?, metrics = ? WHERE id = ?"
    )
      .bind(now, score, label || "", JSON.stringify(metrics || {}), lastEntry.id)
      .run();
  } else {
    // Insert new
    await env.DB.prepare(
      "INSERT INTO ongi_history (timestamp, score, label, metrics) VALUES (?, ?, ?, ?)"
    )
      .bind(now, score, label || "", JSON.stringify(metrics || {}))
      .run();
  }
}

export async function getOngiHistory(env: Env): Promise<OngiHistoryItem[]> {
  // Get filtered history (Last 30 Days)
  const now = Math.floor(Date.now() / 1000);
  const cutoff = now - 30 * 86400; // 30 days

  const results = await env.DB.prepare(
    "SELECT timestamp, score, label, metrics FROM ongi_history WHERE timestamp > ? ORDER BY timestamp ASC"
  ).bind(cutoff).all<{ timestamp: number; score: number; label: string; metrics: string }>();

  if (!results.results) return [];

  return results.results.map(r => {
    let parsedMetrics: RadarData = { hype: 0, panic: 0, faith: 0, gamble: 0, iq: 0 };
    try {
      parsedMetrics = JSON.parse(r.metrics);
    } catch { }
    return {
      timestamp: r.timestamp,
      score: r.score,
      label: r.label,
      metrics: parsedMetrics
    };
  });
}
