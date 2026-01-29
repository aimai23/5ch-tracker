import type { Env } from "./types";

export interface RankingItem {
  ticker: string;
  count: number;
  sentiment?: number;
  rank_delta?: number;
  is_new?: boolean;
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

export interface TradeRecommendation {
  ticker: string;
  reason: string;
}

export interface TradeRecommendations {
  bullish: TradeRecommendation[];
  bearish: TradeRecommendation[];
}

export type RankingPayload = {
  updatedAt: string | null;
  window: string;
  items: RankingItem[];
  topics: TopicItem[];
  overview?: string;
  summary?: string;
  ongi_comment?: string;
  comparative_insight?: string;
  fear_greed?: number;
  trade_recommendations?: TradeRecommendations;
  ai_model?: string;
  radar?: RadarData;

  breaking_news?: string[];
  polymarket?: Array<{ title: string; title_ja?: string; outcomes: string; url: string; volume: number }>;
  reddit_rankings?: Array<{
    rank: number;
    ticker: string;
    name: string;
    count: number;
    upvotes: number;
    rank_24h_ago?: number;
    mentions_24h_ago?: number;
  }>;
  cnn_fear_greed?: { score: number; rating: string; timestamp?: string };
  crypto_fear_greed?: { value: number; classification: string };
  doughcon?: { level: number; description: string };
  sahm_rule?: { value: number; state: string };
  yield_curve?: { value: number; state: string };
  sources: Array<{ name: string; url: string }>;
};

export type MetaPayload = {
  lastRunAt: string | null;
  lastStatus: "success" | "error" | "noop" | null;
  lastError: string | null;
};

export interface RankingHistorySnapshot {
  id: number;
  window: string;
  timestamp: number;
  payload: RankingPayload;
}

// Helper to get meta key for ranking aux data
function rankingMetaKey(window: string): string {
  return `ranking_meta_${window}`;
}

export async function getRanking(env: Env, window: string): Promise<RankingPayload | null> {
  // 1. Get items
  const { results } = await env.DB.prepare(
    "SELECT ticker, count, sentiment, rank_delta, is_new FROM rankings WHERE term = ? ORDER BY count DESC LIMIT 20"
  )
    .bind(window)
    .all<{ ticker: string; count: number; sentiment: number; rank_delta: number; is_new: number }>();

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

  // Convert DB 0/1 to boolean for is_new
  // Explicitly type 'r' as any to avoid implicit-any error if inference fails
  const items = (results || []).map((r: any) => ({
    ...r,
    is_new: !!r.is_new
  }));

  return {
    window,
    updatedAt: meta.updatedAt ?? null,
    items: items,
    topics: meta.topics || [],
    overview: meta.overview || undefined,
    summary: meta.summary || undefined,
    ongi_comment: meta.ongi_comment || undefined,
    comparative_insight: meta.comparative_insight || undefined,
    trade_recommendations: meta.trade_recommendations || undefined,
    ai_model: meta.ai_model || undefined,
    fear_greed: meta.fear_greed,
    radar: meta.radar,
    breaking_news: meta.breaking_news || [],
    polymarket: meta.polymarket || [],
    reddit_rankings: meta.reddit_rankings || [],
    cnn_fear_greed: meta.cnn_fear_greed,
    crypto_fear_greed: meta.crypto_fear_greed,
    doughcon: meta.doughcon,
    sahm_rule: meta.sahm_rule,
    yield_curve: meta.yield_curve,
    sources: meta.sources || [],
  };
}

export async function putRanking(env: Env, window: string, payload: RankingPayload): Promise<void> {
  const statements: any[] = []; // Relax type to avoid namespace issues

  // 1. Delete old ranking for window
  // Use 'term' column
  statements.push(env.DB.prepare("DELETE FROM rankings WHERE term = ?").bind(window));

  // 2. Insert new items
  for (const item of payload.items) {
    statements.push(
      env.DB.prepare("INSERT INTO rankings (term, ticker, count, sentiment, rank_delta, is_new) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(window, item.ticker, item.count, item.sentiment ?? 0, item.rank_delta ?? 0, item.is_new ? 1 : 0)
    );
  }

  // 3. Save meta
  const meta: Partial<RankingPayload> = {
    updatedAt: payload.updatedAt,
    sources: payload.sources,
    topics: payload.topics,
    overview: payload.overview,
    summary: payload.summary,
    ongi_comment: payload.ongi_comment,
    comparative_insight: payload.comparative_insight,
    trade_recommendations: payload.trade_recommendations,
    ai_model: payload.ai_model,
    fear_greed: payload.fear_greed,
    radar: payload.radar,
    breaking_news: payload.breaking_news,
    polymarket: payload.polymarket,
    reddit_rankings: payload.reddit_rankings,
    cnn_fear_greed: payload.cnn_fear_greed,
    crypto_fear_greed: payload.crypto_fear_greed,
    doughcon: payload.doughcon,
    sahm_rule: payload.sahm_rule,
    yield_curve: payload.yield_curve,
  };
  statements.push(
    env.DB.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)")
      .bind(rankingMetaKey(window), JSON.stringify(meta))
  );

  await env.DB.batch(statements);
}

const HISTORY_LIMIT_DEFAULT = 5;

export async function saveRankingHistory(
  env: Env,
  window: string,
  payload: RankingPayload,
  limit = HISTORY_LIMIT_DEFAULT
): Promise<void> {
  const now = Math.floor(Date.now() / 1000);

  await env.DB.prepare(
    "INSERT INTO ranking_history (window, timestamp, payload) VALUES (?, ?, ?)"
  )
    .bind(window, now, JSON.stringify(payload))
    .run();

  // Keep only the latest N snapshots per window
  const keep = Math.max(1, Math.min(10, limit));
  const { results } = await env.DB.prepare(
    "SELECT id FROM ranking_history WHERE window = ? ORDER BY timestamp DESC LIMIT ?"
  )
    .bind(window, keep)
    .all<{ id: number }>();

  if (!results || results.length === 0) return;

  const ids = results.map(r => r.id);
  const placeholders = ids.map(() => "?").join(",");
  await env.DB.prepare(
    `DELETE FROM ranking_history WHERE window = ? AND id NOT IN (${placeholders})`
  )
    .bind(window, ...ids)
    .run();
}

export async function getRankingHistory(
  env: Env,
  window: string,
  limit = HISTORY_LIMIT_DEFAULT
): Promise<RankingHistorySnapshot[]> {
  const take = Math.max(1, Math.min(10, limit));
  const { results } = await env.DB.prepare(
    "SELECT id, window, timestamp, payload FROM ranking_history WHERE window = ? ORDER BY timestamp DESC LIMIT ?"
  )
    .bind(window, take)
    .all<{ id: number; window: string; timestamp: number; payload: string }>();

  if (!results) return [];

  return results.map((r) => {
    let payload: RankingPayload = {
      updatedAt: null,
      window: r.window,
      items: [],
      topics: [],
      sources: []
    };
    try {
      payload = JSON.parse(r.payload) as RankingPayload;
    } catch { }

    return {
      id: r.id,
      window: r.window,
      timestamp: r.timestamp,
      payload
    };
  });
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

  return results.results.map((r: any) => {
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
