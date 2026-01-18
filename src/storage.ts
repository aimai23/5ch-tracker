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

// Helper to get meta key for ranking aux data
function rankingMetaKey(window: string): string {
  return `ranking_meta_${window}`;
}

export async function getRanking(env: Env, window: string): Promise<RankingPayload | null> {
  // 1. Get items
  const { results } = await env.DB.prepare(
    "SELECT ticker, count FROM rankings WHERE window = ? ORDER BY count DESC"
  )
    .bind(window)
    .all<{ ticker: string; count: number }>();

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
    sources: meta.sources || [],
  };
}

export async function putRanking(env: Env, window: string, payload: RankingPayload): Promise<void> {
  const statements: D1PreparedStatement[] = [];

  // 1. Delete old ranking for window
  statements.push(env.DB.prepare("DELETE FROM rankings WHERE window = ?").bind(window));

  // 2. Insert new items
  for (const item of payload.items) {
    statements.push(
      env.DB.prepare("INSERT INTO rankings (window, ticker, count) VALUES (?, ?, ?)")
        .bind(window, item.ticker, item.count)
    );
  }

  // 3. Save meta
  const meta: Partial<RankingPayload> = {
    updatedAt: payload.updatedAt,
    sources: payload.sources,
  };
  statements.push(
    env.DB.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)")
      .bind(rankingMetaKey(window), JSON.stringify(meta))
  );

  await env.DB.batch(statements);
}

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
