import type { Env } from './types';
import { extractTickers } from './ticker';
import { putRanking, putMeta } from './storage';
import { fetchThread } from './fetcher/generic';

// Import monitoring configuration. These JSON files are bundled at build time.
import sources from '../config/sources.json';
import exclude from '../config/exclude.json';

/**
 * Scheduled handler. Runs periodically based on the cron schedule configured in
 * Cloudflare. It fetches all monitored threads, extracts ticker symbols,
 * aggregates counts, stores the ranking in KV and updates metadata.
 */
export const scheduled: any = async (_event: any, env: Env, _ctx: any) => {
  const startedAt = new Date().toISOString();
  try {
    // Aggregate counts across all sources
    const globalCounts: Record<string, number> = {};
    for (const thread of sources.threads) {
      const html = await fetchThread(thread.url);
      const tickers = extractTickers(html, exclude.exclude);
      for (const [ticker, count] of tickers.entries()) {
        globalCounts[ticker] = (globalCounts[ticker] ?? 0) + count;
      }
    }
    // Convert counts into a sorted array
    const items = Object.entries(globalCounts)
      .map(([ticker, count]) => ({ ticker, count }))
      .sort((a, b) => b.count - a.count);
    // Prepare the ranking object
    const ranking = {
      updatedAt: startedAt,
      window: '24h',
      items,
      sources: sources.threads
    };
    // Store ranking and metadata
    await putRanking(env, '24h', ranking);
    await putMeta(env, { lastRunAt: startedAt, lastStatus: 'success' });
  } catch (err) {
    // On failure, record the error in metadata
    await putMeta(env, { lastRunAt: startedAt, lastStatus: 'error', lastError: (err as Error).message });
  }
};