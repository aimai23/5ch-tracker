import type { Env, ScheduledEvent, ExecutionContext } from "./types";
import {
  getMeta,
  getRanking,
  putMeta,
  putRanking,
  type RankingPayload,
  getOngiHistory,
  saveOngiHistory,
  getRankingHistory,
  saveRankingHistory
} from "./storage";
import { scheduled as scheduledImpl } from "./cron";

function json(data: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(extraHeaders ?? {})
    }
  });
}

const PUBLIC_API_PATHS = new Set<string>([
  "/api/meta",
  "/api/ongi-history",
  "/api/ranking-history",
  "/api/ranking"
]);

const PUBLIC_RATE_LIMIT_FALLBACK = 120;
const PUBLIC_RATE_WINDOW_SEC_FALLBACK = 60;
const PUBLIC_RATE_LIMIT_MAX = 2000;
const PUBLIC_RATE_WINDOW_SEC_MAX = 3600;
const RATE_LIMIT_SWEEP_THRESHOLD = 5000;

type RateLimitEntry = {
  count: number;
  resetAt: number;
};

const rateLimitStore = new Map<string, RateLimitEntry>();

function parsePositiveInt(raw: string | undefined, fallback: number, max: number): number {
  const parsed = Number.parseInt(raw ?? "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function normalizeOrigin(raw: string | null): string | null {
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    return parsed.origin;
  } catch {
    return null;
  }
}

function allowedOrigins(env: Env, workerOrigin: string): Set<string> {
  const set = new Set<string>([
    workerOrigin,
    "http://localhost:8787",
    "http://127.0.0.1:8787",
    "http://localhost:3000",
    "http://127.0.0.1:3000"
  ]);

  for (const entry of (env.FRONTEND_ORIGINS ?? "").split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const origin = normalizeOrigin(trimmed);
    if (origin) set.add(origin);
  }

  return set;
}

function requestOrigin(request: Request): string | null {
  const origin = normalizeOrigin(request.headers.get("origin"));
  if (origin) return origin;
  return normalizeOrigin(request.headers.get("referer"));
}

function corsHeaders(origin: string | null): Record<string, string> {
  return {
    ...(origin ? { "Access-Control-Allow-Origin": origin } : {}),
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Vary": "Origin"
  };
}

function checkPublicRateLimit(request: Request, pathname: string, env: Env): { ok: true } | { ok: false; retryAfterSec: number } {
  const limit = parsePositiveInt(env.PUBLIC_API_RATE_LIMIT, PUBLIC_RATE_LIMIT_FALLBACK, PUBLIC_RATE_LIMIT_MAX);
  const windowSec = parsePositiveInt(env.PUBLIC_API_RATE_WINDOW_SEC, PUBLIC_RATE_WINDOW_SEC_FALLBACK, PUBLIC_RATE_WINDOW_SEC_MAX);
  const now = Date.now();
  const resetAfterMs = windowSec * 1000;

  const clientIp =
    request.headers.get("cf-connecting-ip")?.trim() ||
    request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ||
    "unknown";

  const key = `${clientIp}:${pathname}`;
  const current = rateLimitStore.get(key);

  if (!current || current.resetAt <= now) {
    rateLimitStore.set(key, { count: 1, resetAt: now + resetAfterMs });
  } else if (current.count >= limit) {
    const retryAfterSec = Math.max(1, Math.ceil((current.resetAt - now) / 1000));
    return { ok: false, retryAfterSec };
  } else {
    current.count += 1;
    rateLimitStore.set(key, current);
  }

  if (rateLimitStore.size > RATE_LIMIT_SWEEP_THRESHOLD) {
    for (const [k, value] of rateLimitStore) {
      if (value.resetAt <= now) {
        rateLimitStore.delete(k);
      }
    }
  }

  return { ok: true };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const allowlist = allowedOrigins(env, url.origin);
    const reqOrigin = requestOrigin(request);
    const allowedOrigin = reqOrigin && allowlist.has(reqOrigin) ? reqOrigin : null;
    const publicCorsHeaders = corsHeaders(allowedOrigin);

    if (request.method === "OPTIONS") {
      if (PUBLIC_API_PATHS.has(pathname) && !allowedOrigin) {
        return json({ ok: false, error: "forbidden origin" }, 403);
      }
      return new Response(null, { status: 204, headers: publicCorsHeaders });
    }

    if (PUBLIC_API_PATHS.has(pathname)) {
      if (request.method !== "GET") {
        return json({ ok: false, error: "method not allowed" }, 405, publicCorsHeaders);
      }
      if (!allowedOrigin) {
        return json({ ok: false, error: "forbidden origin" }, 403);
      }
      const rateLimit = checkPublicRateLimit(request, pathname, env);
      if (!rateLimit.ok) {
        return json(
          { ok: false, error: "rate limit exceeded" },
          429,
          { ...publicCorsHeaders, "Retry-After": String(rateLimit.retryAfterSec) }
        );
      }
    }

    if (request.method === "GET" && pathname === "/health") {
      return new Response("OK", { status: 200, headers: publicCorsHeaders });
    }

    if (request.method === "GET" && pathname === "/api/meta") {
      const meta = await getMeta(env);
      return json(meta ?? { lastRunAt: null, lastStatus: null, lastError: null }, 200, publicCorsHeaders);
    }

    if (url.pathname === "/api/ongi-history") {
      const history = await getOngiHistory(env);
      return new Response(JSON.stringify(history), {
        headers: { ...publicCorsHeaders, "Content-Type": "application/json" }
      });
    }

    if (url.pathname === "/api/ranking-history") {
      const window = url.searchParams.get("window") || "24h";
      const limitRaw = Number.parseInt(url.searchParams.get("limit") || "10", 10);
      const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(10, limitRaw)) : 10;
      try {
        const history = await getRankingHistory(env, window, limit);
        return json({ window, history }, 200, publicCorsHeaders);
      } catch (e) {
        console.error("getRankingHistory failed", e);
        return json({ window, history: [] }, 200, publicCorsHeaders);
      }
    }

    if (url.pathname === "/api/ranking") {
      const window = url.searchParams.get("window") || "24h";
      const ranking = await getRanking(env, window);

      // Always return valid JSON structure even if empty
      const safeRanking = ranking || {
        updatedAt: null,
        window: "24h",
        items: [],
        topics: [],
        overview: null, // Added overview to match original structure
        sources: [],
        reddit_rankings: [],
        brief_swing: null,
        brief_long: null
      };

      const responseData = { ...safeRanking };

      return new Response(JSON.stringify(responseData), {
        headers: { ...publicCorsHeaders, "Content-Type": "application/json" }
      });
    }

    // Internal endpoint for GitHub Actions (or other fetchers)
    if (request.method === "POST" && pathname === "/internal/ingest") {
      const token = env.INGEST_TOKEN;
      if (!token) {
        return json({ ok: false, error: "INGEST_TOKEN is not set" }, 500, corsHeaders(allowedOrigin));
      }

      const auth = request.headers.get("authorization") ?? "";
      if (auth !== `Bearer ${token}`) {
        return json({ ok: false, error: "unauthorized" }, 401, corsHeaders(allowedOrigin));
      }

      let body: RankingPayload;
      try {
        body = (await request.json()) as RankingPayload;
      } catch {
        return json({ ok: false, error: "invalid json" }, 400, corsHeaders(allowedOrigin));
      }

      const window =
        typeof body.window === "string" && body.window.length > 0
          ? body.window.toLowerCase()
          : "24h";
      const payload: RankingPayload = {
        updatedAt: body.updatedAt ?? new Date().toISOString(),
        window,
        items: Array.isArray(body.items) ? body.items : [],
        topics: Array.isArray(body.topics) ? body.topics : [],
        overview: body.overview,
        summary: body.summary,
        ongi_comment: body.ongi_comment,
        comparative_insight: body.comparative_insight,
        ai_model: body.ai_model,
        brief_swing: body.brief_swing,
        brief_long: body.brief_long,
        fear_greed: body.fear_greed,
        radar: body.radar,
        breaking_news: body.breaking_news,
        polymarket: body.polymarket,
        reddit_rankings: body.reddit_rankings,
        cnn_fear_greed: body.cnn_fear_greed,
        crypto_fear_greed: body.crypto_fear_greed,
        doughcon: body.doughcon,
        sahm_rule: body.sahm_rule,
        yield_curve: body.yield_curve,
        hy_oas: body.hy_oas,
        market_breadth: body.market_breadth,
        volatility: body.volatility,
        sources: Array.isArray(body.sources) ? body.sources : []
      };

      const warnings: string[] = [];

      try {
        // Only aggregated data is stored.
        await putRanking(env, window, payload);
      } catch (e) {
        console.error("putRanking failed", e);
        try {
          await putMeta(env, {
            lastRunAt: new Date().toISOString(),
            lastStatus: "error",
            lastError: "putRanking failed"
          });
        } catch (metaErr) {
          console.error("putMeta failed after putRanking error", metaErr);
        }
        return json({ ok: false, error: "putRanking failed" }, 500, corsHeaders(allowedOrigin));
      }

      try {
        await saveRankingHistory(env, window, payload, 10);
      } catch (e) {
        console.warn("Failed to save ranking history", e);
        warnings.push("ranking_history");
      }

      if (typeof payload.fear_greed === "number") {
        try {
          await saveOngiHistory(
            env,
            payload.fear_greed,
            "",
            payload.radar || { hype: 0, panic: 0, faith: 0, gamble: 0, iq: 0 }
          );
        } catch (e) {
          console.warn("Failed to save ongi history", e);
          warnings.push("ongi_history");
        }
      }

      try {
        await putMeta(env, {
          lastRunAt: new Date().toISOString(),
          lastStatus: "success",
          lastError: warnings.length ? warnings.join(",") : null
        });
      } catch (e) {
        console.warn("Failed to update meta", e);
        warnings.push("meta");
      }

      return json({ ok: true, warnings }, 200, corsHeaders(allowedOrigin));
    }

    if (request.method === "GET" && pathname === "/") {
      return json(
        {
          service: "5ch-tracker",
          endpoints: [
            "/health",
            "/api/meta",
            "/api/ranking?window=24h"
          ]
        },
        200,
        corsHeaders(allowedOrigin)
      );
    }

    return new Response("Not found", { status: 404 });
  },

  scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    // Keep cron handler exported so triggers don't error.
    // It is a no-op by default.
    return scheduledImpl(event, env, ctx);
  }
};
