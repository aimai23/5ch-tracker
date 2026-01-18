import type { Env } from "./types";
import { getMeta, getRanking, putMeta, putRanking, getAllPrices, type RankingPayload } from "./storage";
import { scheduled as scheduledImpl } from "./cron";
import { handleScheduled } from "./scheduler";

function json(data: unknown, status = 200, extraHeaders?: Record<string, string>): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(extraHeaders ?? {})
    }
  });
}

// The original corsHeaders function is no longer used directly in the new logic,
// but keeping it for other potential uses or if the new logic is not fully applied everywhere.
function corsHeaders(origin: string | null): Record<string, string> {
  // If you later serve a Pages frontend, you can tighten this.
  return {
    "access-control-allow-origin": origin ?? "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization"
  };
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const origin = request.headers.get("origin");

    // New CORS headers constant for direct use in the new /api/ranking logic
    const commonCorsHeaders = {
      "Access-Control-Allow-Origin": "*", // Or origin ?? "*" if you want to respect the request origin
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: commonCorsHeaders });
    }

    if (request.method === "GET" && pathname === "/health") {
      return new Response("OK", { status: 200, headers: commonCorsHeaders });
    }

    // Force Update Endpoint (Manual Trigger)
    if (request.method === "GET" && pathname === "/internal/update-prices") {
      const auth = request.headers.get("Authorization");
      const queryKey = url.searchParams.get("key");

      if (auth !== `Bearer ${env.INGEST_TOKEN}` && queryKey !== env.INGEST_TOKEN) {
        return new Response("Unauthorized", { status: 401 });
      }

      try {
        // Pass a mock event
        await handleScheduled({ cron: "manual", type: "scheduled", scheduledTime: Date.now() }, env, ctx);
        return new Response("Manual Update Triggered. Check Logs.", { status: 200, headers: commonCorsHeaders });
      } catch (e) {
        return new Response(`Error: ${e}`, { status: 500 });
      }
    }

    if (request.method === "GET" && pathname === "/api/meta") {
      const meta = await getMeta(env);
      return json(meta ?? { lastRunAt: null, lastStatus: null, lastError: null }, 200, commonCorsHeaders);
    }

    if (url.pathname.startsWith("/api/ranking")) {
      const window = url.searchParams.get("window") || "24h";
      const ranking = await getRanking(env, window);

      // Always return valid JSON structure even if empty
      const safeRanking = ranking || {
        updatedAt: null,
        window: "24h",
        items: [],
        topics: [],
        overview: null, // Added overview to match original structure
        sources: []
      };

      // Fetch Prices
      const priceMap = await getAllPrices(env);

      // Merge for convenience
      const responseData = {
        ...safeRanking,
        prices: priceMap
      };

      return new Response(JSON.stringify(responseData), {
        headers: { ...commonCorsHeaders, "Content-Type": "application/json" }
      });
    }

    // Internal endpoint for GitHub Actions (or other fetchers)
    if (request.method === "POST" && pathname === "/internal/ingest") {
      const token = env.INGEST_TOKEN;
      if (!token) {
        return json({ ok: false, error: "INGEST_TOKEN is not set" }, 500, corsHeaders(origin));
      }

      const auth = request.headers.get("authorization") ?? "";
      if (auth !== `Bearer ${token}`) {
        return json({ ok: false, error: "unauthorized" }, 401, corsHeaders(origin));
      }

      let body: RankingPayload;
      try {
        body = (await request.json()) as RankingPayload;
      } catch {
        return json({ ok: false, error: "invalid json" }, 400, corsHeaders(origin));
      }

      const window = (body.window ?? "24h").toLowerCase();
      const payload: RankingPayload = {
        updatedAt: body.updatedAt ?? new Date().toISOString(),
        window,
        items: Array.isArray(body.items) ? body.items : [],
        topics: Array.isArray(body.topics) ? body.topics : [],
        overview: body.overview,
        sources: Array.isArray(body.sources) ? body.sources : []
      };

      // Only aggregated data is stored.
      await putRanking(env, window, payload);

      await putMeta(env, {
        lastRunAt: new Date().toISOString(),
        lastStatus: "success",
        lastError: null
      });

      return json({ ok: true }, 200, corsHeaders(origin));
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
        corsHeaders(origin)
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
