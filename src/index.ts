import type { Env } from "./types";
import { getMeta, getRanking, putMeta, putRanking, type RankingPayload } from "./storage";
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

function corsHeaders(origin: string | null): Record<string, string> {
  // If you later serve a Pages frontend, you can tighten this.
  return {
    "access-control-allow-origin": origin ?? "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization"
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;
    const origin = request.headers.get("origin");

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    if (request.method === "GET" && pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    if (request.method === "GET" && pathname === "/api/meta") {
      const meta = await getMeta(env);
      return json(meta ?? { lastRunAt: null, lastStatus: null, lastError: null }, 200, corsHeaders(origin));
    }

    if (request.method === "GET" && pathname === "/api/ranking") {
      const window = url.searchParams.get("window") ?? "24h";
      const ranking = await getRanking(env, window);
      return json(
        ranking ?? { updatedAt: null, window, items: [], topics: [], sources: [] },
        200,
        corsHeaders(origin)
      );
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
