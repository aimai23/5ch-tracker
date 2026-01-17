import type { Env } from "./types";
import { getMeta, getRanking } from "./storage";
import { scheduled as scheduledImpl } from "./cron";

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (request.method === "GET" && pathname === "/health") {
      return new Response("ok", { status: 200 });
    }

    if (request.method === "GET" && pathname === "/api/meta") {
      const meta = await getMeta(env);
      return Response.json(meta ?? { lastRunAt: null, lastStatus: null, lastError: null });
    }

    if (request.method === "GET" && pathname === "/api/ranking") {
      const window = url.searchParams.get("window") ?? "24h";
      const ranking = await getRanking(env, window);
      return Response.json(
        ranking ?? { updatedAt: null, window, items: [], sources: [] }
      );
    }

    // ルートは説明にしてもOK（Not foundが嫌ならここを変える）
    if (request.method === "GET" && pathname === "/") {
      return new Response(
        "5ch-tracker\n\nEndpoints:\n/health\n/api/meta\n/api/ranking?window=24h\n",
        { status: 200, headers: { "Content-Type": "text/plain; charset=utf-8" } }
      );
    }

    return new Response("Not found", { status: 404 });
  },

  scheduled(event: any, env: Env, ctx: any) {
    return scheduledImpl(event, env, ctx);
  }
};
