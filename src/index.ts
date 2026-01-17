import type { Env } from "./types";
import { getMeta, getRanking } from "./storage";
// We import the scheduled handler from cron.ts and re‑export it so Wrangler will attach it
export { scheduled } from "./cron";

/**
 * Handle HTTP requests. Exposes health, ranking and meta endpoints.
 *
 * - GET /health returns "ok" for uptime checks.
 * - GET /api/ranking?window=24h returns the ranking JSON for the given window.
 * - GET /api/meta returns metadata about the last cron run.
 */
// Define the fetch handler. We avoid referencing types from @cloudflare/workers-types to
// keep the build simple. The handler receives a Request and Env and returns a Response.
const handler = async (request: Request, env: Env): Promise<Response> => {
  const url = new URL(request.url);
  const pathname = url.pathname;

  // Health check
  if (pathname === "/health") {
    return new Response("ok", { status: 200 });
  }

  // Metadata endpoint
  if (pathname === "/api/meta") {
    const meta = await getMeta(env);
    return new Response(JSON.stringify(meta ?? {}), {
      status: 200,
      headers: { "Content-Type": "application/json; charset=utf-8" }
    });
  }

  // Ranking endpoint
  if (pathname === "/api/ranking") {
    const windowParam = url.searchParams.get("window") ?? "24h";
    const data = await getRanking(env, windowParam);
    return new Response(JSON.stringify(data ?? { updatedAt: null, window: windowParam, items: [], sources: [] }), {
      status: 200,
      headers: { "Content-Type": "application/json; charset=utf-8" }
    });
  }

  return new Response("Not found", { status: 404 });
};

export default {
  fetch: handler,
};