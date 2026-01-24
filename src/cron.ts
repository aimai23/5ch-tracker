import type { Env, ScheduledEvent, ExecutionContext } from "./types";

/**
 * NOTE:
 * 5ch blocks Cloudflare Workers egress with 403 in many cases.
 * We keep the Cron trigger enabled, but this handler is a no-op
 * so it won't overwrite meta/ranking created by GitHub Actions.
 */
export async function scheduled(_event: ScheduledEvent, _env: Env, _ctx: ExecutionContext): Promise<void> {
  // no-op
}
