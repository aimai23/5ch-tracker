import { Env } from "./types";

export async function handleScheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    console.log("--- Scheduled Event (No-op) ---");
    // Heatmap/Price update logic removed.
}
