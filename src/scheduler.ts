import { Env } from "./types";
import { getRanking, putRanking, RankingItem } from "./storage";

const YAHOO_API_BASE = "https://query1.finance.yahoo.com/v8/finance/chart";

export async function handleScheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    console.log("--- Scheduled Price Update Started ---");

    // 1. Load current ranking
    const currentData = await getRanking(env, "24h");
    if (!currentData || currentData.items.length === 0) {
        console.log("No data to update.");
        return;
    }

    // 2. Fetch updated prices
    const updatedItems = await fetchStockPrices(currentData.items);

    // 3. Save back (Preserve other metadata)
    await putRanking(env, "24h", {
        ...currentData,
        updatedAt: new Date().toISOString(), // Update timestamp so frontend sees change
        items: updatedItems
    });

    console.log(`Updated prices for ${updatedItems.length} items.`);
}

async function fetchStockPrices(items: RankingItem[]): Promise<RankingItem[]> {
    // Parallel fetch
    const updated = await Promise.all(items.map(async (item) => {
        try {
            // Using query1 chart endpoint
            const res = await fetch(`${YAHOO_API_BASE}/${item.ticker}?interval=1d&range=2d`);
            if (res.ok) {
                const data: any = await res.json();
                const result = data?.chart?.result?.[0];
                const quote = result?.indicators?.quote?.[0];
                const closes = quote?.close;

                if (closes && closes.length > 0) {
                    // Get last valid
                    const validCloses = closes.filter((c: any) => c !== null);
                    if (validCloses.length >= 1) {
                        const current = validCloses[validCloses.length - 1];
                        // Calculate change vs previous close (chart.result[0].meta.chartPreviousClose)
                        const prev = result?.meta?.chartPreviousClose || validCloses[0];

                        const changeConfig = prev ? ((current - prev) / prev) * 100 : 0;

                        return {
                            ...item,
                            price: parseFloat(current.toFixed(2)),
                            change_percent: parseFloat(changeConfig.toFixed(2))
                        };
                    }
                }
            }
        } catch (e) {
            // console.warn(`Price fetch failed for ${item.ticker}`);
        }
        return item;
    }));

    return updated;
}
