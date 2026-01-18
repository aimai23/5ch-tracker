import { Env } from "./types";
import { getRanking, putPricesBatch, PriceItem } from "./storage";

const YAHOO_API_BASE = "https://query1.finance.yahoo.com/v8/finance/chart";
// Hardcoded fallback or synced with watchlist.json manual entry
const WATCHLIST_TICKERS = ["BETA", "ONDS", "ASTS", "IONQ", "LAES", "WULF", "CRWV", "POET", "OSCR", "TEM"];

export async function handleScheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    console.log("--- Scheduled Price Update Started ---");

    const tickersToUpdate = new Set<string>(WATCHLIST_TICKERS);

    // 1. Load current ranking to get trending tickers
    try {
        const currentData = await getRanking(env, "24h");
        if (currentData && currentData.items) {
            currentData.items.forEach(i => tickersToUpdate.add(i.ticker));
        }
    } catch (e) {
        console.error("Failed to load rankings", e);
    }

    if (tickersToUpdate.size === 0) {
        console.log("No tickers to update.");
        return;
    }

    // 2. Fetch prices
    const priceItems = await fetchStockPrices(Array.from(tickersToUpdate));

    // 3. Save to Prices Table
    if (priceItems.length > 0) {
        await putPricesBatch(env, priceItems);
        console.log(`Updated prices for ${priceItems.length} tickers.`);
    } else {
        console.log("No prices fetched.");
    }
}

async function fetchStockPrices(tickerList: string[]): Promise<PriceItem[]> {
    const now = new Date().toISOString();

    // Parallel fetch
    const results = await Promise.all(tickerList.map(async (ticker) => {
        try {
            const res = await fetch(`${YAHOO_API_BASE}/${ticker}?interval=1d&range=2d`);
            if (res.ok) {
                const data: any = await res.json();
                const result = data?.chart?.result?.[0];
                const quote = result?.indicators?.quote?.[0];
                const closes = quote?.close;

                if (closes && closes.length > 0) {
                    const validCloses = closes.filter((c: any) => c !== null);
                    if (validCloses.length >= 1) {
                        const current = validCloses[validCloses.length - 1];
                        const prev = result?.meta?.chartPreviousClose || validCloses[0];
                        const changeConfig = prev ? ((current - prev) / prev) * 100 : 0;

                        return {
                            ticker: ticker,
                            price: parseFloat(current.toFixed(2)),
                            change_percent: parseFloat(changeConfig.toFixed(2)),
                            updated_at: now
                        } as PriceItem;
                    }
                }
            }
        } catch (e) {
            // console.warn ...
        }
        return null;
    }));

    return results.filter((i): i is PriceItem => i !== null);
}
