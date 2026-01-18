import { Env } from "./types";
import { getRanking, putPricesBatch, PriceItem } from "./storage";

const YAHOO_QUOTE_API = "https://query1.finance.yahoo.com/v7/finance/quote";
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

    // 2. Fetch prices (Batch)
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
    if (tickerList.length === 0) return [];

    try {
        const symbols = tickerList.join(",");
        const url = `${YAHOO_QUOTE_API}?symbols=${symbols}`;
        console.log(`Fetching batch quotes: ${url}`);

        const res = await fetch(url);
        if (!res.ok) {
            console.error(`Yahoo Batch Error: ${res.status} ${res.statusText}`);
            return [];
        }

        const data: any = await res.json();
        const quotes = data?.quoteResponse?.result;

        if (!quotes || !Array.isArray(quotes)) {
            console.warn("No quoteResponse.result found in Yahoo API.");
            return [];
        }

        return quotes.map((q: any) => {
            const price = q.regularMarketPrice;
            const changeP = q.regularMarketChangePercent;
            const symbol = q.symbol;

            if (typeof price === 'number' && typeof changeP === 'number') {
                return {
                    ticker: symbol,
                    price: parseFloat(price.toFixed(2)),
                    change_percent: parseFloat(changeP.toFixed(2)),
                    updated_at: now
                } as PriceItem;
            }
            return null;
        }).filter((i): i is PriceItem => i !== null);

    } catch (e) {
        console.error("Batch fetch failed", e);
        return [];
    }
}
