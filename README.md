# 5ch-tracker (Hybrid Architecture)

This project tracks stock trends on 5ch (Japanese textboard), analyzes sentiment using Gemini AI, and aggregates data via Cloudflare Workers. It also integrates Polymarket, Reddit (WallStreetBets), and Macro indicators for a comprehensive market overview.

## Architecture

1.  **Local Fetcher (Python)**:
    *   Runs locally (or on a server).
    *   Scrapes 5ch stock threads.
    *   Fetches external data (Polymarket, ApeWisdom/Reddit, FRED/Sahm Rule, Doughcon).
    *   Uses **Google Gemini API** to extract tickers, analyze sentiment, and generate "Breaking News" & "Comparative Insights".
    *   Sends aggregated JSON payload to the Cloudflare Worker.

2.  **Cloudflare Worker (TypeScript)**:
    *   Receives data via `/internal/ingest`.
    *   Stores data in **Cloudflare KV**.
    *   Serves public APIs for the frontend.

## API Endpoints (Cloudflare Worker)

Base URL: `https://<your-worker>.workers.dev`

### Public
-   `GET /health`: Health check (returns "OK").
-   `GET /api/ranking?window=24h`: Get the latest market analysis, including rankings, summary, and breaking news.
-   `GET /api/ongi-history`: Get historical Fear & Greed (Ongi) scores.
-   `GET /api/meta`: Get metadata about the last update status.

### Internal (Protected)
-   `POST /internal/ingest`: Ingest analyzed data from the Local Fetcher. Requires `Bearer <INGEST_TOKEN>`.
-   `GET /internal/update-prices`: Manually trigger price/ranking updates (if configured).

## Setup Guide

### 1. Cloudflare Worker
Prerequisites: `npm`, `wrangler`

1.  Clone repository.
2.  Install dependencies:
    ```bash
    npm install
    ```
3.  Configure `wrangler.toml` (if needed) or just run:
    ```bash
    npx wrangler deploy
    ```
4.  **Secrets**: Set the ingest token:
    ```bash
    npx wrangler secret put INGEST_TOKEN
    ```

### 2. Local Fetcher (Python)
Prerequisites: Python 3.10+

1.  Navigate to `local_fetcher/`.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Create `.env` file in `local_fetcher/`:
    ```ini
    GEMINI_API_KEY=your_gemini_api_key
    WORKER_URL=https://your-worker.workers.dev
    INGEST_TOKEN=your_ingest_token
    ```
4.  Run the fetcher:
    ```bash
    python main.py
    ```
    Options:
    -   `--debug`: Run analysis but do not upload to Worker.
    -   `--monitor`: Run in a loop (every 120s).
    -   `--poly-only`: Debug Polymarket fetching only.

## Configuration (`config/`)

-   **`exclude.json`**: List of tickers and words to exclude from analysis (e.g., specific spam or common non-stock terms).
-   **`nickname_dictionary.json`**: Mapping of 5ch slang to Tickers (e.g., `"林檎": "AAPL"`).
-   **`polymarket.json`**: Queries for fetching Polymarket events.
-   **`polymarket_exclude.json`**: Keywords to exclude from Polymarket results.

## Features
-   **Ticker Extraction**: Maps Japanese slang to US Tickers.
-   **Sentiment Analysis**: Fear & Greed scoring (0-100), Radar Chart (Hype, Panic, Faith, etc.).
-   **Breaking News**: AI-generated "Sports Commentary" style headlines.
-   **Comparative Insight**: AI comparison between 5ch (Japan) and Reddit (US) trends.
-   **Macro Indicators**: Sahm Rule, Yield Curve, CNN Fear & Greed.
