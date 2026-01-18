# 5ch-tracker (Workers + KV + GitHub Actions)

## Why GitHub Actions?
5ch frequently blocks Cloudflare Workers egress IPs with 403. This project fetches threads from GitHub Actions and sends **only aggregated ticker counts** to the Worker.

## Endpoints
- GET /health -> ok
- GET /api/ranking?window=24h
- GET /api/meta

## Worker ingest
- POST /internal/ingest (requires Bearer token)

## Cloudflare setup
1. Bind KV namespace to Worker binding name `KV`.
2. Add Worker secret `INGEST_TOKEN`.

## GitHub Actions setup
Add repository secrets:
- WORKER_BASE_URL: e.g. https://5ch-tracker.tentendao.workers.dev
- WORKER_INGEST_TOKEN: same as Cloudflare `INGEST_TOKEN`

## Config
- config/sources.json: list threads
- config/exclude.json: excluded tickers/words
