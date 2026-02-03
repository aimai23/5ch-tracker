# 5ch-tracker（Hybrid Architecture）

5chの株スレをローカルで収集し、Geminiでティッカー抽出・センチメント解析を行った結果を Cloudflare Worker + D1 に保存し、フロントエンドで可視化するプロジェクトです。

## 全体構成

1. **Local Fetcher（Python / `local_fetcher/`）**
   - 5chのスレッドを取得し、本文を解析します。
   - Gemini API で以下を生成します:
     - ティッカー抽出
     - Fear & Greed スコア
     - レーダー指標（Hype / Panic / Faith / Gamble / IQ）
     - 市況サマリー、Ongiコメント、Breaking News
     - 日米比較コメント、AIトレード提案
   - 外部データも取得します:
     - Polymarket（gamma API）
     - Reddit（ApeWisdom / WallStreetBets）
     - CNN Fear & Greed
     - Crypto Fear & Greed（Alternative.me）
     - Sahm Rule / Yield Curve（FRED）
     - Doughcon（pizzint.watch）
   - 集約したJSONを Worker の `/internal/ingest` へ送信します。
   - `last_run.json` を使ってランキングの差分（`rank_delta`）と新規フラグを計算します。

2. **Cloudflare Worker（TypeScript / `src/`）**
   - 受信したデータを **D1** に保存します。
   - 公開APIを提供します（ランキング、履歴、メタ情報など）。
   - `cron` は現状 **no-op**（価格取得機能は削除済み）です。

3. **フロントエンド（静的 / `site/`）**
   - `site/index.html` + `site/app.js` でAPIを描画します。
   - Chart.js / TradingView Widget / wordcloud2.js を使用。
   - `site/app.js` 内の `WORKER_URL` を自分の Worker に合わせて変更してください。

## ディレクトリ概要

- `local_fetcher/` : 5ch収集 + Gemini解析 + Worker送信
- `src/` : Cloudflare Worker API
- `migrations/` : D1スキーマ
- `config/` : 抽出ルール・除外ルール・Polymarket設定
- `site/` : フロントエンド静的ファイル

## セットアップ

### 1) Cloudflare Worker / D1

前提: `node`, `npm`, `wrangler`

```bash
npm install
```

#### D1作成 & 紐付け

```bash
npx wrangler d1 create <your-db-name>
```

生成された `database_name` / `database_id` を `wrangler.toml` に反映します。

#### マイグレーション適用

```bash
npx wrangler d1 migrations apply <your-db-name> --remote
```

#### シークレット設定

```bash
npx wrangler secret put INGEST_TOKEN
```

#### デプロイ

```bash
npx wrangler deploy
```

### 2) Local Fetcher（Python）

前提: Python 3.10+

```bash
cd local_fetcher
pip install -r requirements.txt
```

`.env` を作成:

```ini
GEMINI_API_KEY=your_gemini_api_key
WORKER_URL=https://<your-worker>.workers.dev
INGEST_TOKEN=your_ingest_token
FRED_API_KEY=your_fred_api_key
```

実行:

```bash
python main.py
```

オプション:
- `--debug` : 解析のみ（Worker送信なし）
- `--monitor` : 120秒ごとにループ実行
- `--poly-only` : Polymarket取得のみ

> 補足: `Janome` が入っているとトピック抽出精度が上がります（未導入なら自動で正規表現にフォールバック）

### 3) フロントエンド

`site/app.js` の `WORKER_URL` を自分の Worker に書き換えたうえで、
任意の静的ホスティング（Cloudflare Pages など）で `site/` を配信してください。

## API（Worker）

**Public**
- `GET /health` : ヘルスチェック
- `GET /api/ranking?window=24h` : ランキング + 解析結果一式
- `GET /api/ranking-history?window=24h&limit=5` : 過去スナップショット（最大5件）
- `GET /api/ongi-history` : Ongi履歴（30日）
- `GET /api/meta` : 最終更新情報

**Internal**
- `POST /internal/ingest` : 集計データの受け取り（`Authorization: Bearer <INGEST_TOKEN>` 必須）

## 設定ファイル（`config/`）

- `exclude.json` : 除外ティッカー・ストップワード・スパム文字
- `nickname_dictionary.json` : 5chスラング → ティッカー対応表
- `polymarket.json` : Polymarket検索クエリ
- `polymarket_exclude.json` : Polymarket除外キーワード

## 注意点

- 5chは Worker からの直接アクセスが403になることが多いため、**ローカル Fetcher 前提**です。
- `local_fetcher/last_run.json` は実行時に自動生成されます（`.gitignore` 済み）。
- 解析・外部APIは失敗時に空データで進むことがあります（ログ参照）。
