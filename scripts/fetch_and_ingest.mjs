import fs from "node:fs/promises";

const workerBase = process.env.WORKER_BASE_URL;
const token = process.env.WORKER_INGEST_TOKEN;
if (!workerBase || !token) {
  console.error("Missing WORKER_BASE_URL or WORKER_INGEST_TOKEN in GitHub Secrets");
  process.exit(1);
}

const ingestUrl = new URL("/internal/ingest", workerBase).toString();

const sources = JSON.parse(await fs.readFile("config/sources.json", "utf8"));
const excludeJson = JSON.parse(await fs.readFile("config/exclude.json", "utf8"));
const EX = new Set((excludeJson.exclude ?? []).map((s) => String(s).toUpperCase()));

// --- Ticker extraction ("B" = accuracy-first) ---
// - Counts "$NVDA" and "NVDA" as the same ticker "NVDA".
// - Ignores 1-letter tokens unless they are explicitly prefixed with '$' (e.g. $C).
// - For 2-letter tickers (e.g. "GM"), requires "stock-ish" context nearby to reduce false positives.
// - Strips HTML before extraction to avoid counting tag/attribute noise.

function decodeHtmlEntities(s) {
  // Minimal, dependency-free decoder (enough for 5ch HTML)
  const named = {
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&nbsp;": " ",
  };
  let out = s.replace(/&(amp|lt|gt|quot|nbsp);|&#39;/g, (m) => named[m] ?? m);
  out = out.replace(/&#(\d+);/g, (_m, n) => {
    const code = Number(n);
    if (!Number.isFinite(code) || code < 0 || code > 0x10ffff) return " ";
    try {
      return String.fromCodePoint(code);
    } catch {
      return " ";
    }
  });
  return out;
}

function htmlToText(html) {
  // Remove scripts/styles, turn some breaks into newlines, then strip tags.
  const noScript = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ");
  const withBreaks = noScript
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|tr|h\d)>/gi, "\n");
  const noTags = withBreaks.replace(/<[^>]+>/g, " ");
  return decodeHtmlEntities(noTags).replace(/[\t\f\r]+/g, " ");
}

function hasStockContext(text, start, len) {
  const L = 24;
  const a = Math.max(0, start - L);
  const b = Math.min(text.length, start + len + L);
  const around = text.slice(a, b);
  // Japanese + common trading terms + "$" nearby
  return /(\$|株|買|売|利確|損切|決算|PTS|チャート|上げ|下げ|ショート|ロング|ETF|NASDAQ|NYSE)/.test(around);
}

function extractTickers(text) {
  const plain = htmlToText(text);
  // Lookaround-based boundaries: avoid matching inside words like "FOOBAR" or "NVDA123".
  // Node 20 supports lookbehind.
  const re = /(?<![A-Z0-9$])(\$?[A-Z]{1,5})(?![A-Z0-9])/g;
  const m = new Map();
  for (const match of plain.matchAll(re)) {
    const raw = match[1];
    const hasDollar = raw.startsWith("$");
    const t = (hasDollar ? raw.slice(1) : raw).toUpperCase();

    if (!t) continue;
    if (EX.has(t)) continue;

    // Accuracy-first filters
    if (!hasDollar && t.length === 1) continue;
    if (!hasDollar && t.length === 2) {
      const idx = match.index ?? 0;
      if (!hasStockContext(plain, idx, raw.length)) continue;
    }

    m.set(t, (m.get(t) ?? 0) + 1);
  }
  return m;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
      "Cache-Control": "no-cache",
      "Pragma": "no-cache"
    },
    redirect: "follow"
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`fetch failed: ${res.status} ${res.statusText} ${t.slice(0, 200)}`);
  }
  return await res.text();
}

function merge(dst, src) {
  for (const [k, v] of src.entries()) {
    dst.set(k, (dst.get(k) ?? 0) + v);
  }
}

async function main() {
  const counts = new Map();
  const usedSources = [];

  for (const th of sources.threads ?? []) {
    const html = await fetchText(th.url); // not persisted
    const m = extractTickers(html);
    merge(counts, m);
    usedSources.push({ name: th.name, url: th.url });
  }

  const items = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 200)
    .map(([ticker, count]) => ({ ticker, count }));

  const payload = {
    window: "24h",
    updatedAt: new Date().toISOString(),
    items,
    sources: usedSources
  };

  const res = await fetch(ingestUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`
    },
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`ingest failed: ${res.status} ${t.slice(0, 300)}`);
  }

  console.log("ok");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
