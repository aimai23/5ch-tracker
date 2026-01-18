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

function extractTickers(text) {
  // Match $NVDA or NVDA, 1-5 uppercase letters, word-boundary aware.
  // Uses a non-letter boundary on the left, and non-letter boundary on the right.
  const re = /(^|[^A-Z])\$?([A-Z]{1,5})(?=[^A-Z]|$)/g;
  const m = new Map();
  let r;
  while ((r = re.exec(text)) !== null) {
    const t = r[2];
    if (EX.has(t)) continue;
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
