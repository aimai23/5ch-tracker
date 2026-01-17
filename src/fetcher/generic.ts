type Attempt = { url: string; status?: number; statusText?: string; error?: string };

function parseThreadUrl(threadUrl: string): { host: string; server: string; board: string; key: string } {
  // ex: https://egg.5ch.net/test/read.cgi/stock/1735832043/
  const u = new URL(threadUrl);
  const host = u.host; // egg.5ch.net
  const server = host.split(".")[0]; // egg
  const m = u.pathname.match(/^\/test\/read\.cgi\/([^/]+)\/(\d+)\//);
  if (!m) throw new Error(`Unsupported thread url: ${threadUrl}`);
  const board = m[1];
  const key = m[2];
  return { host, server, board, key };
}

function buildHeaders(referer: string) {
  return {
    // "bot" っぽいUAは弾かれやすいので、普通のブラウザUAに寄せる
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    // 5chはRefererを求める規制があるので付ける :contentReference[oaicite:1]{index=1}
    "Referer": referer
  };
}

async function tryFetch(url: string, headers: Record<string, string>, attempts: Attempt[]): Promise<string | null> {
  try {
    const res = await fetch(url, { headers, redirect: "follow" });
    if (!res.ok) {
      attempts.push({ url, status: res.status, statusText: res.statusText });
      return null;
    }
    return await res.text();
  } catch (e) {
    attempts.push({ url, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) });
    return null;
  }
}

/**
 * Fetch thread text with fallbacks:
 * 1) original read.cgi URL (HTML)
 * 2) DAT URL (/board/dat/key.dat)
 * 3) itest viewer URL (mobile)
 *
 * 本文はどこにも保存せず、呼び出し元でメモリ上で集計するだけ。
 */
export async function fetchThread(threadUrl: string): Promise<string> {
  const { host, server, board, key } = parseThreadUrl(threadUrl);

  const boardTop = `https://${host}/${board}/`;
  const datUrl = `https://${host}/${board}/dat/${key}.dat`; // dat形式 :contentReference[oaicite:2]{index=2}
  const itestUrl = `https://itest.5ch.net/${server}/test/read.cgi/${board}/${key}/`; // itestは5chビューア :contentReference[oaicite:3]{index=3}

  const attempts: Attempt[] = [];

  // 1) read.cgi
  {
    const text = await tryFetch(threadUrl, buildHeaders(boardTop), attempts);
    if (text) return text;
  }

  // 2) dat
  {
    const text = await tryFetch(datUrl, buildHeaders(threadUrl), attempts);
    if (text) return text;
  }

  // 3) itest
  {
    const text = await tryFetch(itestUrl, buildHeaders("https://itest.5ch.net/"), attempts);
    if (text) return text;
  }

  // どれもダメなら、本文は残さず「どこで弾かれたか」だけ返す
  const summary = attempts
    .map(a => `${a.url} => ${a.error ?? `${a.status} ${a.statusText}`}`)
    .join(" | ");
  throw new Error(`Failed to fetch thread (all attempts): ${summary}`);
}
