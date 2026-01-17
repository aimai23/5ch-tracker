/**
 * Extract ticker symbols from a string and return a frequency map.
 *
 * A ticker is defined as 1–5 uppercase letters optionally prefixed with a '$'.
 * We ensure the ticker is bounded by non‑uppercase letters on both sides to avoid
 * matching parts of longer words. Excluded terms are ignored.
 */
export function extractTickers(text: string, exclude: string[]): Map<string, number> {
  const counts = new Map<string, number>();
  // Build a Set for faster exclusion checks
  const excludeSet = new Set(exclude);
  // Regular expression to match tickers. The lookbehind/lookahead ensure we don't
  // match substrings inside longer uppercase words.
  const regex = /(?:^|[^A-Z\$])\$?([A-Z]{1,5})(?=[^A-Z]|$)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(text)) !== null) {
    const ticker = match[1];
    if (excludeSet.has(ticker)) continue;
    // Skip obvious English words of 1–2 letters that are common stop words
    if (ticker.length === 1 || ticker.length === 2) {
      if (excludeSet.has(ticker)) continue;
    }
    const current = counts.get(ticker) ?? 0;
    counts.set(ticker, current + 1);
  }
  return counts;
}