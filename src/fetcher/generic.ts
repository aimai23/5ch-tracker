/**
 * Fetch the HTML content of a 5ch thread. Returns the text body of the response.
 *
 * We don't attempt to parse the HTML here; the caller can run ticker extraction on the
 * raw string. In the future you can swap this for a more sophisticated parser or
 * incorporate monazilla DAT API access.
 */
export async function fetchThread(url: string): Promise<string> {
  const response = await fetch(url, {
    headers: {
      // Set a reasonable User‑Agent to avoid being blocked
      'User-Agent': 'Mozilla/5.0 (compatible; 5chTickerBot/1.0)'
    }
  });
  if (!response.ok) {
    throw new Error(`Failed to fetch thread: ${response.status} ${response.statusText}`);
  }
  return await response.text();
}