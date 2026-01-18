import re
import os
import json
import requests
import time
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load params
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
WORKER_URL = os.getenv("WORKER_URL") # e.g. https://5ch-tracker.foo.workers.dev
INGEST_TOKEN = os.getenv("INGEST_TOKEN")

if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY is not set.")
    exit(1)

def load_config():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # Keeping sources.json reader for back-compat or other threads if needed, 
    # but main logic will use auto-discovery.
    exclude = []
    exclude_path = os.path.join(base_dir, "config", "exclude.json")
    if os.path.exists(exclude_path):
        with open(exclude_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            exclude = data.get("tickers", []) + data.get("words", []) + data.get("exclude", [])
    
    return [], exclude # Return empty threads as we will discover them dynamically

def discover_threads():
    print("Discovering latest threads from 5ch...")
    subject_url = "https://egg.5ch.net/stock/subject.txt"
    try:
        resp = requests.get(subject_url, timeout=10)
        resp.encoding = "CP932"
        text = resp.text
    except Exception as e:
        print(f"Failed to fetch subject.txt: {e}")
        return []

    # Format: 1735832043.dat<>【まとめ】米国株やってる人の溜まり場8730【禁止】 (123)
    # Regex to find title and extract number for sorting
    # Updated to be more flexible for duplicates or suffixes (e.g. 8730★2)
    pattern = re.compile(r"(\d+)\.dat<>(.*【まとめ】米国株やってる人の溜まり場\s*(\d+).*)\s+\(\d+\)")
    
    candidates = []
    for line in text.splitlines():
        m = pattern.search(line)
        if m:
            dat_id = m.group(1)
            title = m.group(2)
            thread_num = int(m.group(3))
            url = f"https://egg.5ch.net/test/read.cgi/stock/{dat_id}/"
            candidates.append({"name": title, "url": url, "num": thread_num})
    
    # Sort by thread number descending (newest first)
    candidates.sort(key=lambda x: x["num"], reverse=True)
    
    # Take top 4
    top_threads = candidates[:4]
    for t in top_threads:
        print(f"Found: {t['name']} (No.{t['num']})")
        
    return top_threads

def cleanup_cache():
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dat_cache")
    if not os.path.exists(cache_dir):
        return
        
    files = [os.path.join(cache_dir, f) for f in os.listdir(cache_dir) if f.endswith(".dat")]
    # Keep max 20 files, sort by mtime desc
    if len(files) > 20:
        files.sort(key=os.path.getmtime, reverse=True)
        for f in files[20:]:
            try:
                os.remove(f)
                print(f"Cleaned up old cache: {os.path.basename(f)}")
            except:
                pass

def parse_dat_content(text_data):
    comments = []
    for line in text_data.splitlines():
        parts = line.split("<>")
        # Format: Name<>Email<>Date ID<>Message<>Title
        if len(parts) >= 4:
            msg = parts[3]
            # Remove HTML tags (mostly <br>)
            clean_msg = re.sub(r"<[^>]+>", "\n", msg)
            comments.append(clean_msg.strip())
    
    if not comments:
        return ""
        
    full_text = "\n".join(comments)
    if len(full_text) > 30000:
        return full_text[:30000]
    return full_text

def fetch_thread_text(url):
    # Setup Cache
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dat_cache")
    os.makedirs(cache_dir, exist_ok=True)

    # Convert to dat URL
    dat_url = None
    thread_id = None
    m = re.match(r"https?://([^/]+)/test/read\.cgi/([^/]+)/(\d+)/?", url)
    if m:
        host, board, tid = m.groups()
        thread_id = tid
        dat_url = f"https://{host}/{board}/dat/{thread_id}.dat"
    
    # 1. Check Cache
    if thread_id:
        cache_path = os.path.join(cache_dir, f"{thread_id}.dat")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Check if thread is "finished" (approx 1000 lines)
                    if content.count('\n') >= 995: 
                        print(f"Using cached (finished): {thread_id}")
                        return parse_dat_content(content)
            except Exception as e:
                print(f"Cache read error: {e}")

    # 2. Fetch from Network
    target_url = dat_url if dat_url else url
    print(f"Fetching {target_url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        resp = requests.get(target_url, headers=headers, timeout=10)
        
        # Handling .dat (whether from conversion or direct)
        if dat_url and resp.status_code == 200:
            resp.encoding = "CP932"
            text_data = resp.text
            
            # Save to cache if we have a thread_id
            if thread_id:
                cache_path = os.path.join(cache_dir, f"{thread_id}.dat")
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(text_data)
                
                cleanup_cache()

            processed = parse_dat_content(text_data)
            if not processed:
                 print("Warning: Parsed .dat but found no comments.")
            return processed
            
        # Fallback to HTML handling if dat fails or not a dat url
        resp.encoding = "CP932"
        soup = BeautifulSoup(resp.text, "html.parser")
        
        comments = []
        
        # Strategy 1: Standard Modern 5ch (div.message)
        msgs = soup.find_all("div", class_="message")
        
        if not msgs:
            msgs = soup.find_all("dd", class_="thread_in")

        if not msgs:
            msgs = soup.select("div.post > div.message")
            
        for msg in msgs:
            text = msg.get_text(strip=True)
            if text:
                comments.append(text)

        if not comments:
             print(f"Warning: No comments parsed from {url}.")
             print("DEBUG: First 500 chars of HTML content:")
             print(soup.prettify()[:500])
             return ""
             
        full_text = "\n".join(comments)
        if len(full_text) > 30000:
            return full_text[:30000]
        return full_text

    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return ""

def analyze_with_gemini(text, exclude_list):
    print("Analyzing with Gemini (via REST API)...")
    
    # Simplified prompt for brevity in logs, strict JSON is key.
    prompt_text = f"""
    You are a financial analyst. Analyze the following text.
    Task: Identify US stock tickers and count them. 
    Ignore Japanese stock codes.
    Convert nicknames (e.g. テスラ -> TSLA).
    Exclude: {json.dumps(exclude_list)}
    
    Output strictly JSON:
    [
      {{"ticker": "NVDA", "count": 15}}
    ]
    
    Text:
    {text[:500000]}
    """

    # Try models in order (Updated based on user's available models)
    models = ["gemini-2.5-flash", "gemini-flash-latest"]
    
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{
                "parts": [{"text": prompt_text}]
            }]
        }
        
        try:
            # print(f"Trying model: {model_name}")
            resp = requests.post(url, headers=headers, json=payload, timeout=300)
            
            if resp.status_code == 200:
                result = resp.json()
                try:
                    content = result["candidates"][0]["content"]["parts"][0]["text"]
                    content = content.replace("```json", "").replace("```", "").strip()
                    return json.loads(content)
                except Exception:
                    pass
            elif resp.status_code == 404:
                continue # Try next model
            else:
                print(f"Model {model_name} error: {resp.status_code}")
                
        except Exception as e:
            print(f"Request failed for {model_name}: {e}")
            
    print("All Gemini models failed.")
    return []

def send_to_worker(items, sources):
    print(f"Sending {len(items)} tickers to Worker...")
    if not WORKER_URL or not INGEST_TOKEN:
        print("Worker config missing (WORKER_URL or INGEST_TOKEN). Skipping upload.")
        return

    payload = {
        "window": "24h",
        "items": items,
        "sources": sources
    }
    
    # Ensure no double slashes
    base_url = WORKER_URL.rstrip("/")
    url = f"{base_url}/internal/ingest"
    
    print(f"DEBUG: Posting to {url}")
    
    headers = {
        "Authorization": f"Bearer {INGEST_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print("Success!")
        else:
            print(f"Worker Error: {resp.status_code}")
            # print(resp.text) # Uncomment for detailed html debug if needed
    except Exception as e:
        print(f"Upload failed: {e}")

def main():
    # Load exclude config only
    _, exclude = load_config()
    
    # Auto-discover threads
    threads = discover_threads()
    
    if not threads:
        print("No threads found. Exiting.")
        return

    all_text = ""
    source_meta = []
    
    for t in threads:
        text = fetch_thread_text(t["url"])
        if not text:
            print(f"Skipping {t['name']} (empty)")
            continue
            
        all_text += f"\n--- Thread: {t['name']} ---\n{text}"
        source_meta.append({"name": t["name"], "url": t["url"]})
        time.sleep(1) # Be polite
    
    if not all_text.strip():
        print("No content fetched.")
        return

    tickers = analyze_with_gemini(all_text, exclude)
    
    agg = {}
    for t in tickers:
        sym = t.get("ticker", "").upper()
        cnt = t.get("count", 0)
        if sym:
            agg[sym] = agg.get(sym, 0) + cnt
    
    final_items = [{"ticker": k, "count": v} for k, v in agg.items()]
    final_items.sort(key=lambda x: x["count"], reverse=True)
    
    print("--- Top 10 ---")
    for i in final_items[:10]:
        print(f"{i['ticker']}: {i['count']}")
        
    send_to_worker(final_items, source_meta)

if __name__ == "__main__":
    main()
