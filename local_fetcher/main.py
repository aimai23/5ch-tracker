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
    with open(os.path.join(base_dir, "config", "sources.json"), "r", encoding="utf-8") as f:
        sources = json.load(f)
    
    exclude = []
    exclude_path = os.path.join(base_dir, "config", "exclude.json")
    if os.path.exists(exclude_path):
        with open(exclude_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            exclude = data.get("tickers", []) + data.get("words", []) + data.get("exclude", [])
    
    return sources["threads"], exclude

def fetch_thread_text(url):
    print(f"Fetching {url}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "CP932" # 5ch usually uses Shift_JIS/CP932
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Extract comments specific to 5ch standard layout
        comments = []
        # Support multiple standard 5ch viewer layouts
        # Pattern A: <div class="post"> ... <div class="message"> ... </div> </div>
        # Pattern B: <div class="thread"> ... <div class="res"> ... <div class="message"> ... </div> </div> </div>
        
        # Try generic message class search first (most effective)
        msgs = soup.find_all("div", class_="message")
        
        # If standard class not found, try data-id attribute (common in some views)
        if not msgs:
             msgs = soup.find_all("dd", class_="thread_in") # Some older views
             
        for msg in msgs:
            # strip=True removes extra newlines and spaces
            text = msg.get_text(strip=True)
            if text:
                comments.append(text)
        
        if not comments:
             print(f"Warning: No comments parsed from {url}. Skipping to save tokens.")
             return ""
             
        # Limit total characters to avoid exceeding token limits (approx 30k chars ~ 7-8k tokens)
        full_text = "\n".join(comments)
        if len(full_text) > 30000:
            print(f"Truncating text (Length: {len(full_text)} > 30000)")
            return full_text[:30000]
            
        return full_text
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return ""

def analyze_with_gemini(text, exclude_list):
    print("Analyzing with Gemini (via REST API)...")
    
    prompt_text = f"You are a financial analyst. Analyze the following Japanese text... (Task: Identify US tickers, Count occurrences, Exclude: {json.dumps(exclude_list)}) Output JSON."
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
            resp = requests.post(url, headers=headers, json=payload, timeout=180)
            
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
    threads, exclude = load_config()
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
