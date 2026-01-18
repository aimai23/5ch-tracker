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
        
        # Extract comments
        comments = []
        for div in soup.find_all("div", class_="post"):
            msg = div.find("div", class_="message")
            if msg:
                comments.append(msg.get_text())
        
        if not comments:
             return soup.get_text()
             
        return "\n".join(comments)
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
    models = ["gemini-2.0-flash", "gemini-2.5-flash", "gemini-flash-latest", "gemini-pro-latest"]
    
    for model_name in models:
        url = f"{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{
                "parts": [{"text": prompt_text}]
            }]
        }
        
        try:
            # print(f"Trying model: {model_name}")
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            
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
    
    url = f"{WORKER_URL}/internal/ingest"
    headers = {
        "Authorization": f"Bearer {INGEST_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print("Success!")
        else:
            print(f"Worker Error: {resp.status_code} {resp.text}")
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
