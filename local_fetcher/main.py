import re
import os
import sys
import json
import requests
import time
import logging
import datetime
import glob
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Base Directory Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
CACHE_DIR = os.path.join(BASE_DIR, "dat_cache")

# Setup Logging
os.makedirs(LOG_DIR, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
log_file = os.path.join(LOG_DIR, f"{current_date}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Load params
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
WORKER_URL = os.getenv("WORKER_URL") # e.g. https://5ch-tracker.foo.workers.dev
INGEST_TOKEN = os.getenv("INGEST_TOKEN")

if not GEMINI_API_KEY:
    logging.error("GEMINI_API_KEY is not set.")
    exit(1)

def cleanup_old_files():
    # Cleanup Cache (Keep top 20)
    if os.path.exists(CACHE_DIR):
        files = [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith(".dat")]
        if len(files) > 20:
            files.sort(key=os.path.getmtime, reverse=True)
            for f in files[20:]:
                try:
                    os.remove(f)
                    logging.info(f"Cleaned up old cache: {os.path.basename(f)}")
                except Exception as e:
                    logging.warning(f"Failed to remove cache {f}: {e}")

    # Cleanup Logs (Keep 30 days)
    now = time.time()
    retention_days = 30
    cutoff = now - (retention_days * 86400)
    
    log_files = glob.glob(os.path.join(LOG_DIR, "*.log"))
    for f in log_files:
        if os.path.getmtime(f) < cutoff:
            try:
                os.remove(f)
                logging.info(f"Cleaned up old log: {os.path.basename(f)}")
            except Exception as e:
                logging.warning(f"Failed to remove log {f}: {e}")

def load_config():
    # Keeping sources.json reader for back-compat or other threads if needed, 
    # but main logic will use auto-discovery.
    exclude = []
    exclude_path = os.path.join(os.path.dirname(BASE_DIR), "config", "exclude.json")
    if os.path.exists(exclude_path):
        with open(exclude_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            exclude = data.get("tickers", []) + data.get("words", []) + data.get("exclude", [])
    
    return [], exclude # Return empty threads as we will discover them dynamically

def discover_threads():
    logging.info("Discovering latest threads from 5ch...")
    subject_url = "https://egg.5ch.net/stock/subject.txt"
    try:
        resp = requests.get(subject_url, timeout=10)
        resp.encoding = "CP932"
        text = resp.text
    except Exception as e:
        logging.error(f"Failed to fetch subject.txt: {e}")
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
        logging.info(f"Found: {t['name']} (No.{t['num']})")
        
    return top_threads

def parse_dat_content(text_data):
    import html
    comments = []
    # Skip the first post (1レス目) as requested
    for line in text_data.splitlines()[1:]:
        parts = line.split("<>")
        # Format: Name<>Email<>Date ID<>Message<>Title
        if len(parts) >= 4:
            msg = parts[3]
            # Remove URLs first
            msg = re.sub(r"https?://[\w/:%#\$&\?\(\)~\.=\+\-]+", "", msg)
            # Remove HTML tags (mostly <br> becomes space)
            msg = re.sub(r"<[^>]+>", " ", msg)
            # Unescape entities (e.g. &gt; -> >)
            clean_msg = html.unescape(msg)
            comments.append(clean_msg.strip())
    
    if not comments:
        return ""
        
    full_text = "\n".join(comments)
    if len(full_text) > 30000:
        logging.info(f"Truncating text (Length: {len(full_text)} > 30000)")
        return full_text[:30000]
    return full_text

def fetch_thread_text(url):
    # Setup Cache
    os.makedirs(CACHE_DIR, exist_ok=True)

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
        cache_path = os.path.join(CACHE_DIR, f"{thread_id}.dat")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Check if thread is "finished" (approx 1000 lines)
                    if content.count('\n') >= 995: 
                        logging.info(f"Using cached (finished): {thread_id}")
                        return parse_dat_content(content)
            except Exception as e:
                logging.warning(f"Cache read error: {e}")

    # 2. Fetch from Network
    target_url = dat_url if dat_url else url
    logging.info(f"Fetching {target_url}...")
    
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
                cache_path = os.path.join(CACHE_DIR, f"{thread_id}.dat")
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(text_data)
                logging.info(f"Saved to cache: {thread_id}")

            processed = parse_dat_content(text_data)
            if not processed:
                 logging.warning("Parsed .dat but found no comments.")
            return processed
            
        # Fallback to HTML handling if dat fails or not a dat url
        logging.info("Fallback to HTML parsing...")
        resp.encoding = "CP932"
        soup = BeautifulSoup(resp.text, "html.parser")
        
        comments = []
        
        # Strategy 1: Standard Modern 5ch (div.message)
        msgs = soup.find_all("div", class_="message")
        
        if not msgs:
            msgs = soup.find_all("dd", class_="thread_in")

        if not msgs:
            msgs = soup.select("div.post > div.message")
            
        # Skip 1st post in Fallback checks
        for msg in msgs[1:]:
            text = msg.get_text(strip=True)
            if text:
                comments.append(text)

        if not comments:
             logging.warning(f"No comments parsed from {url}.")
             logging.debug(f"HTML Head: {soup.prettify()[:500]}")
             return ""
             
        full_text = "\n".join(comments)
        if len(full_text) > 30000:
            return full_text[:30000]
        return full_text

    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return ""

def analyze_with_gemini(text, exclude_list):
    logging.info("Analyzing with Gemini (via REST API)...")
    
    # Simplified prompt for brevity in logs, strict JSON is key.
    prompt_text = f"""
    You are a financial analyst. Analyze the following text.
    Task: Identify US stock tickers and company names, then count them as their standardized ticker symbol.
    Map company names (English or Japanese) to the official US ticker (e.g. "Apple", "アップル" -> AAPL).
    Ignore Japanese stock codes.
    Convert nicknames (e.g. テスラ -> TSLA).
    VERIFY that each output ticker is a valid, currently trading US stock symbol. Do not output invalid or hallucinated tickers.
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
            # logging.info(f"Trying model: {model_name}")
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
                logging.error(f"Model {model_name} error: {resp.status_code}")
                
        except Exception as e:
            logging.error(f"Request failed for {model_name}: {e}")
            
    logging.error("All Gemini models failed.")
    return []

def analyze_topics(text):
    logging.info("Analyzing topics (Keyword Extraction)...")
    
    # Common Japanese stop words / noise
    stop_words = {
        'こと', 'もの', 'さん', 'これ', 'それ', 'あれ', 'どれ', 
        'よう', 'そう', 'はず', 'まま', 'ため', 'だけ', 'ばっ',
        'どこ', 'そこ', 'あそこ', 'いま', 'いつ', 'なん',
        'の', 'し', 'て', 'いる', 'ある', 'する', 'なる', 
        'ない', 'いい', 'も', 'な', 'だ', 'ん', 'ー', 'www', 'w',
        'gt', 'amp', 'nbsp', 'http', 'https', 'com', 'co', 'jp',
        # User Feedback Additions
        'みたい', 'ども', 'やつ', 'わけ', 'ほう', 'スレ', 'レス', 'マジ', 
        'バカ', 'マン', 'おじ', '本当', '以上', '一つ', '我々', '自分', '何処',
        'ここ', 'たち', 'とも', '的', '化', '明日', '今日', '昨日', '今回', '前回'
    }
    
    words = []
    
    try:
        from janome.tokenizer import Tokenizer
        logging.info("Using Janome for morphological analysis.")
        t = Tokenizer()
        tokens = t.tokenize(text)
        for token in tokens:
            # part_of_speech looks like "名詞,一般,*,*,..."
            pos_parts = token.part_of_speech.split(',')
            main_pos = pos_parts[0]
            sub_pos = pos_parts[1] if len(pos_parts) > 1 else '*'
            
            # Select Nouns, but exclude Dependent/Pronoun/Suffix/Number
            if main_pos == '名詞' and sub_pos not in ['非自立', '代名詞', '数', '接尾']:
                word = token.surface
                
                # Filter Garbage
                # 1. Skip if digits only
                if word.isdigit(): continue
                # 2. Skip single char (except Kanji? No, skip all single usually safe for trends)
                if len(word) < 2: continue
                # 3. Skip known stop words
                if word in stop_words: continue
                # 4. Skip specific symbol noise common in 5ch (e.g. gt, ;&)
                if re.search(r'[;&=<>\(\)\{\}\[\]]', word): continue
                # 5. Skip URLs or path-like (should be cleaned in parse, but double check)
                if 'http' in word or '.com' in word: continue
                
                words.append(word)

    except ImportError:
        logging.warning("Janome not found. Falling back to simple Regex extraction.")
        logging.info("To improve accuracy, please install janome: python3 -m pip install janome")
        pattern = re.compile(r"([一-龠ァ-ヶa-zA-Z]{2,})") 
        matches = pattern.findall(text)
        for w in matches:
             if w not in stop_words and not w.isdigit() and len(w) > 1:
                 if 'http' in w or 'gt' == w: continue
                 words.append(w)

    # Count frequency
    from collections import Counter
    counter = Counter(words)
    
    # Format top 50
    top_words = [{"word": k, "count": v} for k, v in counter.most_common(50)]
    
    logging.info("--- Top Topics ---")
    for t in top_words[:5]:
        logging.info(f"{t['word']}: {t['count']}")
        
    return top_words

def send_to_worker(items, topics, sources):
    logging.info(f"Sending {len(items)} tickers and {len(topics)} topics to Worker...")
    if not WORKER_URL or not INGEST_TOKEN:
        logging.warning("Worker config missing (WORKER_URL or INGEST_TOKEN). Skipping upload.")
        return

    payload = {
        "window": "24h",
        "items": items,
        "topics": topics,
        "sources": sources
    }
    
    # Ensure no double slashes
    base_url = WORKER_URL.rstrip("/")
    url = f"{base_url}/internal/ingest"
    
    logging.info(f"DEBUG: Posting to {url}")
    
    headers = {
        "Authorization": f"Bearer {INGEST_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            logging.info("Success!")
        else:
            logging.error(f"Worker Error: {resp.status_code}")
            # logging.error(resp.text)
    except Exception as e:
        logging.error(f"Upload failed: {e}")

def main():
    # Cleanup old logs and cache
    cleanup_old_files()
    
    # Load exclude config only
    _, exclude = load_config()
    
    # Auto-discover threads
    threads = discover_threads()
    
    if not threads:
        logging.info("No threads found. Exiting.")
        return

    all_text = ""
    source_meta = []
    
    for t in threads:
        text = fetch_thread_text(t["url"])
        if not text:
            logging.info(f"Skipping {t['name']} (empty)")
            continue
            
        all_text += f"\n--- Thread: {t['name']} ---\n{text}"
        source_meta.append({"name": t["name"], "url": t["url"]})
        time.sleep(1) # Be polite
    
    if not all_text.strip():
        logging.info("No content fetched.")
        return

    # Analyze Topics (No AI)
    topics = analyze_topics(all_text)

    # DEBUG MODE CHECK
    if len(sys.argv) > 1 and sys.argv[1] == "debug":
        logging.info("!!! DEBUG MODE: Skipping AI and Upload !!!")
        logging.info("--- Top 50 Topics (Debug) ---")
        for t in topics:
            logging.info(f"{t['word']}: {t['count']}")
        return

    # Analyze Tickers (AI)
    tickers = analyze_with_gemini(all_text, exclude)
    
    agg = {}
    for t in tickers:
        sym = t.get("ticker", "").upper()
        cnt = t.get("count", 0)
        if sym:
            agg[sym] = agg.get(sym, 0) + cnt
    
    final_items = [{"ticker": k, "count": v} for k, v in agg.items()]
    final_items.sort(key=lambda x: x["count"], reverse=True)
    
    logging.info("--- Top 10 Tickers ---")
    for i in final_items[:10]:
        logging.info(f"{i['ticker']}: {i['count']}")
        
    send_to_worker(final_items, topics, source_meta)

if __name__ == "__main__":
    main()
