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
WORKER_URL = os.getenv("WORKER_URL") 
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
    exclude = []
    spam = []
    stopwords = []
    exclude_path = os.path.join(os.path.dirname(BASE_DIR), "config", "exclude.json")
    if os.path.exists(exclude_path):
        with open(exclude_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            exclude = data.get("tickers", []) + data.get("words", []) + data.get("exclude", [])
            spam = data.get("spam", [])
            stopwords = data.get("stopwords", [])
    return stopwords, exclude, spam

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
    
    candidates.sort(key=lambda x: x["num"], reverse=True)
    top_threads = candidates[:4]
    for t in top_threads:
        logging.info(f"Found: {t['name']} (No.{t['num']})")
        
    return top_threads

def parse_dat_content(text_data, spam_list=[]):
    import html
    comments = []
    for line in text_data.splitlines()[1:]:
        parts = line.split("<>")
        if len(parts) >= 4:
            msg = parts[3]
            
            # Spam Filter
            is_spam = False
            for s in spam_list:
                if s in msg: 
                    is_spam = True
                    break
            if is_spam: continue

            msg = re.sub(r"https?://[\w/:%#\$&\?\(\)~\.=\+\-]+", "", msg)
            msg = re.sub(r"<[^>]+>", " ", msg)
            clean_msg = html.unescape(msg)
            comments.append(clean_msg.strip())
    
    if not comments: return ""
    full_text = "\n".join(comments)
    if len(full_text) > 15000: return full_text[:15000]
    return full_text

def fetch_thread_text(url, spam_list=[]):
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
    
    if thread_id:
        cache_path = os.path.join(CACHE_DIR, f"{thread_id}.dat")
        if os.path.exists(cache_path):
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if content.count('\n') >= 995: 
                        logging.info(f"Using cached (finished): {thread_id}")
                        return parse_dat_content(content, spam_list)
            except Exception as e:
                logging.warning(f"Cache read error: {e}")

    target_url = dat_url if dat_url else url
    logging.info(f"Fetching {target_url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        resp = requests.get(target_url, headers=headers, timeout=10)
        
        if dat_url and resp.status_code == 200:
            resp.encoding = "CP932"
            text_data = resp.text
            if thread_id:
                cache_path = os.path.join(CACHE_DIR, f"{thread_id}.dat")
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(text_data)
            return parse_dat_content(text_data, spam_list)
            
        logging.info("Fallback to HTML parsing...")
        resp.encoding = "CP932"
        soup = BeautifulSoup(resp.text, "html.parser")
        comments = []
        msgs = soup.find_all("div", class_="message")
        if not msgs: msgs = soup.find_all("dd", class_="thread_in")
        if not msgs: msgs = soup.select("div.post > div.message")
            
        for msg in msgs[1:]:
            text = msg.get_text(strip=True)
            
            if text:
                is_spam = False
                for s in spam_list:
                    if s in text:
                        is_spam = True
                        break
                if is_spam: continue
                
                comments.append(text)

        if not comments: return ""
        full_text = "\n".join(comments)
        if len(full_text) > 30000: return full_text[:30000]
        return full_text

    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return ""

def analyze_market_data(text, exclude_list, prev_state=None):
    """
    Combined analysis: Extracts tickers AND generates a summary in one shot.
    """
    logging.info("Analyzing with Gemini (Combined Ticker Extraction & Summary & Breaking News)...")
    
    # Build Context String
    context_info = "No previous data available."
    if prev_state:
        try:
            prev_rank = ", ".join([f"{x['ticker']}(#{i+1})" for i, x in enumerate(prev_state.get('rankings', [])[:5])])
            prev_ongi = prev_state.get('fear_greed', 50)
            prev_radar = prev_state.get('radar', {})
            context_info = f"Previous Rankings: {prev_rank}. Previous Ongi Score: {prev_ongi}. Previous Radar: {json.dumps(prev_radar)}."
        except:
            pass

    prompt_text = f"""
    You are a cynical 5ch Market AI.
    IMPORTANT POLICY: The PRIMARY GOAL is accurate Ticker Ranking. Extracting every single mentioned ticker is the #1 PRIORITY.
    Prioritize ACCURACY over speed. Take your time to ensure high precision in ticker extraction and sentiment analysis.
    Analyze the following text to extract US stock trends, a general summary, a vibe check, and 5 specific sentiment metrics.

    CONTEXT FROM PREVIOUS RUN (Use this for "Breaking News" comparison):
    {context_info}

    1. Identify US stock tickers:
       - Map company names to valid US tickers (e.g. "Apple" -> AAPL).
       - Sentiment (-1.0 to 1.0).
       - Exclude: {json.dumps(exclude_list)}
       - Extract ALL mentioned tickers. Do not limit to Top 10. Aim for Top 20 if data allows.

    2. Analyze Market Sentiment (Ongi & Greed):
       - Score 0-100 (0=Despair, 100=Euphoria).

    3. Extract 5 Radar Metrics (0-10 Scale):
       - "hype": Momentum/Excitement
       - "panic": Fear/Despair
       - "faith": HODL mentality/Confidence
       - "gamble": Pure Gambling/Speculation (Shakoushin/High risk appetite)
       - "iq": Quality of discussion (vs noise)

    4. Write TWO Summaries (max 100 chars each):
       - "summary": General market news/movers. Style: Highly entertaining and cynical. ACCURATELY MIMIC the specific slang/tone used in the thread (e.g. if they say "God NVDA", use that). Do NOT use generic "www" unless the thread is full of it. Make it sound like a witty recap.
       - "ongi_comment": STRICT analysis of the THREAD's collective psychology/atmosphere. Analyze the residents' panic or delusion objectively. Do not focus on external market news, focus on the board's reaction. Style: Analytical, intellectual, cold Japanese.

    5. LIVE BREAKING NEWS (Jikkyo/Ticker Style):
       - Compare PREVIOUS vs CURRENT state.
       - Generate 1-3 short, punchy headlines (max 60 chars each).
       - Style: "Sports Commentary" or "Breaking News Ticker". DRAMATIC and EXAGGERATED.
       - Focus on CHANGE: Rank swaps, Sentiment flips (Fear->Greed), crash or moon.
       - Examples:
         - "【速報】SOXL、"阿鼻叫喚" から "脳汁" モードへ転換！買い豚の息が吹き返しました"
         - "【悲報】NVDA、順位ランクダウン。民度が "知性5" から "チンパン1" に低下中"
         - "【異変】TSLA、突然の急浮上！アンチが泡を吹いて倒れています"

    Output STRICT JSON format:
    {{
      "tickers": [
        {{ "ticker": "NVDA", "count": 15, "sentiment": 0.5 }},
        ...
      ],
      "fear_greed_score": 50,
      "radar": {{ "hype": 5, "panic": 5, "faith": 5, "gamble": 5, "iq": 5 }},
      "summary": "...",
      "ongi_comment": "...",
      "breaking_news": ["Headline 1", "Headline 2"]
    }}

    Text:
    {text[:400000]}
    """

    # Use fast and cost-effective models
    models = ["gemini-3-flash-preview", "gemini-2.5-flash", "gemini-2.5-flash-lite"]
    
    for model_name in models:
        logging.info(f"Trying model: {model_name}...")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {"response_mime_type": "application/json"} 
        }
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=600)
            if resp.status_code == 200:
                logging.info(f"Gemini Success ({model_name})")
                result = resp.json()
                try:
                    content = result["candidates"][0]["content"]["parts"][0]["text"]
                    data = json.loads(content)
                    return data.get("tickers", []), data.get("summary", "相場は混沌としています..."), data.get("fear_greed_score", 50), data.get("radar", {}), data.get("ongi_comment", ""), data.get("breaking_news", [])
                except Exception:
                    logging.warning(f"Parsing response failed for {model_name}")
            else:
                logging.warning(f"Model {model_name} returned status: {resp.status_code}")
                
        except Exception as e:
            logging.error(f"Request error for {model_name}: {e}")
            
    logging.error("All Gemini models failed.")
    return [], "要約生成失敗", 50, {}, "", []

def analyze_topics(text, stopwords_list=[]):
    logging.info("Analyzing topics (Keyword Extraction)...")
    stop_words = set(stopwords_list)
    
    words = []
    try:
        from janome.tokenizer import Tokenizer
        t = Tokenizer()
        tokens = t.tokenize(text)
        for token in tokens:
            pos_parts = token.part_of_speech.split(',')
            main_pos = pos_parts[0]
            sub_pos = pos_parts[1]
            if main_pos == '名詞' and sub_pos not in ['非自立', '代名詞', '数', '接尾']:
                word = token.surface
                word = token.surface
                if word.isdigit() or len(word) < 2 or word in stop_words: continue
                # Filter symbols (Half-width and Full-width) including ～, ：, ”, ％
                if re.search(r'[!-/:-@[-`{-~]', word): continue 
                if re.search(r'[！-／：-＠［-｀｛-～、-〜”’・％]', word): continue
                if 'http' in word or '.com' in word: continue
                words.append(word)
    except ImportError:
        logging.warning("Janome not found. Falling back to Regex.")
        pattern = re.compile(r"([一-龠ァ-ヶa-zA-Z]{2,})") 
        matches = pattern.findall(text)
        for w in matches:
            if w not in stop_words and not w.isdigit() and len(w) > 1:
                words.append(w)

    from collections import Counter
    counter = Counter(words)
    
    # Format top 50
    top_words = [{"word": k, "count": v} for k, v in counter.most_common(50)]
    
    logging.info("--- Top Topics ---")
    for t in top_words[:5]:
        logging.info(f"{t['word']}: {t['count']}")
        
    return top_words

def send_to_worker(items, topics, sources, overview="", ongi_comment="", fear_greed=50, radar={}, breaking_news=[]):
    logging.info(f"Sending {len(items)} tickers, {len(topics)} topics, and overview to Worker...")
    if not WORKER_URL or not INGEST_TOKEN:
        logging.warning("Worker config missing. Skipping upload.")
        return

    payload = {
        "window": "24h",
        "items": items,
        "topics": topics,
        "sources": sources,
        "overview": overview,
        "ongi_comment": ongi_comment,
        "fear_greed": fear_greed,
        "radar": radar,
        "breaking_news": breaking_news
    }
    
    base_url = WORKER_URL.rstrip("/")
    url = f"{base_url}/internal/ingest"
    headers = { "Authorization": f"Bearer {INGEST_TOKEN}", "Content-Type": "application/json" }
    
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            logging.info("Success! Data uploaded.")
        else:
            logging.error(f"Worker Error: {resp.status_code}")
    except Exception as e:
        logging.error(f"Upload failed: {e}")

STATE_FILE = "last_run.json"

def load_prev_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_current_state(data):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save state: {e}")

def run_analysis(debug_mode=False):
    cleanup_old_files()
    stopwords, exclude, spam = load_config()
    threads = discover_threads()
    if not threads:
        logging.info("No threads found.")
        return

    # Load Previous State
    prev_state = load_prev_state()

    all_text = ""
    source_meta = []
    
    for t in threads:
        text = fetch_thread_text(t["url"], spam)
        if text:
            all_text += f"\n--- Thread: {t['name']} ---\n{text}"
            source_meta.append({"name": t["name"], "url": t["url"]})
        time.sleep(1) 
    
    if not all_text.strip(): return

    topics = analyze_topics(all_text, stopwords)

    if debug_mode:
        logging.info("DEBUG MODE: Skipping AI and Upload.")
        return

    # Combined Gemini Analysis with Context
    tickers_raw, market_summary, fear_greed, radar_data, ongi_comment, breaking_news = analyze_market_data(all_text, exclude, prev_state)
    
    # Validation: If Gemini failed, DO NOT upload empty data (protects backend DB)
    if market_summary == "要約生成失敗":
        logging.error("Analysis Failed (Gemini API Error). Aborting upload.")
        return
    
    agg = {}
    for t in tickers_raw:
        sym = t.get("ticker", "").upper()
        cnt = t.get("count", 0)
        sent = t.get("sentiment", 0.0)
        if sym:
            if sym not in agg: agg[sym] = {"count": 0, "sent_w_sum": 0.0}
            agg[sym]["count"] += cnt
            agg[sym]["sent_w_sum"] += (sent * cnt)
    
    final_items = []
    for k, v in agg.items():
        avg_sent = v["sent_w_sum"] / v["count"] if v["count"] > 0 else 0.0
        final_items.append({ "ticker": k, "count": v["count"], "sentiment": round(avg_sent, 2) })

    final_items.sort(key=lambda x: x["count"], reverse=True)
    
    # Save State for Next Run
    current_state = {
        "timestamp": time.time(),
        "rankings": final_items[:10], # Top 10 for context
        "fear_greed": fear_greed,
        "radar": radar_data
    }
    save_current_state(current_state)
    
    logging.info("--- Top 20 Tickers ---")
    for i in final_items[:20]:
        logging.info(f"{i['ticker']}: {i['count']} (Sent: {i['sentiment']})")
    logging.info(f"Summary: {market_summary}")
    logging.info(f"Ongi Comment: {ongi_comment}")
    logging.info(f"Breaking News: {breaking_news}")
    logging.info(f"Fear & Ongi: {fear_greed}")

    send_to_worker(final_items, topics, source_meta, overview=market_summary, ongi_comment=ongi_comment, fear_greed=fear_greed, radar=radar_data, breaking_news=breaking_news)

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "monitor":
        logging.info("--- MONITOR MODE (120s) ---")
        try:
            while True:
                run_analysis(debug_mode=False)
                logging.info("Waiting 120s...")
                time.sleep(120) 
        except KeyboardInterrupt:
            logging.info("Monitor stopped.")
    elif mode == "debug":
        run_analysis(debug_mode=True)
    else:
        run_analysis(debug_mode=False)

if __name__ == "__main__":
    main()
