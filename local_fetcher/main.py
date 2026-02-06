import re
import math
import os
import sys
import json
import html
import requests
import time
import logging
import datetime
import glob
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
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
FRED_API_KEY = os.getenv("FRED_API_KEY")
STATE_FILE = os.path.join(BASE_DIR, "last_run.json")
CALENDAR_FILE = os.path.join(BASE_DIR, "finnhub_calendar.json")
SPAM_SCORE_THRESHOLD = int(os.getenv("SPAM_SCORE_THRESHOLD", "6"))
SPAM_DUP_THRESHOLD = int(os.getenv("SPAM_DUP_THRESHOLD", "2"))
SPAM_ID_LIMIT = int(os.getenv("SPAM_ID_LIMIT", "25"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))

if not GEMINI_API_KEY:
    logging.error("GEMINI_API_KEY is not set.")
    exit(1)

THREAD_URL_PATTERN = re.compile(r"https?://([^/]+)/test/read\.cgi/([^/]+)/(\d+)/?")
POST_ID_PATTERN = re.compile(r"ID:([A-Za-z0-9+/]+)")
NORMALIZE_URL_PATTERN = re.compile(r"https?://\S+")
WHITESPACE_PATTERN = re.compile(r"\s+")
NON_WORD_PATTERN = re.compile(r"[\W_]+")
CLEAN_URL_PATTERN = re.compile(r"https?://[\w/:%#\$&\?\(\)~\.=\+\-]+")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
JSON_CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
JSON_TRAILING_COMMA_PATTERN = re.compile(r",\s*([}\]])")
TICKER_FORMAT_PATTERN = re.compile(r"^[A-Z]{1,6}(?:\.[A-Z])?$")
TICKER_SCAN_PATTERN = re.compile(r"\$?[A-Z]{1,6}(?:\.[A-Z])?\b")
SHORT_NAME_SYMBOL_PATTERN = re.compile(r"[^\w\s]")
SPAM_REPEAT_PATTERN = re.compile(r"(.)\1{8,}")
SPAM_NOISE_PATTERN = re.compile(r"[!\uFF01?\uFF1Fw\uFF57]{6,}")
SPAM_URL_PATTERN = re.compile(r"https?://")
SPAM_MEANINGFUL_PATTERN = re.compile(r"[A-Za-z0-9?-??-??-?]")
SPAM_TICKER_HINT_PATTERN = re.compile(r"\$[A-Za-z]{1,5}\b|\b[A-Z]{2,5}\b")

_JANOME_TOKENIZER = None

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

    # Cleanup Logs (Keep 1 month)
    now = time.time()
    retention_days = LOG_RETENTION_DAYS
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
    
    nicknames = {}
    nick_path = os.path.join(os.path.dirname(BASE_DIR), "config", "nickname_dictionary.json")
    if os.path.exists(nick_path):
        try:
            with open(nick_path, "r", encoding="utf-8") as f:
                nicknames = json.load(f)
        except Exception as e:
            logging.warning(f"Failed to load nickname_dictionary.json: {e}")

    return stopwords, exclude, spam, nicknames

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
    top_threads = candidates[:5]
    for t in top_threads:
        logging.info(f"Found: {t['name']} (No.{t['num']})")
        
    return top_threads

def extract_post_id(meta_text):
    if not meta_text:
        return None
    m = POST_ID_PATTERN.search(meta_text)
    return m.group(1) if m else None

def normalize_message(text):
    if not text:
        return ""
    normalized = text.lower()
    normalized = NORMALIZE_URL_PATTERN.sub("", normalized)
    normalized = WHITESPACE_PATTERN.sub("", normalized)
    normalized = NON_WORD_PATTERN.sub("", normalized)
    return normalized

def clean_message(text):
    if not text:
        return ""
    cleaned = CLEAN_URL_PATTERN.sub("", text)
    cleaned = HTML_TAG_PATTERN.sub(" ", cleaned)
    return html.unescape(cleaned).strip()

def sanitize_brief(brief, max_watchlist=8, mode="swing"):
    def to_text(value):
        if value is None:
            return ""
        text = str(value).strip()
        text = re.sub(r"^\u63a8\u6e2c[:\uFF1A]", "", text).strip()
        return text

    def to_list(value):
        return value if isinstance(value, list) else []

    if not isinstance(brief, dict):
        brief = {}

    focus_themes = [to_text(x) for x in to_list(brief.get("focus_themes"))]
    focus_themes = [x for x in focus_themes if x]

    cautions = [to_text(x) for x in to_list(brief.get("cautions"))]
    cautions = [x for x in cautions if x]

    watchlist = []
    seen = set()
    theme_hint = focus_themes[0] if focus_themes else ""
    caution_hint = cautions[0] if cautions else ""
    for item in to_list(brief.get("watchlist")):
        if not isinstance(item, dict):
            continue
        ticker = to_text(item.get("ticker")).upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)

        reason_raw = to_text(item.get("reason"))
        catalyst_raw = to_text(item.get("catalyst"))
        risk_raw = to_text(item.get("risk"))
        invalidation_raw = to_text(item.get("invalidation"))
        valid_until_raw = to_text(item.get("valid_until") or item.get("deadline"))
        confidence_raw = to_text(item.get("confidence")).lower()
        bias_raw = to_text(item.get("bias")).lower()

        missing = sum(1 for v in [reason_raw, catalyst_raw, risk_raw, invalidation_raw, valid_until_raw] if not v)
        if confidence_raw in ["high", "mid", "low"]:
            confidence = confidence_raw
            if confidence == "high" and missing > 0:
                confidence = "mid"
            if missing > 2:
                confidence = "low"
        else:
            if missing == 0:
                confidence = "high"
            elif missing <= 2:
                confidence = "mid"
            else:
                confidence = "low"

        reason = reason_raw or "\u8a71\u984c\u4e0a\u4f4d\u306e\u305f\u3081\u76e3\u8996"
        catalyst = catalyst_raw or (f"\u30c6\u30fc\u30de:{theme_hint}" if theme_hint else "\u8a71\u984c\u5148\u884c")
        risk = risk_raw or (f"\u6ce8\u610f:{caution_hint}" if caution_hint else "\u53cd\u52d5\u30ea\u30b9\u30af")
        invalidation = invalidation_raw or "\u8a71\u984c\u6c88\u9759"
        if valid_until_raw:
            valid_until = valid_until_raw
        else:
            if mode == "long":
                valid_until = "\u4eca\u6708\u672b\u307e\u3067"
            elif mode == "swing":
                valid_until = "\u4eca\u9031\u672b\u307e\u3067"
            else:
                valid_until = "\u672a\u5b9a"

        if bias_raw in ["bull", "bear"]:
            bias = bias_raw
        else:
            bias_source = " ".join([reason_raw, catalyst_raw, risk_raw, invalidation_raw]).lower()
            bull_keys = ["強気", "上昇", "反発", "買い", "回復", "期待", "需要", "追い風", "好決算", "増", "上振れ", "支え", "底打ち", "安定", "資金流入", "上向き", "bull", "long"]
            bear_keys = ["弱気", "下落", "売り", "懸念", "警戒", "減速", "失速", "暴落", "崩壊", "下振れ", "逆風", "売却", "利確", "ロスカット", "下押し", "bear", "short"]
            bull_score = sum(1 for k in bull_keys if k in bias_source)
            bear_score = sum(1 for k in bear_keys if k in bias_source)
            if bull_score >= bear_score:
                bias = "bull"
            else:
                bias = "bear"

        watchlist.append({
            "ticker": ticker,
            "reason": reason,
            "catalyst": catalyst,
            "risk": risk,
            "invalidation": invalidation,
            "valid_until": valid_until,
            "confidence": confidence,
            "bias": bias
        })
        if len(watchlist) >= max_watchlist:
            break
    calendar = []
    for entry in to_list(brief.get("catalyst_calendar")):
        if isinstance(entry, str):
            cleaned = entry.strip()
            if cleaned:
                calendar.append(cleaned)
            continue
        if not isinstance(entry, dict):
            continue
        date = to_text(entry.get("date"))
        event = to_text(entry.get("event"))
        note = to_text(entry.get("note"))
        impact = to_text(entry.get("impact")).lower()
        if impact not in ["low", "mid", "high"]:
            impact = ""
        if date or event or note:
            calendar.append({
                "date": date,
                "event": event,
                "note": note,
                "impact": impact
            })

    return {
        "headline": to_text(brief.get("headline")),
        "market_regime": to_text(brief.get("market_regime")),
        "focus_themes": focus_themes,
        "watchlist": watchlist,
        "cautions": cautions,
        "catalyst_calendar": calendar
    }

def coerce_text(value):
    if value is None:
        return ""
    if isinstance(value, dict):
        return ""
    if isinstance(value, list):
        parts = [str(x).strip() for x in value if isinstance(x, (str, int, float))]
        return " ".join([p for p in parts if p])
    return str(value).strip()

def sanitize_for_json(value):
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    return value

def extract_json_from_text(text):
    if not text:
        return ""
    cleaned = text.strip()
    cleaned = cleaned.replace("```json", "").replace("```", "").strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        return cleaned[start:end+1]
    return cleaned

def parse_json_lenient(raw_text):
    if not raw_text:
        return None
    candidates = []
    extracted = extract_json_from_text(raw_text)
    if extracted:
        candidates.append(extracted)
    if extracted != raw_text:
        candidates.append(raw_text.strip())

    for cand in candidates:
        if not cand:
            continue
        # Remove ASCII control chars that break JSON parsing
        cleaned = JSON_CONTROL_CHARS_PATTERN.sub(" ", cand)
        cleaned = cleaned.strip()
        for attempt in (cand, cleaned, JSON_TRAILING_COMMA_PATTERN.sub(r"\1", cleaned)):
            try:
                return json.loads(attempt)
            except Exception:
                continue
    return None

def repair_json_with_gemini(raw_text, model_name="gemini-2.5-flash-lite"):
    if not raw_text:
        return None
    snippet = raw_text.strip()
    if len(snippet) > 12000:
        snippet = snippet[:12000]

    prompt = f"""
    You are a JSON repair tool.
    Fix the following content into VALID JSON only.
    - Output must be a single JSON object.
    - Do not add commentary or code fences.

    Input:
    {snippet}
    """

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code != 200:
            return None
        result = resp.json()
        content = result["candidates"][0]["content"]["parts"][0]["text"]
        return parse_json_lenient(content)
    except Exception:
        return None

def normalize_ticker_items(items, exclude_set):
    output = []
    if not isinstance(items, list):
        return output

    for item in items:
        ticker = ""
        count = 1
        sentiment = 0.0
        if isinstance(item, str):
            ticker = item.strip().upper()
        elif isinstance(item, dict):
            raw_ticker = item.get("ticker") or item.get("symbol") or item.get("code") or item.get("name")
            ticker = str(raw_ticker).strip().upper() if raw_ticker else ""
            raw_count = item.get("count")
            if raw_count is None:
                raw_count = item.get("mentions") or item.get("frequency")
            try:
                count = int(float(raw_count))
            except Exception:
                count = 1
            raw_sent = item.get("sentiment")
            if raw_sent is None:
                raw_sent = item.get("score")
            try:
                sentiment = float(raw_sent)
            except Exception:
                sentiment = 0.0
        else:
            continue

        if not ticker:
            continue
        if exclude_set and ticker in exclude_set:
            continue
        if not TICKER_FORMAT_PATTERN.match(ticker):
            continue
        if count <= 0:
            count = 1
        output.append({"ticker": ticker, "count": count, "sentiment": sentiment})

    return output

def fallback_extract_tickers(text, nicknames, exclude_set):
    if not text:
        return []

    counts = {}
    for match in TICKER_SCAN_PATTERN.findall(text):
        ticker = match.lstrip("$")
        if exclude_set and ticker in exclude_set:
            continue
        if not TICKER_FORMAT_PATTERN.match(ticker):
            continue
        counts[ticker] = counts.get(ticker, 0) + 1

    if isinstance(nicknames, dict):
        for ticker, names in nicknames.items():
            if not ticker:
                continue
            tick = str(ticker).strip().upper()
            if exclude_set and tick in exclude_set:
                continue
            if not TICKER_FORMAT_PATTERN.match(tick):
                continue
            if not isinstance(names, list):
                continue
            for name in names:
                if not name:
                    continue
                name_str = str(name)
                if len(name_str) < 2 and not SHORT_NAME_SYMBOL_PATTERN.search(name_str):
                    continue
                count = text.count(name_str)
                if count:
                    counts[tick] = counts.get(tick, 0) + count

    items = [{"ticker": t, "count": c, "sentiment": 0.0} for t, c in counts.items()]
    items.sort(key=lambda x: x["count"], reverse=True)
    return items

def build_brief_from_tickers(tickers, headline, mode="swing"):
    seeds = []
    for item in tickers:
        if isinstance(item, dict):
            ticker = item.get("ticker")
        else:
            ticker = item
        if ticker:
            seeds.append({"ticker": ticker})
        if len(seeds) >= 8:
            break
    base = {"headline": headline, "watchlist": seeds}
    return sanitize_brief(base, mode=mode)

def spam_score_message(message, spam_list, dup_counter, id_counter, user_id=None):
    if not message:
        return 999

    for s in spam_list:
        if s and s in message:
            return 999

    score = 0
    length = len(message)
    if length <= 2:
        score += 3
    elif length <= 5:
        score += 1
    if length >= 500:
        score += 2
    if length >= 1000:
        score += 3

    if SPAM_REPEAT_PATTERN.search(message):
        score += 3
    if SPAM_NOISE_PATTERN.search(message):
        score += 2
    if SPAM_URL_PATTERN.search(message):
        score += 2

    compact = WHITESPACE_PATTERN.sub("", message)
    if compact:
        meaningful = len(SPAM_MEANINGFUL_PATTERN.findall(compact))
        ratio = meaningful / max(len(compact), 1)
        if ratio < 0.3 and len(compact) > 10:
            score += 2

    norm = normalize_message(message)
    if norm:
        dup_counter[norm] = dup_counter.get(norm, 0) + 1
        if dup_counter[norm] > SPAM_DUP_THRESHOLD:
            score += 3
        elif dup_counter[norm] > 1:
            score += 1

    if user_id:
        id_counter[user_id] = id_counter.get(user_id, 0) + 1
        if id_counter[user_id] > SPAM_ID_LIMIT and length < 60:
            score += 2
        if id_counter[user_id] > SPAM_ID_LIMIT + 10:
            score += 2

    if SPAM_TICKER_HINT_PATTERN.search(message):
        score = max(score - 2, 0)

    return score

def parse_dat_content(text_data, spam_list=None):
    if not text_data:
        return ""
    spam_terms = spam_list or ()
    comments = []
    dup_counter = {}
    id_counter = {}
    filtered = 0
    for line in text_data.splitlines()[1:]:
        parts = line.split("<>", 4)
        if len(parts) >= 4:
            meta = parts[2]
            msg = parts[3]

            clean_msg = clean_message(msg)
            if not clean_msg:
                continue

            user_id = extract_post_id(meta)
            score = spam_score_message(clean_msg, spam_terms, dup_counter, id_counter, user_id)
            if score >= SPAM_SCORE_THRESHOLD:
                filtered += 1
                continue

            comments.append(clean_msg)

    if filtered:
        logging.info(f"Soft-spam filtered: {filtered} posts")
    if not comments: return ""
    full_text = "\n".join(comments)
    if len(full_text) > 15000: return full_text[:15000]
    return full_text

def fetch_thread_text(url, spam_list=None):
    spam_terms = spam_list or ()
    # Setup Cache
    os.makedirs(CACHE_DIR, exist_ok=True)

    # Convert to dat URL
    dat_url = None
    thread_id = None
    m = THREAD_URL_PATTERN.match(url)
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
                        return parse_dat_content(content, spam_terms)
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
            return parse_dat_content(text_data, spam_terms)
            
        logging.info("Fallback to HTML parsing...")
        resp.encoding = "CP932"
        soup = BeautifulSoup(resp.text, "html.parser")
        comments = []
        msgs = soup.find_all("div", class_="message")
        if not msgs: msgs = soup.find_all("dd", class_="thread_in")
        if not msgs: msgs = soup.select("div.post > div.message")
            
        dup_counter = {}
        id_counter = {}
        filtered = 0
        for msg in msgs[1:]:
            text = msg.get_text(strip=True)

            clean_text = clean_message(text)
            if not clean_text:
                continue

            score = spam_score_message(clean_text, spam_terms, dup_counter, id_counter, None)
            if score >= SPAM_SCORE_THRESHOLD:
                filtered += 1
                continue

            comments.append(clean_text)

        if filtered:
            logging.info(f"Soft-spam filtered (html): {filtered} posts")

        if not comments: return ""
        full_text = "\n".join(comments)
        if len(full_text) > 30000: return full_text[:30000]
        return full_text

    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return ""

def analyze_market_data(text, exclude_list, nicknames=None, prev_state=None, reddit_rankings=None, doughcon_data=None, sahm_data=None, earnings_hints=None):
    """
    Combined analysis: Extracts tickers, Generates Summary, AND Comparative Insight.
    """
    logging.info("Analyzing with Gemini (Combined Ticker Extraction & Summary & Breaking News)...")
    nicknames = nicknames or {}
    reddit_rankings = reddit_rankings or []
    earnings_hints = earnings_hints or []
    
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
    
    # Format Reddit Data for Context
    reddit_context = "No Reddit data available."
    if reddit_rankings:
        top_reddit = ", ".join([f"{r.get('ticker')}" for r in reddit_rankings[:15]])
        reddit_context = f"CURRENT US REDDIT TRENDS (WallStreetBets): {top_reddit}"

    # Crisis Indicators Context
    crisis_context = "Crisis Indicators: "
    if doughcon_data:
        crisis_context += f"DOUGHCON PENTAGON PIZZA INDEX: {doughcon_data.get('level')} ({doughcon_data.get('description')}). "
    crisis_context += "NOTE: Doughcon baseline is effectively DEFCON 4; treat a shift to 3 as the alert threshold. "
    if sahm_data:
        crisis_context += f"SAHM RULE RECESSION SIGNAL: {sahm_data.get('value')} (Status: {sahm_data.get('state')})."

    earnings_context = ""
    if earnings_hints:
        earnings_context = f"EARNINGS_HINTS (Top tickers from 5ch+Reddit, reference only): {json.dumps(earnings_hints, ensure_ascii=False)}"

    max_chars_primary = int(os.getenv("GEMINI_MAX_INPUT_CHARS", "300000"))
    max_chars_fallback = int(os.getenv("GEMINI_FALLBACK_INPUT_CHARS", "180000"))

    def build_prompt(max_chars):
        return f"""
    You are a cynical 5ch Market AI.
    IMPORTANT POLICY: The PRIMARY GOAL is accurate Ticker Ranking. Extracting every single mentioned ticker is the #1 PRIORITY.
    Prioritize ACCURACY over speed. Take your time to ensure high precision in ticker extraction and sentiment analysis.
    LANGUAGE: JAPANESE ONLY (for all text outputs like summaries and news).
    Analyze the following text to extract US stock trends, a general summary, a vibe check, 5 specific sentiment metrics, AND A COMPARATIVE INSIGHT.

    CONTEXT:
    1. PREVIOUS RUN (Use for "Breaking News" comparison): {context_info}
    2. {reddit_context}
    3. {crisis_context}
    4. {earnings_context}

    1. Identify US stock tickers:
       - Map company names to valid US tickers (e.g. "Apple","アップル","林檎" -> AAPL).
       - Sentiment (-1.0 to 1.0).
       - Exclude: {json.dumps(exclude_list)}
       - REFERENCE NICKNAMES (Use these to identify Tickers):
         {json.dumps(nicknames, ensure_ascii=False)}
       - Extract ALL mentioned tickers. Do not limit to Top 10. Aim for Top 20 if data allows.

    2. Analyze Market Sentiment (Ongi & Greed):
       - Score 0-100 (0=Despair, 100=Euphoria).

    3. Extract 5 Radar Metrics (0-10 Scale):
       - "hype": Momentum/Excitement
       - "panic": Fear/Despair
       - "faith": HODL mentality/Confidence
       - "gamble": Pure Gambling/Speculation (Shakoushin/High risk appetite)
       - "iq": Quality of discussion (vs noise)

    4. Write TWO Summaries:
       - "summary": General market news/movers. (Max 200 chars).
         - Style: Highly entertaining and cynical. ACCURATELY MIMIC the specific slang/tone used in the thread (e.g. if they say "God NVDA", use that). Do NOT use generic "www" unless the thread is full of it. Make it sound like a witty recap.
         - Cover multiple topics/tickers if possible. (Note: Topics do NOT have to be strictly about stocks. If the thread is buzzing about politics/games/food, include it).
         - IMPORTANT: Do NOT add "(TICKER)" after nicknames. Just use the nickname naturally (e.g. "パンチラ" is fine, NEVER "パンチラ（PLTR）").
       - "ongi_comment": STRICT analysis of the THREAD's collective psychology/atmosphere. Analyze the residents' panic or delusion objectively. Do not focus on external market news, focus on the board's reaction. Style: Analytical, intellectual, cold Japanese. (Max 100 chars).

    5. LIVE BREAKING NEWS (Jikkyo/Ticker Style):
       - Compare PREVIOUS vs CURRENT state.
       - Generate 1-3 short, punchy headlines (max 60 chars each).
       - Style: "Sports Commentary" or "Breaking News Ticker". DRAMATIC and EXAGGERATED.
       - IMPORTANT: DO NOT use "【速報】". You can use "【悲報】", "【朗報】", "【異変】" etc.
       - Focus on CHANGE: Rank swaps, Sentiment flips (Fear->Greed), crash or moon.
       - Examples:
         - "【朗報】SOXL、"阿鼻叫喚" から "脳汁" モードへ転換！買い豚の息が吹き返しました"
         - "【悲報】NVDA、順位ランクダウン。民度が "知性5" から "チンパン1" に低下中"
         - "【異変】TSLA、突然の急浮上！アンチが泡を吹いて倒れています"

    6. COMPARATIVE INSIGHT (JP 5ch vs US Reddit):
       - Compare the "JP 5ch Trends" (from your analysis of the text) vs "US Reddit Trends" (provided in Context).
       - Provide a "Deep Strategic Contrast" (Max 250 chars, Japanese).
       - Go beyond listing names. Analyze the *underlying psychology* or *sector preference* divergence.
       - Example: "Japan is defensive on Semis due to currency fears, while US is aggressively leveraging into Crypto miners."
       - Why is there a gap? what does it imply for the next 24h?
       - Tone: Professional Analyst, Insightful, Slightly Cynical.

    7. INVEST BRIEF (Monitor-only, NO trade advice):
       BRIEF RULES (Section 7 only):
       - Use tickers that appear in TEXT/CONTEXT. For those tickers, you MAY add general market context not in the TEXT; prefix it with "一般知識:" and set confidence="low".
       - If evidence is weak or implicit, still answer with short, generic wording and lower the confidence instead of omitting.
       - Confidence: "high" (explicit), "mid" (implied), "low" (generic/assumption).
       - Bias per watchlist item: "bull" (??????) or "bear" (??????). Bias is REQUIRED. This is a direction label only (NOT a trade recommendation).
       - Provide TWO briefs: "brief_swing" and "brief_long" with the same keys.
       - Each brief must include:
         - "headline" (Max 80 chars)
         - "market_regime" (日本語のみ。英語フレーズは使わず簡潔に翻訳)
         - "focus_themes" (3-5 items)
         - "watchlist" (exactly 8 items; pad with low-confidence items if needed):
           { "ticker", "reason", "catalyst", "risk", "invalidation", "valid_until", "confidence", "bias" }
         - "cautions" (3 items)
         - "catalyst_calendar" (3 items):
           { "date", "event", "note", "impact" } with impact in "low" | "mid" | "high"
       - IMPORTANT: Do NOT say Buy/Sell/Entry/Target. Only monitoring language.
       - Output must include all required keys. Use empty strings/arrays instead of null.
       OUTPUT JSON FORMAT (STRICT):
       {{
         "tickers": [{{ "ticker": "AAPL", "count": 12, "sentiment": 0.1 }}],
         "summary": "string (NOT object)",
         "ongi_comment": "string (NOT object)",
         "fear_greed_score": 50,
         "radar": {{ "hype": 0, "panic": 0, "faith": 0, "gamble": 0, "iq": 0 }},
         "breaking_news": ["..."],
         "comparative_insight": "string",
         "brief_swing": {{
           "headline": "...",
           "market_regime": "...",
           "focus_themes": ["..."],
           "watchlist": [{{ "ticker": "...", "reason": "...", "catalyst": "...", "risk": "...", "invalidation": "...", "valid_until": "...", "confidence": "high|mid|low" }}],
           "cautions": ["..."],
           "catalyst_calendar": [{{ "date": "...", "event": "...", "note": "...", "impact": "low|mid|high" }}]
         }},
         "brief_long": {{ "... same keys as brief_swing ..." }}
       }}
       - All keys must be present even if empty.
       - "summary" and "ongi_comment" must be plain strings, not nested objects.
       - Each ticker object must include "count" (>=1) and "sentiment" (-1.0 to 1.0).
    Text:
    {text[:max_chars]}
    """

    # Use fast and cost-effective models
    models = ["gemini-3-flash-preview", "gemini-2.5-flash"]
    
    for i, model_name in enumerate(models):
        max_chars = max_chars_primary if i == 0 else max_chars_fallback
        prompt_text = build_prompt(max_chars)
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
                usage = result.get("usageMetadata", {})
                prompt_tokens = usage.get("promptTokenCount", "N/A")
                total_tokens = usage.get("totalTokenCount", "N/A")
                logging.info(f"Token Usage - Input: {prompt_tokens}, Total: {total_tokens}")
                try:
                    content = result["candidates"][0]["content"]["parts"][0]["text"]
                    data = parse_json_lenient(content)
                    if data is None:
                        data = repair_json_with_gemini(content)
                        if data is not None:
                            logging.info(f"Repaired JSON response via Gemini for {model_name}")
                    if data is None:
                        raise ValueError("JSON parse failed")
                    if isinstance(data, dict) and isinstance(data.get("result"), dict):
                        data = data["result"]

                    summary_value = data.get("summary", "")
                    summary_obj = summary_value if isinstance(summary_value, dict) else None
                    summary_text = coerce_text(summary_value)
                    if not summary_text and summary_obj:
                        summary_text = coerce_text(summary_obj.get("summary") or summary_obj.get("text"))
                    if not summary_text:
                        summary_text = "\u76f8\u5834\u306f\u6df7\u6c8c\u3068\u3057\u3066\u3044\u307e\u3059..."

                    ongi_comment = coerce_text(data.get("ongi_comment", ""))
                    if not ongi_comment and summary_obj:
                        ongi_comment = coerce_text(summary_obj.get("ongi_comment") or summary_obj.get("comment"))

                    exclude_set = {str(x).upper() for x in exclude_list if isinstance(x, str)}
                    tickers_raw = normalize_ticker_items(
                        data.get("tickers") or data.get("items") or data.get("symbols") or [],
                        exclude_set
                    )
                    if not tickers_raw:
                        tickers_raw = fallback_extract_tickers(text, nicknames, exclude_set)

                    fear_greed_score = data.get("fear_greed_score", 50)
                    try:
                        fear_greed_score = int(float(fear_greed_score))
                    except Exception:
                        fear_greed_score = 50

                    radar = data.get("radar", {})
                    if not isinstance(radar, dict):
                        radar = {}

                    breaking_news = data.get("breaking_news", [])
                    if isinstance(breaking_news, str):
                        breaking_news = [breaking_news]
                    elif not isinstance(breaking_news, list):
                        breaking_news = []

                    comparative_insight = coerce_text(data.get("comparative_insight", ""))

                    brief_swing = sanitize_brief(data.get("brief_swing", {}), mode="swing")
                    brief_long = sanitize_brief(data.get("brief_long", {}), mode="long")
                    if summary_text:
                        if not brief_swing.get("headline"):
                            brief_swing["headline"] = summary_text
                        if not brief_long.get("headline"):
                            brief_long["headline"] = summary_text
                    if not brief_swing.get("watchlist") and tickers_raw:
                        brief_swing = build_brief_from_tickers(tickers_raw, summary_text, mode="swing")
                    if not brief_long.get("watchlist") and tickers_raw:
                        brief_long = build_brief_from_tickers(tickers_raw, summary_text, mode="long")

                    return tickers_raw, summary_text, fear_greed_score, radar, ongi_comment, breaking_news, comparative_insight, brief_swing, brief_long, model_name
                except Exception as parse_err:
                    logging.warning(f"Parsing response failed for {model_name}: {parse_err}")
            else:
                logging.warning(f"Model {model_name} returned status: {resp.status_code}")
                
        except Exception as e:
            logging.error(f"Request error for {model_name}: {e}")
            
    logging.error("All Gemini models failed.")
    return [], "要約生成失敗", 50, {}, "", [], "", {}, {}, "Gemini (Fallback)"

def get_janome_tokenizer():
    global _JANOME_TOKENIZER
    if _JANOME_TOKENIZER is None:
        from janome.tokenizer import Tokenizer
        _JANOME_TOKENIZER = Tokenizer()
    return _JANOME_TOKENIZER

def analyze_topics(text, stopwords_list=None):
    logging.info("Analyzing topics (Keyword Extraction)...")
    stop_words = set(stopwords_list or [])
    
    words = []
    try:
        tokenizer = get_janome_tokenizer()
        tokens = tokenizer.tokenize(text)
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

    counter = Counter(words)
    
    # Format top 50
    top_words = [{"word": k, "count": v} for k, v in counter.most_common(50)]
    
    logging.info("--- Top Topics ---")
    for t in top_words[:5]:
        logging.info(f"{t['word']}: {t['count']}")
        
    return top_words



def fetch_apewisdom_rankings():
    """Fetch Top 20 stocks from ApeWisdom (WallStreetBets)"""
    url = "https://apewisdom.io/api/v1.0/filter/wallstreetbets/page/1"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = []
            for item in data.get("results", [])[:20]:
                items.append({
                    "rank": item.get("rank"),
                    "ticker": item.get("ticker"),
                    "name": item.get("name"),
                    "count": item.get("mentions"),   # Use mentions as count equivalent
                    "upvotes": item.get("upvotes"),
                    "rank_24h_ago": item.get("rank_24h_ago"),
                    "mentions_24h_ago": item.get("mentions_24h_ago")
                })
            logging.info(f"Fetched {len(items)} Reddit rankings from ApeWisdom.")
            return items
    except Exception as e:
        logging.error(f"Failed to fetch ApeWisdom data: {e}")
    
    return []

def post_json_with_retry(url, headers, payload, retries=3, timeout=30):
    body = None
    try:
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except ValueError:
        safe_payload = sanitize_for_json(payload)
        body = json.dumps(safe_payload, ensure_ascii=False, allow_nan=False)

    body_bytes = body.encode("utf-8")
    send_headers = dict(headers or {})
    content_type = send_headers.get("Content-Type") or send_headers.get("content-type")
    if not content_type or "charset" not in content_type.lower():
        send_headers["Content-Type"] = "application/json; charset=utf-8"

    last_resp = None
    for attempt in range(retries):
        try:
            resp = requests.post(url, data=body_bytes, headers=send_headers, timeout=timeout)
            last_resp = resp
            if resp.status_code < 500 and resp.status_code != 429:
                return resp
            logging.warning(f"Worker upload attempt {attempt + 1} failed: {resp.status_code}")
        except Exception as e:
            logging.warning(f"Worker upload attempt {attempt + 1} error: {e}")

        if attempt < retries - 1:
            sleep_s = 5 * (2 ** attempt)
            time.sleep(sleep_s)

    return last_resp

def fetch_doughcon_data():
    """Fetch Doughcon data from the API."""
    logging.info("Fetching Doughcon data...")
    url = "https://doughcon.com/api/v1/data" # Placeholder URL
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            logging.info("Successfully fetched Doughcon data.")
            return data
        else:
            logging.warning(f"Doughcon API returned status: {resp.status_code}")
    except Exception as e:
        logging.error(f"Failed to fetch Doughcon data: {e}")
    return None

def    send_to_worker(
        tickers, topics, source_meta, summary, ongi_comment, fear_greed, radar, breaking_news, polymarket, cnn_fg, reddit_rankings, comparative_insight, brief_swing, brief_long, ai_model, doughcon_data, sahm_data, yield_curve_data, crypto_fg, hy_oas_data, market_breadth_data, volatility_data
    ):
    logging.info(f"Sending {len(tickers)} tickers, {len(topics)} topics, {len(polymarket or [])} polymarket, {len(reddit_rankings or [])} reddit items to Worker...")
    if not WORKER_URL or not INGEST_TOKEN:
        logging.warning("Worker config missing. Skipping upload.")
        return

    logging.info(
        "Payload Indicators: "
        f"Pizza={doughcon_data is not None}, "
        f"Sahm={sahm_data is not None}, "
        f"Yield={yield_curve_data is not None}, "
        f"CryptoFG={crypto_fg is not None}, "
        f"HY_OAS={hy_oas_data is not None}, "
        f"Breadth={market_breadth_data is not None}, "
        f"Volatility={volatility_data is not None}"
    )
    
    payload = {
        "updatedAt": datetime.datetime.now().isoformat(),
        "window": "24h",
        "items": tickers,
        "topics": topics,
        "sources": source_meta,
        "overview": summary,
        "summary": summary,
        "ongi_comment": ongi_comment,
        "comparative_insight": comparative_insight,
        "brief_swing": brief_swing,
        "brief_long": brief_long,
        "ai_model": ai_model,
        "fear_greed": fear_greed,
        "radar": radar,
        "breaking_news": breaking_news,
        "polymarket": polymarket or [],
        "reddit_rankings": reddit_rankings or [],
        "cnn_fear_greed": cnn_fg,
        "crypto_fear_greed": crypto_fg,
        "doughcon": doughcon_data,
        "sahm_rule": sahm_data,
        "yield_curve": yield_curve_data,
        "hy_oas": hy_oas_data,
        "market_breadth": market_breadth_data,
        "volatility": volatility_data
    }
    
    base_url = WORKER_URL.rstrip("/")
    url = f"{base_url}/internal/ingest"
    headers = { "Authorization": f"Bearer {INGEST_TOKEN}", "Content-Type": "application/json" }
    
    resp = post_json_with_retry(url, headers, payload, retries=3, timeout=30)
    if resp is None:
        logging.error("Upload failed: no response")
        return
    if resp.status_code == 200:
        logging.info("Success! Data uploaded.")
        try:
            res_json = resp.json()
            if isinstance(res_json, dict) and res_json.get("warnings"):
                logging.warning(f"Worker warnings: {res_json.get('warnings')}")
        except Exception:
            pass
        return

    logging.error(f"Worker Error: {resp.status_code}")
    logging.error(f"Worker Response: {resp.text}")

def fetch_polymarket_events():
    logging.info("Fetching Polymarket data with Diversified Search...")
    url = "https://gamma-api.polymarket.com/events"
    headers = {
        "User-Agent": "5ch-Tracker/1.0",
        "Accept": "application/json"
    }
    
    def get_events(params):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            return []
        except:
            return []

    # Load queries from config or use default
    config_path = os.path.join(os.path.dirname(BASE_DIR), "config", "polymarket.json")
    queries = []
    
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                queries = json.load(f)
            logging.info(f"Loaded {len(queries)} Polymarket queries from config.")
        except Exception as e:
            logging.warning(f"Failed to load polymarket.json: {e}")
    
    # Fallback if empty or failed
    if not queries:
        logging.warning("No Polymarket queries found in config. Skipping Polymarket fetch.")
        return []

    # Load excluded titles
    exclude_path = os.path.join(os.path.dirname(BASE_DIR), "config", "polymarket_exclude.json")
    excluded_keywords = []
    if os.path.exists(exclude_path):
        try:
            with open(exclude_path, "r", encoding="utf-8") as f:
                excluded_keywords = json.load(f)
            logging.info(f"Loaded {len(excluded_keywords)} exclusion keywords.")
        except Exception as e:
            logging.warning(f"Failed to load polymarket_exclude.json: {e}")
    excluded_keywords_lower = [kw.lower() for kw in excluded_keywords if isinstance(kw, str) and kw]

    # Process and Filter
    all_events = []
    seen_ids = set()
    
    for q in queries:
        q["active"] = "true"
        q["closed"] = "false"
        res = get_events(q)
        for e in res:
            eid = e.get("id")
            title = e.get("title", "")
            
            # Exclusion Check
            title_lower = title.lower()
            is_excluded = any(kw in title_lower for kw in excluded_keywords_lower)
            
            if is_excluded:
                continue

            if eid not in seen_ids:
                seen_ids.add(eid)
                all_events.append(e)
        logging.info(f"Polymarket Query '{q.get('q', q.get('tag_slug'))}': Found {len(res)} events.")

    # Sort everything by volume first
    all_events.sort(key=lambda x: float(x.get("volume", 0) or 0), reverse=True)
    
    logging.info(f"Total Polymarket events found: {len(all_events)}. Top 8 selected.")
    return all_events[:8]

def translate_polymarket_events(events):
    if not events: return []
    
    logging.info("Translating Polymarket events with Gemini...")
    
    items = []
    titles = []
    
    for e in events:
        title = e.get("title", "")
        # Find best market (highest volume)
        markets = e.get("markets", [])
        if not markets: continue
        
        # Sort markets by volume if possible, or just take first
        markets.sort(key=lambda x: float(x.get("volume", 0) or 0), reverse=True)
        # Determine if it's a group of binary markets (e.g. "Will X happen by Y?")
        # or a single market with multiple outcomes (e.g. "Election Winner")
        outcomes = []
        is_group = len(markets) > 1
        
        if is_group:
             # Take top 3 markets, assume they are binary Yes/No
             for m in markets[:3]:
                 label = m.get("groupItemTitle") or m.get("question")
                 # If label is too long/complex, might need trimming, but let's try raw first
                 # Fallback if label is missing
                 if not label: label = "Yes"
                 
                 price = 0
                 try:
                    prices = json.loads(m.get("outcomePrices", "[]"))
                    outs = json.loads(m.get("outcomes", "[]"))
                    target_idx = 0
                    if "Yes" in outs: target_idx = outs.index("Yes")
                    price = float(prices[target_idx]) * 100
                 except: pass
                 outcomes.append(f"{label}: {price:.1f}%")
        else:
            # Single market (could be binary or multi-outcome)
            m = markets[0]
            try:
                outcomes_raw = json.loads(m.get("outcomes", "[]"))
                outcome_prices = json.loads(m.get("outcomePrices", "[]"))
                temp_outs = []
                for i, name in enumerate(outcomes_raw):
                    p = 0
                    if i < len(outcome_prices):
                        try: p = float(outcome_prices[i]) * 100
                        except: pass
                    temp_outs.append((name, p))
                
                # Sort by probability
                temp_outs.sort(key=lambda x: x[1], reverse=True)
                outcomes = [f"{n}: {v:.1f}%" for n, v in temp_outs[:2]]
            except: pass

        item = {
            "title": title,
            "outcomes": " | ".join(outcomes[:3]), # Top 3 outcomes
            "url": f"https://polymarket.com/event/{e.get('slug')}",
            "volume": e.get("volume", 0)
        }
        items.append(item)
        titles.append(title)
        
    if not items: return []

    # Batch Translate
    batch_data = [{"id": i, "title": item["title"], "outcomes": item["outcomes"]} for i, item in enumerate(items)]

    prompt = f"""
    Translate the 'title' and 'outcomes' fields to Japanese.
    - Title: Natural, "Cool" news headline style.
    - Outcomes: Translate labels (Yes->はい, No->いいえ, Trump->トランプ). Keep numbers/symbols/separators exactly as is.
    - OUTPUT MUST BE VALID JSON ONLY.

    Input:
    {json.dumps(batch_data)}

    Output JSON Format:
    {{
        "results": [
            {{ "id": 0, "title_ja": "...", "outcomes_ja": "..." }}
        ]
    }}
    """
    
    models = ["gemma-3-27b-it", "gemma-3-12b-it", "gemini-2.5-flash-lite"]
    success = False
    
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        
        generation_config = {}
        if "gemini" in model_name:
             generation_config = {"response_mime_type": "application/json"}

        payload = { "contents": [{"parts": [{"text": prompt}]}] }
        if generation_config:
            payload["generationConfig"] = generation_config
            
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                res_json = resp.json()
                try:
                    content = res_json["candidates"][0]["content"]["parts"][0]["text"]
                    # Cleanup for manual JSON parsing
                    content = content.replace("```json", "").replace("```", "").strip()
                    if "{" in content:
                        content = content[content.find("{"):content.rfind("}")+1]
                    
                    parsed = json.loads(content)
                    results = parsed.get("results", [])
                    
                    # Apply translations
                    result_map = {r.get("id"): r for r in results}
                    for i, item in enumerate(items):
                         if i in result_map:
                             item["title_ja"] = result_map[i].get("title_ja", item["title"])
                             # Overwrite outcomes with translated version
                             if result_map[i].get("outcomes_ja"):
                                item["outcomes"] = result_map[i].get("outcomes_ja")
                         else:
                             item["title_ja"] = item["title"]
                    
                    logging.info(f"Polymarket Translation Success: {len(results)}/{len(items)}")
                    success = True
                    break 

                except Exception as e:
                     logging.warning(f"Polymarket Parse Error {model_name}: {e}")
            else:
                logging.warning(f"Polymarket Translation failed with {model_name}: Status {resp.status_code}")
        except Exception as e:
            logging.warning(f"Polymarket Translation model error {model_name}: {e}")
            pass
        
    if not success:
        logging.warning("All Polymarket translations failed. Using English.")
        for item in items:
            item["title_ja"] = item["title"]
            
    return items

def load_prev_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def load_finnhub_calendar():
    if not os.path.exists(CALENDAR_FILE):
        return []
    try:
        with open(CALENDAR_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("earnings", [])
        return items if isinstance(items, list) else []
    except Exception as e:
        logging.warning(f"Failed to load finnhub_calendar.json: {e}")
        return []

def build_ticker_pool(prev_state, reddit_rankings, limit=40):
    seen = set()
    ordered = []
    for item in (prev_state or {}).get("rankings", [])[:20]:
        ticker = str(item.get("ticker") or "").upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            ordered.append(ticker)

    reddit_source = (prev_state or {}).get("reddit_rankings") or reddit_rankings or []
    for item in reddit_source[:20]:
        ticker = str(item.get("ticker") or "").upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            ordered.append(ticker)
        if len(ordered) >= limit:
            break
    return ordered if limit is None else ordered[:limit]

def build_earnings_hints(earnings, tickers):
    if not earnings or not tickers:
        return []
    ticker_set = set(tickers)
    earliest = {}
    for item in earnings:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        date = str(item.get("date") or "")
        if not symbol or not date or symbol not in ticker_set:
            continue
        if symbol not in earliest or date < earliest[symbol]:
            earliest[symbol] = date
    hints = []
    for ticker in tickers:
        date = earliest.get(ticker)
        if date:
            hints.append({"ticker": ticker, "date": date})
    return hints

def save_current_state(data):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.warning(f"Failed to save state: {e}")

def fetch_cnn_fear_greed():
    try:
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://edition.cnn.com/"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            fg = data.get("fear_and_greed", {})
            score = fg.get("score")
            rating = fg.get("rating")
            timestamp = fg.get("timestamp")
            if score is not None:
                return {
                    "score": round(float(score), 1), 
                    "rating": rating, 
                    "timestamp": timestamp
                }
    except Exception as e:
        logging.warning(f"Failed to fetch CNN F&G: {e}")
    return None

def fetch_with_retry(url, retries=3, delay=2):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }
    
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp
            logging.warning(f"Fetch failed {url}: Status {resp.status_code}")
        except Exception as e:
            logging.warning(f"Fetch error {url}: {e}")
        
        if i < retries - 1:
            time.sleep(delay)
            
    return None

def fetch_fred_series_value(series_id):
    if not FRED_API_KEY:
        logging.warning("FRED_API_KEY not set. Skipping FRED API call.")
        return None

    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for obs in data.get("observations", []):
                val = obs.get("value")
                if val is None:
                    continue
                if isinstance(val, str) and val.strip() == ".":
                    continue
                try:
                    value = float(val)
                    logging.info(f"FRED API Success {series_id}: {value}")
                    return value
                except Exception:
                    continue
        else:
            logging.warning(f"FRED API error {series_id}: {resp.status_code}")
    except Exception as e:
        logging.warning(f"Failed to fetch FRED API series {series_id}: {e}")
    return None

def fetch_doughcon_level():
    try:
        url = "https://www.pizzint.watch/api/dashboard-data?nocache=1"
        resp = fetch_with_retry(url)
        if resp:
            data = resp.json()
            level = data.get("defcon_level")
            descriptions = {
                1: "EMERGENCY",
                2: "DANGER",
                3: "WARNING",
                4: "CAUTION",
                5: "SAFE"
            }
            return {
                "level": level,
                "description": descriptions.get(level, "Unknown")
            }
    except Exception as e:
        logging.warning(f"Failed to fetch DOUGHCON: {e}")
    return None

def fetch_sahm_rule():
    try:
        val = fetch_fred_series_value("SAHMREALTIME")
        if val is None:
            return None
        # Sahm Rule Logic
        # >= 0.50: Danger
        # >= 0.30: Warning
        # < 0.30: Safe
        state = "Safe"
        if val >= 0.50:
            state = "Recession Signal"
        elif val >= 0.30:
            state = "Warning"

        return {
            "value": val,
            "state": state
        }
    except Exception as e:
        logging.warning(f"Failed to fetch Sahm Rule: {e}")
    return None

def fetch_crypto_fear_greed():
    """Fetch Crypto Fear & Greed Index from Alternative.me"""
    try:
        url = "https://api.alternative.me/fng/?limit=1"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data['data']:
                item = data['data'][0]
                return {
                    "value": int(item['value']),
                    "classification": item['value_classification']
                }
    except Exception as e:
        logging.warning(f"Failed to fetch Crypto F&G: {e}")
    return None

def fetch_yield_curve():
    try:
        val = fetch_fred_series_value("T10Y2Y")
        if val is None:
            return None
        # Yield Curve Logic
        # < 0: Inverted (Danger)
        # 0 - 0.2: Flattening (Warning)
        # > 0.2: Normal (Safe)
        state = "Normal"
        if val < 0:
            state = "Inverted"
        elif val < 0.2:
            state = "Flattening"

        return {
            "value": val,
            "state": state
        }
    except Exception as e:
        logging.warning(f"Failed to fetch Yield Curve: {e}")
    return None

def fetch_hy_oas():
    value = fetch_fred_series_value("BAMLH0A0HYM2")
    if value is None:
        return None
    if value >= 7:
        state = "Stress"
    elif value >= 5:
        state = "Warning"
    elif value >= 3:
        state = "Normal"
    else:
        state = "Tight"
    return {
        "value": value,
        "state": state
    }

def fetch_market_breadth():
    try:
        url = "https://indexmood.com/breadth/advance-decline/today"
        resp = fetch_with_retry(url)
        if not resp or resp.status_code != 200:
            resp = requests.get(url, timeout=10)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            plain = " ".join(soup.stripped_strings)
            m = re.search(r"A\\s*/?\\s*D\\s*Line:\\s*([-0-9,]+).*?Trend:\\s*([A-Za-z]+)", plain, re.IGNORECASE)
            if not m:
                m = re.search(r"Net\\s+Advance\\s*/?\\s*Decline\\s*([-0-9,]+)", plain, re.IGNORECASE)
                trend_match = re.search(r"Trend:\\s*([A-Za-z]+)", plain, re.IGNORECASE)
                if m and trend_match:
                    m = (m.group(1), trend_match.group(1))
                else:
                    m = None
            if not m:
                value_match = re.search(r"Current\\s+Breadth\\s*([-0-9,]+)", plain, re.IGNORECASE)
                trend_match = re.search(r"Net\\s+Advance\\s*/?\\s*Decline\\s*[-–—−]\\s*([A-Za-z]+)", plain, re.IGNORECASE)
                if value_match and trend_match:
                    m = (value_match.group(1), trend_match.group(1))
            if not m:
                tokens = [t.strip(" :") for t in plain.split()]
                value = None
                trend = None
                for i in range(len(tokens) - 2):
                    if tokens[i].lower() == "current" and tokens[i + 1].lower().startswith("breadth"):
                        value = tokens[i + 2]
                        break
                for i in range(len(tokens) - 3):
                    if tokens[i].lower() == "net" and tokens[i + 1].lower().startswith("advance"):
                        if tokens[i + 2] in ["-", "–", "—", "−"]:
                            trend = tokens[i + 3]
                        else:
                            trend = tokens[i + 2]
                        break
                if value and trend:
                    m = (value, trend)
            if m:
                if isinstance(m, tuple):
                    value_raw, trend_raw = m
                else:
                    value_raw, trend_raw = m.group(1), m.group(2)
                value = float(str(value_raw).replace(",", ""))
                trend = str(trend_raw).capitalize()
                return {
                    "value": value,
                    "state": trend
                }
            logging.warning(f"IndexMood breadth parse failed. Snippet: {plain[:200]}")
    except Exception as e:
        logging.warning(f"Failed to fetch IndexMood breadth: {e}")

    return None

def fetch_volatility():
    vix = fetch_fred_series_value("VIXCLS")
    if vix is None:
        return None

    if vix >= 25:
        state = "Stress"
    elif vix >= 20:
        state = "Elevated"
    else:
        state = "Calm"

    return {
        "vix": vix,
        "state": state
    }

def safe_fetch(label, fn, default):
    try:
        return fn()
    except Exception as e:
        logging.warning(f"Failed to fetch {label}: {e}")
        return default

def log_debug_timing_summary(phase_times, total_elapsed, external_task_times=None, external_meta=None):
    logging.info("--- DEBUG TIMING SUMMARY ---")
    for phase, elapsed in sorted(phase_times.items(), key=lambda x: x[1], reverse=True):
        logging.info(f"DEBUG TIMING {phase}: {elapsed:.3f}s")
    logging.info(f"DEBUG TIMING total: {total_elapsed:.3f}s")

    if external_meta:
        wall_time = external_meta.get("wall_time", 0.0)
        sequential_estimate = external_meta.get("sequential_estimate", 0.0)
        saved_time = max(sequential_estimate - wall_time, 0.0)
        speedup = (sequential_estimate / wall_time) if wall_time > 0 else 0.0
        logging.info(
            "DEBUG TIMING external parallel: "
            f"wall={wall_time:.3f}s, seq_est={sequential_estimate:.3f}s, "
            f"saved={saved_time:.3f}s, speedup={speedup:.2f}x"
        )
        if external_task_times:
            for key, elapsed in sorted(external_task_times.items(), key=lambda x: x[1], reverse=True):
                logging.info(f"DEBUG TIMING external task {key}: {elapsed:.3f}s")

def fetch_external_data(include_timing=False):
    jobs = {
        "reddit_data": ("ApeWisdom", fetch_apewisdom_rankings, []),
        "doughcon_data": ("DOUGHCON", fetch_doughcon_level, None),
        "sahm_data": ("Sahm Rule", fetch_sahm_rule, None),
        "crypto_fg": ("Crypto Fear & Greed", fetch_crypto_fear_greed, None),
        "cnn_fg": ("CNN Fear & Greed", fetch_cnn_fear_greed, None),
        "yield_curve_data": ("Yield Curve", fetch_yield_curve, None),
        "hy_oas_data": ("HY OAS", fetch_hy_oas, None),
        "market_breadth_data": ("Market Breadth", fetch_market_breadth, None),
        "volatility_data": ("Volatility", fetch_volatility, None),
    }

    results = {}
    task_timings = {}

    def timed_job(label, fn, default):
        started = time.perf_counter()
        value = safe_fetch(label, fn, default)
        return value, time.perf_counter() - started

    wall_started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(8, len(jobs))) as executor:
        future_to_key = {
            executor.submit(timed_job, label, fn, default): (key, default)
            for key, (label, fn, default) in jobs.items()
        }
        for future in as_completed(future_to_key):
            key, default = future_to_key[future]
            try:
                value, elapsed = future.result()
                results[key] = value
                task_timings[key] = elapsed
            except Exception:
                results[key] = default
                task_timings[key] = 0.0

    wall_elapsed = time.perf_counter() - wall_started
    for key, (_, _, default) in jobs.items():
        results.setdefault(key, default)
        task_timings.setdefault(key, 0.0)

    if include_timing:
        sequential_estimate = sum(task_timings.values())
        meta = {
            "wall_time": wall_elapsed,
            "sequential_estimate": sequential_estimate
        }
        return results, task_timings, meta
    return results

def run_analysis(debug_mode=False, poly_only=False, retry_count=0):
    run_started = time.perf_counter()
    phase_times = {}
    external_task_times = {}
    external_meta = None

    phase_started = time.perf_counter()
    cleanup_old_files()
    stopwords, exclude, spam, nicknames = load_config()
    phase_times["setup_and_config"] = time.perf_counter() - phase_started
    
    # Polymarket Fetch (skip AI translation in debug mode)
    phase_started = time.perf_counter()
    polymarket_data = []
    if not debug_mode:
        polymarket_raw = safe_fetch("Polymarket events", fetch_polymarket_events, [])
        polymarket_data = safe_fetch("Polymarket translation", lambda: translate_polymarket_events(polymarket_raw), [])
    else:
        logging.info("DEBUG MODE: Skipping Polymarket translation (AI).")
    phase_times["polymarket"] = time.perf_counter() - phase_started

    if poly_only:
        if debug_mode:
            log_debug_timing_summary(
                phase_times,
                time.perf_counter() - run_started,
                external_task_times,
                external_meta
            )
        logging.info("--- POLYMARKET ONLY MODE ---")
        logging.info(json.dumps(polymarket_data, indent=2, ensure_ascii=False))
        return

    # External data (non-AI) should run before AI analysis
    phase_started = time.perf_counter()
    if debug_mode:
        external_data, external_task_times, external_meta = fetch_external_data(include_timing=True)
    else:
        external_data = fetch_external_data()
    phase_times["external_data_fetch"] = time.perf_counter() - phase_started
    reddit_data = external_data["reddit_data"]
    doughcon_data = external_data["doughcon_data"]
    sahm_data = external_data["sahm_data"]
    crypto_fg = external_data["crypto_fg"]
    cnn_fg = external_data["cnn_fg"]
    yield_curve_data = external_data["yield_curve_data"]
    hy_oas_data = external_data["hy_oas_data"]
    market_breadth_data = external_data["market_breadth_data"]
    volatility_data = external_data["volatility_data"]

    if doughcon_data:
        logging.info(f"DOUGHCON Fetched: Level {doughcon_data['level']}")

    if sahm_data:
        logging.info(f"Sahm Rule Fetched: {sahm_data['value']} ({sahm_data['state']})")

    if crypto_fg:
        logging.info(f"Crypto F&G Fetched: {crypto_fg['value']} ({crypto_fg['classification']})")

    if cnn_fg:
        logging.info(f"CNN Fear & Greed Fetched: {cnn_fg.get('score')} ({cnn_fg.get('rating')})")

    if yield_curve_data:
        logging.info(f"Yield Curve Fetched: {yield_curve_data['value']} ({yield_curve_data['state']})")

    if hy_oas_data:
        logging.info(f"HY OAS Fetched: {hy_oas_data['value']} ({hy_oas_data['state']})")

    if market_breadth_data:
        logging.info(f"Market Breadth Fetched: {market_breadth_data.get('state')} ({market_breadth_data.get('value')})")

    if volatility_data:
        logging.info(f"Volatility Fetched: VIX={volatility_data.get('vix')} MOVE={volatility_data.get('move')} ({volatility_data.get('state')})")

    phase_started = time.perf_counter()
    threads = discover_threads()
    phase_times["discover_threads"] = time.perf_counter() - phase_started
    if not threads:
        if debug_mode:
            log_debug_timing_summary(
                phase_times,
                time.perf_counter() - run_started,
                external_task_times,
                external_meta
            )
        logging.info("No threads found.")
        return

    # Load Previous State
    phase_started = time.perf_counter()
    prev_state = load_prev_state()
    earnings_calendar = load_finnhub_calendar()
    ticker_pool = build_ticker_pool(prev_state, reddit_data, limit=40)
    earnings_hints = build_earnings_hints(earnings_calendar, ticker_pool)
    phase_times["load_state_and_hints"] = time.perf_counter() - phase_started

    phase_started = time.perf_counter()
    all_text_chunks = []
    source_meta = []
    
    for t in threads:
        thread_started = time.perf_counter()
        text = fetch_thread_text(t["url"], spam)
        thread_elapsed = time.perf_counter() - thread_started
        if text:
            all_text_chunks.append(f"\n--- Thread: {t['name']} ---\n{text}")
            source_meta.append({"name": t["name"], "url": t["url"]})
        if debug_mode:
            logging.info(
                f"DEBUG TIMING thread_fetch {t['num']}: "
                f"{thread_elapsed:.3f}s, chars={len(text) if text else 0}"
            )
        time.sleep(1) 
    phase_times["thread_fetch_total"] = time.perf_counter() - phase_started
    
    all_text = "".join(all_text_chunks)
    if not all_text.strip():
        if debug_mode:
            log_debug_timing_summary(
                phase_times,
                time.perf_counter() - run_started,
                external_task_times,
                external_meta
            )
        return

    phase_started = time.perf_counter()
    topics = analyze_topics(all_text, stopwords)
    phase_times["topic_analysis"] = time.perf_counter() - phase_started

    if debug_mode:
        log_debug_timing_summary(
            phase_times,
            time.perf_counter() - run_started,
            external_task_times,
            external_meta
        )
        logging.info("DEBUG MODE: Skipping AI and Upload.")
        return

    # Combined Gemini Analysis with Context
    phase_started = time.perf_counter()
    tickers_raw, market_summary, fear_greed, radar_data, ongi_comment, breaking_news, comparative_insight, brief_swing, brief_long, ai_model = analyze_market_data(
        all_text, exclude, nicknames, prev_state, reddit_data, doughcon_data, sahm_data, earnings_hints
    )
    phase_times["ai_analysis"] = time.perf_counter() - phase_started
    if market_summary == "要約生成失敗":
        logging.error("Analysis Failed (Gemini API Error).")
        
        if retry_count < 1:
            logging.info("Waiting 10 minutes before retrying process from the beginning...")
            time.sleep(600)
            return run_analysis(debug_mode, poly_only, retry_count + 1)
        else:
            logging.error("Retry failed or limit reached. Aborting upload.")
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
    
    # Calculate Deltas from Previous State
    prev_ranks = {}
    if prev_state and "rankings" in prev_state:
        for i, item in enumerate(prev_state["rankings"]):
            t_name = item.get("ticker")
            if t_name:
                prev_ranks[t_name] = i + 1 # 1-based rank

    # Create temporary list properly first
    temp_list = []
    for k, v in agg.items():
        avg_sent = v["sent_w_sum"] / v["count"] if v["count"] > 0 else 0.0
        temp_list.append({ "ticker": k, "count": v["count"], "sentiment": round(avg_sent, 2) })
    
    # Sort to determine CURRENT rank
    temp_list.sort(key=lambda x: x["count"], reverse=True)

    # Assign Delta/New status
    for i, item in enumerate(temp_list):
        current_rank = i + 1
        ticker = item["ticker"]
        
        # Logic:
        # If in prev_ranks: Delta = Prev - Current
        #   (e.g. Prev=5, Cur=2 -> Delta = 3 (Up))
        #   (e.g. Prev=1, Cur=5 -> Delta = -4 (Down))
        # If not, Is New = True
        
        if ticker in prev_ranks:
            delta = prev_ranks[ticker] - current_rank
            item["rank_delta"] = delta
            item["is_new"] = False
        else:
            item["rank_delta"] = 0
            item["is_new"] = True
            
        final_items.append(item)

    # Limit top 20 for storing/sending is usually done by slice later
    # but final_items is effectively sorted now.
    
    # Save to last_run.json
    try:
        current_state = {
            "timestamp": time.time(),
            "rankings": final_items[:20],
            "fear_greed": fear_greed,
            "radar": radar_data,
            "doughcon": doughcon_data,
            "sahm_rule": sahm_data,
            "yield_curve": yield_curve_data,
            "crypto_fear_greed": crypto_fg,
            "reddit_rankings": reddit_data,
            "comparative_insight": comparative_insight,
            "ongi_comment": ongi_comment,
            "summary": market_summary,
            "brief_swing": brief_swing,
            "brief_long": brief_long,
            "breaking_news": breaking_news,
            "ai_model": ai_model
        }
        save_current_state(current_state)
    except Exception as e:
        logging.error(f"Failed to save state: {e}")
    
    logging.info("--- Top 20 Tickers ---")
    for i in final_items[:20]:
        logging.info(f"{i['ticker']}: {i['count']} (Sent: {i['sentiment']})")
    logging.info(f"Summary: {market_summary}")
    logging.info(f"Ongi Comment: {ongi_comment}")
    logging.info(f"Breaking News: {breaking_news}")
    logging.info(f"Fear & Ongi: {fear_greed}")

    if comparative_insight:
        logging.info(f"Comparative Insight: {comparative_insight}")

    phase_times["total"] = time.perf_counter() - run_started
    logging.info(
        f"TIMING total={phase_times['total']:.3f}s "
        f"(thread_fetch={phase_times.get('thread_fetch_total', 0.0):.3f}s, "
        f"external={phase_times.get('external_data_fetch', 0.0):.3f}s, "
        f"topics={phase_times.get('topic_analysis', 0.0):.3f}s, "
        f"ai={phase_times.get('ai_analysis', 0.0):.3f}s)"
    )

    send_to_worker(
        final_items, topics, source_meta, market_summary, ongi_comment, fear_greed, radar_data,
        breaking_news, polymarket_data, cnn_fg, reddit_data, comparative_insight,
        brief_swing, brief_long, ai_model, doughcon_data, sahm_data, yield_curve_data,
        crypto_fg, hy_oas_data, market_breadth_data, volatility_data
    )

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Run in debug mode (no upload)")
    parser.add_argument("--poly-only", action="store_true", help="Run only Polymarket fetch/translate experiment")
    parser.add_argument("--monitor", action="store_true", help="Run in monitor mode (loop every 120s)")
    args = parser.parse_args()

    if args.monitor:
        logging.info("--- MONITOR MODE (120s) ---")
        try:
            while True:
                run_analysis(debug_mode=args.debug, poly_only=args.poly_only)
                logging.info("Waiting 120s...")
                time.sleep(120) 
        except KeyboardInterrupt:
            logging.info("Monitor stopped.")
    else:
        run_analysis(debug_mode=args.debug)
