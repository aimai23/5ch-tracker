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
STATE_FILE = os.path.join(BASE_DIR, "last_run.json")
SPAM_SCORE_THRESHOLD = int(os.getenv("SPAM_SCORE_THRESHOLD", "7"))
SPAM_DUP_THRESHOLD = int(os.getenv("SPAM_DUP_THRESHOLD", "2"))
SPAM_ID_LIMIT = int(os.getenv("SPAM_ID_LIMIT", "25"))
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))

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
    m = re.search(r"ID:([A-Za-z0-9+/]+)", meta_text)
    return m.group(1) if m else None

def normalize_message(text):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[\W_]+", "", text)
    return text

def clean_message(text):
    import html
    if not text:
        return ""
    text = re.sub(r"https?://[\w/:%#\$&\?\(\)~\.=\+\-]+", "", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(text).strip()

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

        watchlist.append({
            "ticker": ticker,
            "reason": reason,
            "catalyst": catalyst,
            "risk": risk,
            "invalidation": invalidation,
            "valid_until": valid_until,
            "confidence": confidence
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

    if re.search(r"(.)\1{8,}", message):
        score += 3
    if re.search(r"[!\uFF01?\uFF1Fw\uFF57]{6,}", message):
        score += 2
    if re.search(r"https?://", message):
        score += 2

    compact = re.sub(r"\s+", "", message)
    if compact:
        meaningful = len(re.findall(r"[A-Za-z0-9?-??-??-?]", compact))
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

    if re.search(r"\$[A-Za-z]{1,5}\b|\b[A-Z]{2,5}\b", message):
        score = max(score - 2, 0)

    return score

def parse_dat_content(text_data, spam_list=[]):
    comments = []
    dup_counter = {}
    id_counter = {}
    filtered = 0
    for line in text_data.splitlines()[1:]:
        parts = line.split("<>")
        if len(parts) >= 4:
            meta = parts[2]
            msg = parts[3]

            clean_msg = clean_message(msg)
            if not clean_msg:
                continue

            user_id = extract_post_id(meta)
            score = spam_score_message(clean_msg, spam_list, dup_counter, id_counter, user_id)
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
            
        dup_counter = {}
        id_counter = {}
        filtered = 0
        for msg in msgs[1:]:
            text = msg.get_text(strip=True)

            clean_text = clean_message(text)
            if not clean_text:
                continue

            score = spam_score_message(clean_text, spam_list, dup_counter, id_counter, None)
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

def analyze_market_data(text, exclude_list, nicknames={}, prev_state=None, reddit_rankings=[], doughcon_data=None, sahm_data=None):
    """
    Combined analysis: Extracts tickers, Generates Summary, AND Comparative Insight.
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

    prompt_text = f"""
    You are a cynical 5ch Market AI.
    IMPORTANT POLICY: The PRIMARY GOAL is accurate Ticker Ranking. Extracting every single mentioned ticker is the #1 PRIORITY.
    Prioritize ACCURACY over speed. Take your time to ensure high precision in ticker extraction and sentiment analysis.
    LANGUAGE: JAPANESE ONLY (for all text outputs like summaries and news).
    Analyze the following text to extract US stock trends, a general summary, a vibe check, 5 specific sentiment metrics, AND A COMPARATIVE INSIGHT.

    CONTEXT:
    1. PREVIOUS RUN (Use for "Breaking News" comparison): {context_info}
    2. {reddit_context}
    3. {crisis_context}

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
         - IMPORTANT: Do NOT add "(TICKER)" after nicknames. Just use the nickname naturally (e.g. "珍テル" is fine, NEVER "珍テル(INTC)").
       - "ongi_comment": STRICT analysis of the THREAD's collective psychology/atmosphere. Analyze the residents' panic or delusion objectively. Do not focus on external market news, focus on the board's reaction. Style: Analytical, intellectual, cold Japanese. (Max 100 chars).

    5. LIVE BREAKING NEWS (Jikkyo/Ticker Style):
       - Compare PREVIOUS vs CURRENT state.
       - Generate 1-3 short, punchy headlines (max 60 chars each).
       - Style: "Sports Commentary" or "Breaking News Ticker". DRAMATIC and EXAGGERATED.
       - IMPORTANT: DO NOT use "【速報】". You can use "【悲報】", "【朗報】", "【異変】" etc, or just start with the Ticker.
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
       BRIEF GROUNDING RULES (Section 7 only):
       - Use only facts/tickers/events that appear in TEXT or CONTEXT. Do NOT invent specifics.
       - If a detail is not explicit, you MAY infer for catalyst/risk/valid_until using focus_themes/cautions; avoid specific dates unless present; prefer generic phrases over blanks.
       - Do NOT add macro events or data that are not present in TEXT/CONTEXT.
       - If a field lacks explicit evidence, use ONLY the generic placeholders listed below; do not invent specifics.
       - Output must include all required keys. Use empty strings/arrays instead of null.
       - Provide TWO briefs: "brief_swing" (few-day swing) and "brief_long" (mid/long term).
       - Each brief should include:
         - "headline": short 1-line summary (Max 80 chars)
         - "market_regime": e.g. Risk-on / Risk-off / Mixed (JP okay)
         - "focus_themes": 2-5 themes (short phrases)
         - "watchlist": up to 8 items with:
           { "ticker", "reason", "catalyst", "risk", "invalidation", "valid_until", "confidence" }
         - "cautions": 2-4 items
         - "catalyst_calendar": 3-6 items with:
           { "date", "event", "note", "impact" } where impact is one of "low" | "mid" | "high"
       - IMPORTANT: Do NOT say Buy/Sell/Entry/Target. Only monitoring language.
       - Keep it practical and grounded in the thread context.
       - Self-check pass: DO NOT remove items solely for weak evidence.
       - If ticker is in extracted tickers but not clearly in TEXT/CONTEXT, keep it with confidence="low".
       - Set confidence per item: "high" (explicit), "mid" (implied by themes/cautions), "low" (generic placeholder).
       - Ensure reason/catalyst/risk/invalidation/valid_until are not empty.
       - If missing, fill with allowed generic placeholders (do not invent specifics).
       - Allowed generic placeholders (ONLY when evidence is missing):
         - catalyst: "\u30c6\u30fc\u30de:<focus theme>" / "\u8a71\u984c\u5148\u884c" / "\u9700\u7d66\u4e3b\u5c0e"
         - risk: "\u6ce8\u610f:<caution>" / "\u53cd\u52d5\u30ea\u30b9\u30af" / "\u8a71\u984c\u6e1b\u901f"
         - invalidation: "\u8a71\u984c\u6c88\u9759" / "\u9700\u7d66\u53cd\u8ee2"
         - valid_until: "\u4eca\u9031\u672b\u307e\u3067" (brief_swing) / "\u4eca\u6708\u672b\u307e\u3067" (brief_long) / "\u672a\u5b9a"
    Text:
    {text[:400000]}
    """

    # Use fast and cost-effective models
    models = ["gemini-3-flash-preview", "gemini-2.5-flash"]
    
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
                usage = result.get("usageMetadata", {})
                prompt_tokens = usage.get("promptTokenCount", "N/A")
                total_tokens = usage.get("totalTokenCount", "N/A")
                logging.info(f"Token Usage - Input: {prompt_tokens}, Total: {total_tokens}")
                try:
                    content = result["candidates"][0]["content"]["parts"][0]["text"]
                    data = json.loads(content)
                    brief_swing = sanitize_brief(data.get("brief_swing", {}), mode="swing")
                    brief_long = sanitize_brief(data.get("brief_long", {}), mode="long")
                    return data.get("tickers", []), data.get("summary", "\u76f8\u5834\u306f\u6df7\u6c8c\u3068\u3057\u3066\u3044\u307e\u3059..."), data.get("fear_greed_score", 50), data.get("radar", {}), data.get("ongi_comment", ""), data.get("breaking_news", []), data.get("comparative_insight", ""), brief_swing, brief_long, model_name
                except Exception:
                    logging.warning(f"Parsing response failed for {model_name}")
            else:
                logging.warning(f"Model {model_name} returned status: {resp.status_code}")
                
        except Exception as e:
            logging.error(f"Request error for {model_name}: {e}")
            
    logging.error("All Gemini models failed.")
    return [], "要約生成失敗", 50, {}, "", [], "", {}, {}, "Gemini (Fallback)"

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
        tickers, topics, source_meta, summary, ongi_comment, fear_greed, radar, breaking_news, polymarket, cnn_fg, reddit_rankings, comparative_insight, brief_swing, brief_long, ai_model, doughcon_data, sahm_data, yield_curve_data, crypto_fg
    ):
    logging.info(f"Sending {len(tickers)} tickers, {len(topics)} topics, {len(polymarket or [])} polymarket, {len(reddit_rankings or [])} reddit items to Worker...")
    if not WORKER_URL or not INGEST_TOKEN:
        logging.warning("Worker config missing. Skipping upload.")
        return

    logging.info(f"Payload Indicators: Pizza={doughcon_data is not None}, Sahm={sahm_data is not None}, Yield={yield_curve_data is not None}, CryptoFG={crypto_fg is not None}")
    
    payload = {
        "updatedAt": datetime.datetime.now().isoformat(),
        "window": "24h",
        "items": tickers,
        "topics": topics,
        "sources": source_meta,
        "overview": summary,
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
        "yield_curve": yield_curve_data
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
            logging.error(f"Worker Response: {resp.text}")
    except Exception as e:
        logging.error(f"Upload failed: {e}")

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
            is_excluded = False
            for kw in excluded_keywords:
                if kw.lower() in title.lower():
                    is_excluded = True
                    break
            
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
        url = "https://fred.stlouisfed.org/series/SAHMREALTIME"
        resp = fetch_with_retry(url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            val_el = soup.find(class_="series-meta-observation-value")
            if val_el:
                val = float(val_el.get_text(strip=True))
                # Sahm Rule Logic
                # >= 0.50: Danger
                # >= 0.30: Warning
                # < 0.30: Safe
                state = "Safe"
                if val >= 0.50: state = "Recession Signal"
                elif val >= 0.30: state = "Warning"
                
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
        url = "https://fred.stlouisfed.org/series/T10Y2Y"
        resp = fetch_with_retry(url)
        if resp:
            soup = BeautifulSoup(resp.text, "html.parser")
            val_el = soup.find(class_="series-meta-observation-value")
            if val_el:
                val = float(val_el.get_text(strip=True))
                # Yield Curve Logic
                # < 0: Inverted (Danger)
                # 0 - 0.2: Flattening (Warning)
                # > 0.2: Normal (Safe)
                state = "Normal"
                if val < 0: state = "Inverted"
                elif val < 0.2: state = "Flattening"
                
                return {
                    "value": val,
                    "state": state
                }
    except Exception as e:
        logging.warning(f"Failed to fetch Yield Curve: {e}")
    return None

def run_analysis(debug_mode=False, poly_only=False, retry_count=0):
    cleanup_old_files()
    stopwords, exclude, spam, nicknames = load_config()
    
    # Polymarket Fetch (Parallel-ish in effect)
    polymarket_raw = fetch_polymarket_events()
    polymarket_data = translate_polymarket_events(polymarket_raw)

    if poly_only:
        logging.info("--- POLYMARKET ONLY MODE ---")
        logging.info(json.dumps(polymarket_data, indent=2, ensure_ascii=False))
        return

    # External data (non-AI) should run before AI analysis
    reddit_data = fetch_apewisdom_rankings()

    doughcon_data = fetch_doughcon_level()
    if doughcon_data:
        logging.info(f"DOUGHCON Fetched: Level {doughcon_data['level']}")

    sahm_data = fetch_sahm_rule()
    if sahm_data:
        logging.info(f"Sahm Rule Fetched: {sahm_data['value']} ({sahm_data['state']})")

    crypto_fg = fetch_crypto_fear_greed()
    if crypto_fg:
        logging.info(f"Crypto F&G Fetched: {crypto_fg['value']} ({crypto_fg['classification']})")

    cnn_fg = fetch_cnn_fear_greed()
    if cnn_fg:
        logging.info(f"CNN Fear & Greed Fetched: {cnn_fg.get('score')} ({cnn_fg.get('rating')})")

    yield_curve_data = fetch_yield_curve()

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
    tickers_raw, market_summary, fear_greed, radar_data, ongi_comment, breaking_news, comparative_insight, brief_swing, brief_long, ai_model = analyze_market_data(
        all_text, exclude, nicknames, prev_state, reddit_data, doughcon_data, sahm_data
    )
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

    send_to_worker(final_items, topics, source_meta, market_summary, ongi_comment, fear_greed, radar_data, breaking_news, polymarket_data, cnn_fg, reddit_data, comparative_insight, brief_swing, brief_long, ai_model, doughcon_data, sahm_data, yield_curve_data, crypto_fg)

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
        run_analysis(debug_mode=False)
