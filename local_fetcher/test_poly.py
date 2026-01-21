import requests
import json
import os
import sys
from dotenv import load_dotenv

# Load params like main.py
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    print("WARN: GEMINI_API_KEY is not set.")
    # Exit or allow fetch-only? User asked to reference it, so we try.


def fetch_polymarket_events():
    print("Fetching Polymarket data with Diversified Search...")
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
        except Exception as e:
            print(f"Error: {e}")
            return []

    queries = [
        {"tag_slug": "business", "sort": "volume", "limit": 10}, 
        {"q": "Fed", "sort": "volume", "limit": 5},              
        {"q": "Nvidia", "sort": "volume", "limit": 5},          
        {"q": "Rate", "sort": "volume", "limit": 5},
        {"q": "Japan", "sort": "volume", "limit": 5}
    ]
    
    all_events = []
    seen_ids = set()
    
    for q in queries:
        q["active"] = "true"
        q["closed"] = "false"
        res = get_events(q)
        print(f"Query [{q.get('tag_slug') or q.get('q')}]: Found {len(res)} items")
        
        for e in res:
            eid = e.get("id")
            if eid not in seen_ids:
                seen_ids.add(eid)
                all_events.append(e)

    all_events.sort(key=lambda x: float(x.get("volume", 0) or 0), reverse=True)
    return all_events[:10]

def translate_polymarket_events(events):
    if not events: return []
    if not GEMINI_API_KEY:
        print("WARN: GEMINI_API_KEY not found. Skipping translation.")
        return events

    print("Translating with Gemma-3-12b...")
    
    # 1. Prepare items with formatted Outcomes string (Logic from main.py)
    items = []
    for e in events:
        # Outcome Logic
        markets = e.get("markets", [])
        outcomes_str = ""
        if markets:
            markets.sort(key=lambda x: float(x.get("volume", 0) or 0), reverse=True)
            outcomes = []
            is_group = len(markets) > 1
            if is_group:
                 for m in markets[:3]:
                     label = m.get("groupItemTitle") or m.get("question")
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
                m = markets[0]
                try:
                    outcomes_raw = json.loads(m.get("outcomes", "[]"))
                    outcome_prices = json.loads(m.get("outcomePrices", "[]"))
                    temp_outs = []
                    for k, name in enumerate(outcomes_raw):
                        p = 0
                        if k < len(outcome_prices):
                            try: p = float(outcome_prices[k]) * 100
                            except: pass
                        temp_outs.append((name, p))
                    temp_outs.sort(key=lambda x: x[1], reverse=True)
                    outcomes = [f"{n}: {v:.1f}%" for n, v in temp_outs[:2]]
                except: pass
            outcomes_str = " | ".join(outcomes[:3])
            
        item = e.copy()
        item["outcomes"] = outcomes_str
        items.append(item)

    # 2. Batch Translate
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
    
    models = ["gemma-3-27b-it", "gemma-3-12b-it", "gemma-3-4b-it"]
    success = False
    
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = { "contents": [{"parts": [{"text": prompt}]}] }
        
        try:
            print(f"Trying {model_name}...")
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                res_json = resp.json()
                try:
                    content = res_json["candidates"][0]["content"]["parts"][0]["text"]
                    content = content.replace("```json", "").replace("```", "").strip()
                    if "{" in content:
                        content = content[content.find("{"):content.rfind("}")+1]
                    
                    parsed = json.loads(content)
                    results = parsed.get("results", [])
                    
                    # Apply
                    result_map = {r.get("id"): r for r in results}
                    for i, item in enumerate(items):
                         if i in result_map:
                             item["title_ja"] = result_map[i].get("title_ja", item["title"])
                             if result_map[i].get("outcomes_ja"):
                                item["outcomes"] = result_map[i].get("outcomes_ja")
                         else:
                             item["title_ja"] = item["title"]
                    
                    print(f"Success with {model_name}")
                    success = True
                    break 
                except: 
                    print("Parse Error (Non-JSON)")
            else:
                print(f"Failed {model_name}: {resp.status_code}")
        except Exception as e: 
            print(f"Err {model_name}: {e}")
            pass
            
    if not success:
        for item in items:
            item["title_ja"] = item["title"]

    return items

def display_events(events):
    print("\n" + "="*80)
    print(f" FINAL DATA (Top {len(events)})")
    print("="*80)
    
    for i, e in enumerate(events, 1):
        title = e.get("title", "No Title")
        title_ja = e.get("title_ja", "")
        volume = float(e.get("volume", 0) or 0)
        
        # Outcomes string is already generated and translated in e["outcomes"]
        outcomes_str = e.get("outcomes", "")

        print(f"{i}. {title}")
        if title_ja: print(f"   JA: {title_ja}")
        print(f"   Vol: ${volume:,.0f} | Odds: {outcomes_str}")
        print("-" * 40)

if __name__ == "__main__":
    data = fetch_polymarket_events()
    data = translate_polymarket_events(data)
    display_events(data)
