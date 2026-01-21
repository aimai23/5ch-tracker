import requests
import json
import os
import sys

# Try to get API KEY from environment or simple hardcode check (User has it in env)
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

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
    
    titles = [e.get("title", "") for e in events]
    
    prompt = f"""
    Translate these prediction market event titles to Japanese. 
    Make them short, catchy.

    Titles:
    {json.dumps(titles)}

    Output JSON:
    {{
        "translations": ["Translated Title 1", "Translated Title 2", ...]
    }}
    """
    
    models = ["gemma-3-27b-it", "gemma-3-12b-it", "gemma-3-4b-it"]
    translations = titles
    
    for model_name in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = { 
            "contents": [{"parts": [{"text": prompt}]}], 
            "generationConfig": {"response_mime_type": "application/json"} 
        }
        try:
            print(f"Trying {model_name}...")
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            if resp.status_code == 200:
                res_json = resp.json()
                try:
                    content = res_json["candidates"][0]["content"]["parts"][0]["text"]
                    parsed = json.loads(content)
                    translations = parsed.get("translations", titles)
                    print(f"Success with {model_name}")
                    break 
                except: 
                    print("Parse Error")
            else:
                print(f"Failed {model_name}: {resp.status_code}")
        except Exception as e: 
            print(f"Err {model_name}: {e}")
            pass
            
    items = []
    for i, e in enumerate(events):
        item = e.copy()
        if i < len(translations):
            item["title_ja"] = translations[i]
        items.append(item)
    return items

def display_events(events):
    print("\n" + "="*80)
    print(f" FINAL DATA (Top {len(events)})")
    print("="*80)
    
    for i, e in enumerate(events, 1):
        title = e.get("title", "No Title")
        title_ja = e.get("title_ja", "")
        volume = float(e.get("volume", 0) or 0)
        
        # Outcomes
        markets = e.get("markets", [])
        outcomes_str = ""
        if markets:
            markets.sort(key=lambda x: float(x.get("volume", 0) or 0), reverse=True)
            main_market = markets[0]
            try:
                outcomes_raw = json.loads(main_market.get("outcomes", "[]"))
                outcome_prices = json.loads(main_market.get("outcomePrices", "[]"))
                
                parts = []
                for j, out_name in enumerate(outcomes_raw):
                    val = 0
                    if j < len(outcome_prices):
                        try:
                            val = float(outcome_prices[j]) * 100
                        except: val = 0
                    parts.append(f"{out_name}: {val:.1f}%")
                outcomes_str = " | ".join(parts[:2])
            except: pass

        print(f"{i}. {title}")
        if title_ja: print(f"   JA: {title_ja}")
        print(f"   Vol: ${volume:,.0f} | Odds: {outcomes_str}")
        print("-" * 40)

if __name__ == "__main__":
    data = fetch_polymarket_events()
    data = translate_polymarket_events(data)
    display_events(data)
