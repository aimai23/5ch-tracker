import requests
import json

def test_poly(label, params):
    url = "https://gamma-api.polymarket.com/events"
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"[{label}] Count: {len(data)}")
            if len(data) > 0:
                print(f"  Top: {data[0].get('title')}")
        else:
            print(f"[{label}] Error: {resp.status_code}")
    except Exception as e:
        print(f"[{label}] Exception: {e}")

print("Testing Polymarket API...")
test_poly("Business Tag", {"limit": 5, "active": "true", "closed": "false", "tag_slug": "business", "sort": "volume"})
test_poly("Finance Tag", {"limit": 5, "active": "true", "closed": "false", "tag_slug": "finance", "sort": "volume"})
test_poly("Economics Tag", {"limit": 5, "active": "true", "closed": "false", "tag_slug": "economics", "sort": "volume"})
test_poly("Search 'Stock'", {"limit": 5, "active": "true", "closed": "false", "q": "Stock", "sort": "volume"})
test_poly("Search 'Fed'", {"limit": 5, "active": "true", "closed": "false", "q": "Fed", "sort": "volume"})
test_poly("No Tag (Top Volume)", {"limit": 5, "active": "true", "closed": "false", "sort": "volume"})
