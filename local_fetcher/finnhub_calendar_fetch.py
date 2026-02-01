import argparse
import datetime
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(BASE_DIR, "finnhub_calendar.json")
BASE_URL = "https://finnhub.io/api/v1"


def iso_day(dt: datetime.date) -> str:
    return dt.strftime("%Y-%m-%d")


def build_range(from_arg: Optional[str], to_arg: Optional[str], days: int) -> Tuple[str, str]:
    if from_arg and to_arg:
        return from_arg, to_arg

    start = datetime.datetime.now(datetime.timezone.utc).date()
    end = start + datetime.timedelta(days=days)
    return iso_day(start), iso_day(end)


def fetch_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, dict) else {}

def chunk_dates(start: datetime.date, end: datetime.date, chunk_days: int) -> List[Tuple[datetime.date, datetime.date]]:
    chunks: List[Tuple[datetime.date, datetime.date]] = []
    cursor = start
    delta = datetime.timedelta(days=chunk_days - 1)
    while cursor <= end:
        chunk_end = min(end, cursor + delta)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + datetime.timedelta(days=1)
    return chunks

def fetch_earnings_range(from_date: str, to_date: str, token: str, chunk_days: int) -> Dict[str, Any]:
    start = datetime.datetime.strptime(from_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(to_date, "%Y-%m-%d").date()
    items: List[Dict[str, Any]] = []
    seen = set()
    errors: List[str] = []

    for chunk_start, chunk_end in chunk_dates(start, end, chunk_days):
        params: Dict[str, Any] = {
            "from": iso_day(chunk_start),
            "to": iso_day(chunk_end),
            "token": token,
        }
        try:
            data = fetch_json(f"{BASE_URL}/calendar/earnings", params)
        except Exception:
            errors.append(f"earnings_chunk_error_{iso_day(chunk_start)}")
            continue

        for item in data.get("earningsCalendar", []) or []:
            if not isinstance(item, dict):
                continue
            key = (
                item.get("symbol"),
                item.get("date"),
                item.get("hour"),
                item.get("quarter"),
                item.get("year"),
            )
            if key in seen:
                continue
            seen.add(key)
            items.append(item)

    return {"earningsCalendar": items, "errors": errors}


def normalize_earnings(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        date = item.get("date") or ""
        output.append({
            "date": date,
            "symbol": item.get("symbol"),
        })
    output.sort(key=lambda x: (x.get("date") or "", x.get("symbol") or ""))
    return output


def main() -> int:
    load_dotenv()
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        print("FINNHUB_API_KEY is not set.", file=sys.stderr)
        return 1

    parser = argparse.ArgumentParser(description="Fetch Finnhub earnings and economic calendars.")
    parser.add_argument("--from", dest="from_date", help="From date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="To date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=30, help="Range length in days when from/to not set")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output JSON path")
    parser.add_argument("--chunk-days", type=int, default=7, help="Chunk size in days to avoid API truncation")
    args = parser.parse_args()

    from_date, to_date = build_range(args.from_date, args.to_date, args.days)

    earnings_raw = fetch_earnings_range(
        from_date,
        to_date,
        api_key,
        max(1, args.chunk_days),
    )
    errors: List[str] = list(earnings_raw.get("errors", []))
    earnings_list = normalize_earnings(earnings_raw.get("earningsCalendar", []) or [])

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "range": {"from": from_date, "to": to_date, "days": args.days},
        "source": "finnhub",
        "earnings": earnings_list,
    }
    if errors:
        payload["errors"] = errors

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(earnings_list)} earnings events to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
