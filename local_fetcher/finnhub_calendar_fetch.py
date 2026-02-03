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
FRED_BASE_URL = "https://api.stlouisfed.org/fred"
IMPORTANT_FRED_RELEASE_NAMES = [
    "Employment Situation",
    "Consumer Price Index",
    "Producer Price Index",
    "Personal Income and Outlays",
    "Gross Domestic Product",
    "Advance Monthly Sales for Retail and Food Services",
    "G.17 Industrial Production and Capacity Utilization",
    "Job Openings and Labor Turnover Survey",
    "Surveys of Consumers",
]
MAX_DATES_PER_RELEASE = 6


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

def fetch_fred_releases_list(api_key: str) -> Dict[str, Any]:
    url = f"{FRED_BASE_URL}/releases"
    params: Dict[str, Any] = {
        "api_key": api_key,
        "file_type": "json",
        "limit": 1000,
        "offset": 0,
    }
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    while True:
        try:
            data = fetch_json(url, params)
        except Exception:
            errors.append(f"fred_releases_error_offset_{params.get('offset', 0)}")
            break

        batch = data.get("releases") or []
        if isinstance(batch, list):
            for item in batch:
                if isinstance(item, dict):
                    items.append(item)

        count = int(data.get("count") or 0)
        offset = int(data.get("offset") or params.get("offset", 0))
        limit = int(data.get("limit") or params.get("limit", 1000))
        if count == 0 or (offset + limit) >= count:
            break
        params["offset"] = offset + limit

    return {"releases": items, "errors": errors}

def fetch_fred_release_dates_for_release(release_id: int, from_date: str, to_date: str, api_key: str) -> Dict[str, Any]:
    url = f"{FRED_BASE_URL}/release/dates"
    params: Dict[str, Any] = {
        "api_key": api_key,
        "file_type": "json",
        "release_id": release_id,
        "realtime_start": from_date,
        "realtime_end": to_date,
        "include_release_dates_with_no_data": "true",
        "sort_order": "asc",
        "limit": 10000,
        "offset": 0,
    }
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    while True:
        try:
            data = fetch_json(url, params)
        except Exception:
            errors.append(f"fred_release_dates_error_{release_id}_offset_{params.get('offset', 0)}")
            break

        batch = data.get("release_dates") or []
        if isinstance(batch, list):
            for item in batch:
                if isinstance(item, dict):
                    items.append(item)

        count = int(data.get("count") or 0)
        offset = int(data.get("offset") or params.get("offset", 0))
        limit = int(data.get("limit") or params.get("limit", 10000))
        if count == 0 or (offset + limit) >= count:
            break
        params["offset"] = offset + limit

    return {"release_dates": items, "errors": errors}

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

def build_fred_key_releases(
    releases: List[Dict[str, Any]],
    from_date: str,
    to_date: str,
    api_key: str,
    names: List[str],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    key_events: List[Dict[str, Any]] = []
    errors: List[str] = []
    if not releases:
        return key_events, ["fred_releases_missing"]

    name_set = {n.strip().lower() for n in names if n and isinstance(n, str)}
    matched = []
    for rel in releases:
        name = str(rel.get("name") or "")
        if not name:
            continue
        name_lc = name.lower()
        if name_lc in name_set:
            matched.append(rel)

    if not matched:
        return key_events, ["fred_key_release_no_match"]

    seen = set()
    for rel in matched:
        try:
            release_id = int(rel.get("id"))
        except Exception:
            continue
        name = str(rel.get("name") or "")
        raw = fetch_fred_release_dates_for_release(release_id, from_date, to_date, api_key)
        errors.extend(raw.get("errors", []))
        dates: List[str] = []
        for item in raw.get("release_dates", []) or []:
            if not isinstance(item, dict):
                continue
            date = item.get("date") or ""
            if date and (date < from_date or date > to_date):
                continue
            if date:
                dates.append(date)

        if not dates:
            continue

        dates = sorted(set(dates))
        if name.lower() == "fomc press release" and len(dates) > MAX_DATES_PER_RELEASE:
            # FRED returns daily dates for FOMC press release when future data is missing.
            # Keep only the first Wednesday in range as a rough proxy.
            wed = []
            for d in dates:
                try:
                    dt = datetime.datetime.strptime(d, "%Y-%m-%d").date()
                    if dt.weekday() == 2:
                        wed.append(d)
                except Exception:
                    continue
            dates = wed[:1] if wed else dates[:1]
        elif len(dates) > MAX_DATES_PER_RELEASE:
            dates = dates[:MAX_DATES_PER_RELEASE]

        for date in dates:
            key = (date, name.lower())
            if key in seen:
                continue
            seen.add(key)
            key_events.append({
                "date": date,
                "release_name": name,
            })

    key_events.sort(key=lambda x: (x.get("date") or "", x.get("release_name") or ""))
    return key_events, errors

def main() -> int:
    load_dotenv()
    api_key = os.getenv("FINNHUB_API_KEY")
    fred_key = os.getenv("FRED_API_KEY")
    if not api_key and not fred_key:
        print("FINNHUB_API_KEY or FRED_API_KEY is not set.", file=sys.stderr)
        return 1

    parser = argparse.ArgumentParser(description="Fetch Finnhub earnings and economic calendars.")
    parser.add_argument("--from", dest="from_date", help="From date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="To date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=30, help="Range length in days when from/to not set")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output JSON path")
    parser.add_argument("--chunk-days", type=int, default=7, help="Chunk size in days to avoid API truncation")
    args = parser.parse_args()

    from_date, to_date = build_range(args.from_date, args.to_date, args.days)

    errors: List[str] = []
    earnings_list: List[Dict[str, Any]] = []
    fred_key_releases: List[Dict[str, Any]] = []

    if api_key:
        earnings_raw = fetch_earnings_range(
            from_date,
            to_date,
            api_key,
            max(1, args.chunk_days),
        )
        errors.extend(list(earnings_raw.get("errors", [])))
        earnings_list = normalize_earnings(earnings_raw.get("earningsCalendar", []) or [])
    else:
        errors.append("FINNHUB_API_KEY_missing")

    if fred_key:
        releases_raw = fetch_fred_releases_list(fred_key)
        errors.extend(list(releases_raw.get("errors", [])))
        key_list, key_errors = build_fred_key_releases(
            releases_raw.get("releases", []) or [],
            from_date,
            to_date,
            fred_key,
            IMPORTANT_FRED_RELEASE_NAMES,
        )
        errors.extend(key_errors)
        fred_key_releases = key_list
    else:
        errors.append("FRED_API_KEY_missing")

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "range": {"from": from_date, "to": to_date, "days": args.days},
        "source": "finnhub+fred" if api_key and fred_key else ("finnhub" if api_key else "fred"),
        "earnings": earnings_list,
        "fred_key_releases": fred_key_releases,
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
