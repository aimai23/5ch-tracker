import argparse
import bisect
import datetime
import json
import logging
import math
import os
import re
import sys

import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HINDENBURG_HISTORY_FILE = os.path.join(BASE_DIR, "hindenburg_history.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
log_file = os.path.join(LOG_DIR, f"{current_date}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)


def parse_numeric_text(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    if text in {"-", "--", "N/A"}:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def parse_int_text(value):
    num = parse_numeric_text(value)
    if num is None:
        return None
    try:
        return int(round(num))
    except Exception:
        return None


def parse_bool_text(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def parse_history_date(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.date.fromisoformat(text)
    except Exception:
        return None


def compute_hindenburg_threshold(issues_traded=None, advances=None, declines=None):
    adv_num = parse_numeric_text(advances)
    dec_num = parse_numeric_text(declines)
    issues_num = parse_numeric_text(issues_traded)

    base_count = None
    if adv_num is not None and dec_num is not None and (adv_num + dec_num) > 0:
        base_count = int(round(adv_num + dec_num))
    elif issues_num is not None and issues_num > 0:
        base_count = int(round(issues_num))

    if base_count is None or base_count <= 0:
        return None
    return int(math.ceil(base_count * 0.028))


def evaluate_hindenburg_base_signal(highs, lows, issues_traded=None, advances=None, declines=None):
    highs_num = parse_numeric_text(highs)
    lows_num = parse_numeric_text(lows)
    threshold_count = compute_hindenburg_threshold(
        issues_traded=issues_traded,
        advances=advances,
        declines=declines
    )

    cond_highs = threshold_count is not None and highs_num is not None and highs_num >= threshold_count
    cond_lows = threshold_count is not None and lows_num is not None and lows_num >= threshold_count
    cond_ratio = highs_num is not None and lows_num is not None and lows_num > 0 and highs_num <= (lows_num * 2)
    base_signal = bool(cond_highs and cond_lows and cond_ratio)
    return {
        "base_signal": base_signal,
        "threshold_count": threshold_count
    }


def calculate_ema(values, period):
    if not values or period <= 0:
        return None
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema_val = float(values[0])
    for v in values[1:]:
        ema_val = (float(v) - ema_val) * k + ema_val
    return ema_val


def is_hindenburg_lamp_on(state, triggered):
    if isinstance(triggered, bool) and triggered:
        return True
    state_text = str(state or "").upper()
    return ("WATCH" in state_text) or ("TRIGGER" in state_text)


def load_hindenburg_history(limit=500):
    if not os.path.exists(HINDENBURG_HISTORY_FILE):
        return []
    try:
        with open(HINDENBURG_HISTORY_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load hindenburg history: {e}")
        return []

    if not isinstance(raw, list):
        return []

    items = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        date_text = str(item.get("date") or "").strip()
        if parse_history_date(date_text) is None:
            continue
        items.append({
            "date": date_text,
            "advances": parse_numeric_text(item.get("advances")),
            "declines": parse_numeric_text(item.get("declines")),
            "net_advances": parse_numeric_text(item.get("net_advances")),
            "new_highs": parse_numeric_text(item.get("new_highs")),
            "new_lows": parse_numeric_text(item.get("new_lows")),
            "issues_traded": parse_numeric_text(item.get("issues_traded")),
            "trin": parse_numeric_text(item.get("trin")),
            "base_signal": parse_bool_text(item.get("base_signal")),
            "state": str(item.get("state") or "").strip() or None,
            "mode": str(item.get("mode") or "").strip() or None,
            "risk": str(item.get("risk") or "").strip() or None,
            "triggered": parse_bool_text(item.get("triggered")),
            "lamp_on": parse_bool_text(item.get("lamp_on")),
            "derived": bool(item.get("derived"))
        })

    items.sort(key=lambda x: x.get("date", ""))
    if limit and len(items) > limit:
        items = items[-limit:]
    return items


def save_hindenburg_history(history, limit=500):
    normalized = []
    for item in history:
        if not isinstance(item, dict):
            continue
        date_text = str(item.get("date") or "").strip()
        if parse_history_date(date_text) is None:
            continue
        normalized.append({
            "date": date_text,
            "advances": parse_numeric_text(item.get("advances")),
            "declines": parse_numeric_text(item.get("declines")),
            "net_advances": parse_numeric_text(item.get("net_advances")),
            "new_highs": parse_numeric_text(item.get("new_highs")),
            "new_lows": parse_numeric_text(item.get("new_lows")),
            "issues_traded": parse_numeric_text(item.get("issues_traded")),
            "trin": parse_numeric_text(item.get("trin")),
            "base_signal": parse_bool_text(item.get("base_signal")),
            "state": str(item.get("state") or "").strip() or None,
            "mode": str(item.get("mode") or "").strip() or None,
            "risk": str(item.get("risk") or "").strip() or None,
            "triggered": parse_bool_text(item.get("triggered")),
            "lamp_on": parse_bool_text(item.get("lamp_on")),
            "derived": bool(item.get("derived"))
        })

    normalized.sort(key=lambda x: x.get("date", ""))
    if limit and len(normalized) > limit:
        normalized = normalized[-limit:]

    with open(HINDENBURG_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)


def upsert_hindenburg_entry(history, entry):
    if not isinstance(entry, dict):
        return
    date_text = str(entry.get("date") or "").strip()
    if parse_history_date(date_text) is None:
        return

    idx = None
    for i, item in enumerate(history):
        if str(item.get("date") or "") == date_text:
            idx = i
            break

    clean = {
        "date": date_text,
        "advances": parse_numeric_text(entry.get("advances")),
        "declines": parse_numeric_text(entry.get("declines")),
        "net_advances": parse_numeric_text(entry.get("net_advances")),
        "new_highs": parse_numeric_text(entry.get("new_highs")),
        "new_lows": parse_numeric_text(entry.get("new_lows")),
        "issues_traded": parse_numeric_text(entry.get("issues_traded")),
        "trin": parse_numeric_text(entry.get("trin")),
        "base_signal": parse_bool_text(entry.get("base_signal")),
        "state": str(entry.get("state") or "").strip() or None,
        "mode": str(entry.get("mode") or "").strip() or None,
        "risk": str(entry.get("risk") or "").strip() or None,
        "triggered": parse_bool_text(entry.get("triggered")),
        "lamp_on": parse_bool_text(entry.get("lamp_on")),
        "derived": bool(entry.get("derived"))
    }

    if idx is None:
        history.append(clean)
        return

    prev = history[idx]
    prev_derived = bool(prev.get("derived"))
    curr_derived = clean.get("derived", False)
    if prev_derived and not curr_derived:
        history[idx] = clean
        return

    merged = dict(prev)
    for k, v in clean.items():
        if k == "date":
            continue
        if v is not None:
            merged[k] = v
    merged["derived"] = prev_derived and curr_derived
    history[idx] = merged


def bootstrap_barchart_session(seed_symbol="$ADVN"):
    session = requests.Session()
    seed_path = requests.utils.quote(str(seed_symbol), safe="")
    seed_url = f"https://www.barchart.com/stocks/quotes/{seed_path}/price-history/historical"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8"
    }
    resp = session.get(seed_url, headers=headers, timeout=20)
    if resp.status_code != 200:
        return None, None, None
    m = re.search(r'<meta name="csrf-token" content="([^"]+)"', resp.text)
    csrf_token = m.group(1).strip() if m else None
    xsrf_token = session.cookies.get("XSRF-TOKEN")
    return session, csrf_token, xsrf_token


def parse_barchart_history_csv(raw_text):
    rows = []
    for line in str(raw_text or "").splitlines():
        text = str(line or "").strip()
        if not text or text.lower().startswith("symbol,"):
            continue
        parts = [p.strip() for p in text.split(",")]
        if len(parts) < 7:
            continue
        date_text = parts[1]
        if parse_history_date(date_text) is None:
            continue
        rows.append({
            "date": date_text,
            "close": parse_numeric_text(parts[5])
        })
    rows.sort(key=lambda x: x.get("date", ""))
    return rows


def fetch_barchart_history_series(symbol, maxrecords=300, session=None, csrf_token=None, xsrf_token=None):
    local_session = session or requests.Session()
    sym_text = str(symbol).strip()
    quote_path = requests.utils.quote(sym_text, safe="")
    referer_url = f"https://www.barchart.com/stocks/quotes/{quote_path}/price-history/historical"
    api_url = "https://www.barchart.com/proxies/timeseries/queryeod.ashx"
    params = {
        "symbol": sym_text,
        "data": "historical",
        "maxrecords": str(max(10, int(maxrecords or 300))),
        "volume": "contract",
        "order": "asc",
        "dividends": "false",
        "backadjust": "false",
        "daystoexpiration": "1",
        "contractroll": "combined"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer_url,
        "Origin": "https://www.barchart.com",
        "X-Requested-With": "XMLHttpRequest"
    }
    if csrf_token:
        headers["X-CSRF-TOKEN"] = str(csrf_token)
    if xsrf_token:
        headers["X-XSRF-TOKEN"] = str(xsrf_token)

    resp = local_session.get(api_url, params=params, headers=headers, timeout=20)
    if resp.status_code != 200:
        return []
    return parse_barchart_history_csv(resp.text)


def fetch_yahoo_chart_with_dates(symbol="^NYA", range_key="1y", interval="1d"):
    encoded = requests.utils.quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
    params = {
        "range": range_key,
        "interval": interval
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*"
    }
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    if resp.status_code != 200:
        return []
    payload = resp.json()
    chart = payload.get("chart", {})
    results = chart.get("result") or []
    if not results:
        return []

    result0 = results[0] if isinstance(results[0], dict) else {}
    timestamps = result0.get("timestamp") or []
    quote = ((result0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    out = []
    for i, ts in enumerate(timestamps):
        if i >= len(closes):
            continue
        close_val = closes[i]
        if not isinstance(ts, (int, float)) or not isinstance(close_val, (int, float)):
            continue
        day = datetime.datetime.fromtimestamp(int(ts), datetime.timezone.utc).date()
        out.append({"date": day.isoformat(), "close": float(close_val)})
    out.sort(key=lambda x: x.get("date", ""))
    return out


def build_hindenburg_rows(lookback_days=95):
    lookback = max(30, int(lookback_days or 95))
    today = datetime.datetime.now(datetime.timezone.utc).date()
    cutoff = today - datetime.timedelta(days=lookback)

    session, csrf_token, xsrf_token = bootstrap_barchart_session(seed_symbol="$ADVN")
    if session is None:
        raise RuntimeError("barchart_session_failed")

    symbol_map = {
        "advances": "$ADVN",
        "declines": "$DECN",
        "new_highs": "$HIGN",
        "new_lows": "$LOWN",
        "trin": "$TRIN"
    }

    daily_maps = {}
    for key, symbol in symbol_map.items():
        rows = fetch_barchart_history_series(
            symbol=symbol,
            maxrecords=300,
            session=session,
            csrf_token=csrf_token,
            xsrf_token=xsrf_token
        )
        if not rows:
            raise RuntimeError(f"no_data_{key}")
        m = {}
        for row in rows:
            d = parse_history_date(row.get("date"))
            if d is None or d < cutoff:
                continue
            close_val = parse_numeric_text(row.get("close"))
            if close_val is None:
                continue
            m[d.isoformat()] = close_val
        daily_maps[key] = m

    required_keys = ["advances", "declines", "new_highs", "new_lows"]
    date_sets = [set(daily_maps.get(k, {}).keys()) for k in required_keys]
    if not date_sets or any(len(s) == 0 for s in date_sets):
        raise RuntimeError("insufficient_required_series")

    common_dates = sorted(set.intersection(*date_sets))
    if not common_dates:
        raise RuntimeError("no_common_dates")

    nya_series = fetch_yahoo_chart_with_dates("^NYA", range_key="1y", interval="1d")
    nya_dates = [str(x.get("date")) for x in nya_series if x.get("date")]
    nya_closes = [parse_numeric_text(x.get("close")) for x in nya_series if x.get("date")]
    nya_pairs = [(d, c) for d, c in zip(nya_dates, nya_closes) if c is not None]
    nya_dates = [p[0] for p in nya_pairs]
    nya_closes = [float(p[1]) for p in nya_pairs]
    nya_index_by_date = {d: i for i, d in enumerate(nya_dates)}

    rows = []
    for date_text in common_dates:
        adv = parse_int_text(daily_maps["advances"].get(date_text))
        dec = parse_int_text(daily_maps["declines"].get(date_text))
        highs = parse_int_text(daily_maps["new_highs"].get(date_text))
        lows = parse_int_text(daily_maps["new_lows"].get(date_text))
        trin = parse_numeric_text((daily_maps.get("trin") or {}).get(date_text))
        issues_traded = (adv + dec) if (adv is not None and dec is not None) else None
        net_adv = (adv - dec) if (adv is not None and dec is not None) else None

        base_eval = evaluate_hindenburg_base_signal(
            highs=highs,
            lows=lows,
            issues_traded=issues_traded,
            advances=adv,
            declines=dec
        )

        rows.append({
            "date": date_text,
            "advances": adv,
            "declines": dec,
            "net_advances": net_adv,
            "new_highs": highs,
            "new_lows": lows,
            "issues_traded": issues_traded,
            "trin": trin,
            "base_signal": bool(base_eval["base_signal"]),
            "threshold_count": base_eval["threshold_count"]
        })

    rows.sort(key=lambda x: x.get("date", ""))
    if not rows:
        raise RuntimeError("no_rows_after_cutoff")

    for i, row in enumerate(rows):
        net_series = [float(x.get("net_advances")) for x in rows[: i + 1] if x.get("net_advances") is not None]
        ema19 = calculate_ema(net_series, 19)
        ema39 = calculate_ema(net_series, 39)
        mcclellan = (ema19 - ema39) if (ema19 is not None and ema39 is not None) else None
        cond_mcclellan_negative = (mcclellan is not None and mcclellan < 0)

        cluster_rows_30 = rows[max(0, i - 29): i + 1]
        cluster_signal_count_30td = sum(1 for x in cluster_rows_30 if bool(x.get("base_signal")))
        cluster_condition = cluster_signal_count_30td >= 2

        trend_condition = None
        date_text = row["date"]
        nyse_idx = nya_index_by_date.get(date_text)
        if nyse_idx is None and nya_dates:
            insert_idx = bisect.bisect_right(nya_dates, date_text) - 1
            if insert_idx >= 0:
                nyse_idx = insert_idx
        if nyse_idx is not None and nyse_idx >= 50 and nyse_idx < len(nya_closes):
            trend_condition = nya_closes[nyse_idx] > nya_closes[nyse_idx - 50]

        strict_history_ready = (i + 1) >= 40 and len(net_series) >= 40
        strict_ready = strict_history_ready and (mcclellan is not None) and (trend_condition is not None)
        strict_triggered = strict_ready and cluster_condition and cond_mcclellan_negative and bool(trend_condition)

        base_signal = bool(row.get("base_signal"))
        net_advances = parse_numeric_text(row.get("net_advances"))
        trin_val = parse_numeric_text(row.get("trin"))
        lite_breadth_negative = bool(
            (net_advances is not None and net_advances < 0) or
            (trin_val is not None and trin_val > 1.0)
        )
        lite_triggered = base_signal and (trend_condition is not False) and lite_breadth_negative

        if strict_ready:
            mode = "strict"
            triggered = strict_triggered
            if strict_triggered:
                state = "TRIGGERED"
                risk = "high"
            elif base_signal and bool(trend_condition):
                state = "WATCH (STRICT-INITIAL)"
                risk = "mid"
            elif cluster_condition and bool(trend_condition):
                state = "WATCH (STRICT-CLUSTER)"
                risk = "mid"
            else:
                state = "NO SIGNAL"
                risk = "low"
        else:
            mode = "lite"
            triggered = False
            if lite_triggered:
                state = "WATCH (LITE)"
                risk = "mid"
            elif base_signal:
                state = "WATCH (LITE-BASE)"
                risk = "mid"
            else:
                state = "NO SIGNAL"
                risk = "low"

        row["state"] = state
        row["mode"] = mode
        row["risk"] = risk
        row["triggered"] = bool(triggered)
        row["lamp_on"] = is_hindenburg_lamp_on(state, triggered)

    return rows


def run_backfill(lookback_days=95):
    rows = build_hindenburg_rows(lookback_days=lookback_days)
    history = load_hindenburg_history(limit=500)
    for row in rows:
        upsert_hindenburg_entry(history, {
            "date": row["date"],
            "advances": row["advances"],
            "declines": row["declines"],
            "net_advances": row["net_advances"],
            "new_highs": row["new_highs"],
            "new_lows": row["new_lows"],
            "issues_traded": row["issues_traded"],
            "trin": row["trin"],
            "base_signal": row["base_signal"],
            "state": row["state"],
            "mode": row["mode"],
            "risk": row["risk"],
            "triggered": row["triggered"],
            "lamp_on": row["lamp_on"],
            "derived": False
        })

    history.sort(key=lambda x: x.get("date", ""))
    save_hindenburg_history(history, limit=500)

    lit_count = sum(1 for row in rows if bool(row.get("lamp_on")))
    trigger_count = sum(1 for row in rows if bool(row.get("triggered")))
    return {
        "generated_days": len(rows),
        "from_date": rows[0]["date"],
        "to_date": rows[-1]["date"],
        "lit_count": lit_count,
        "trigger_count": trigger_count
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=95, help="Lookback days for one-time backfill (default: 95)")
    args = parser.parse_args()

    try:
        result = run_backfill(lookback_days=args.days)
        logging.info(
            "Hindenburg backfill completed: "
            f"days={result['generated_days']} "
            f"range={result['from_date']}..{result['to_date']} "
            f"lamp_on={result['lit_count']} trigger={result['trigger_count']}"
        )
        return 0
    except Exception as e:
        logging.error(f"Hindenburg backfill failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
