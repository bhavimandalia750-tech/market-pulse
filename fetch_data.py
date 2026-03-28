"""
NSE Data Fetcher - Multi-Strategy IP-Block Bypass
==================================================
NSE blocks GitHub Actions datacenter IPs. This script tries 4 strategies:

  Strategy 1: ScraperAPI proxy  (set SCRAPER_API_KEY secret — free 1000 req/month)
  Strategy 2: Direct HTTP with hardened browser headers + cookie harvest
  Strategy 3: nse Python package (alternate session handling)
  Strategy 4: Yahoo Finance (indices only, as fallback)

HOW TO ADD SCRAPER API KEY (recommended):
  1. Sign up free at https://www.scraperapi.com  (1000 req/month free tier)
  2. Go to your GitHub repo → Settings → Secrets and variables → Actions
  3. New repository secret: Name = SCRAPER_API_KEY, Value = your key
  4. That's it — this script reads it automatically.

Also saves 5-min OHLC candle snapshots to data/candles/ for 7-day backtest.
"""

import json, time, traceback, sys, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

try:
    import requests
except ImportError:
    os.system("pip install requests --quiet")
    import requests

OUT = Path("data")
OUT.mkdir(exist_ok=True)

CANDLE_DIR = OUT / "candles"
CANDLE_DIR.mkdir(exist_ok=True)

# ── GitHub Actions secret (optional but strongly recommended) ──────────────
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

NAV_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Cache-Control": "no-cache",
    "DNT": "1",
}

API_HEADERS = {
    "User-Agent": UA,
    "Accept": "*/*",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

MAX_CANDLE_DAYS = 7

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str, indent=2))
    print(f"  SAVED: {name}")

def save_error(stage, err):
    existing = {}
    try:
        existing = json.loads((OUT / "fetch_errors.json").read_text())
    except Exception:
        pass
    existing[stage] = {"error": str(err), "time": datetime.now(timezone.utc).isoformat()}
    (OUT / "fetch_errors.json").write_text(json.dumps(existing, indent=2))

def gf0(d, *keys):
    for k in keys:
        try:
            v = d.get(k)
            if v is not None and str(v).strip() not in ("", "-", "--", "nan"):
                return float(str(v).replace(",", ""))
        except Exception:
            pass
    return 0.0

def now_ist():
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))

def ist_date_key():
    return now_ist().strftime("%Y-%m-%d")

# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY 1 — ScraperAPI proxy
# ═══════════════════════════════════════════════════════════════════════════

def scraper_get(url, retries=3):
    """Route request through ScraperAPI to bypass NSE IP block."""
    if not SCRAPER_API_KEY:
        return None
    proxy_url = (
        "http://api.scraperapi.com"
        f"?api_key={SCRAPER_API_KEY}"
        f"&url={requests.utils.quote(url, safe='')}"
        "&render=false&country_code=in"
    )
    for attempt in range(retries):
        try:
            r = requests.get(proxy_url, timeout=60)
            label = url.split("/")[-1][:35]
            print(f"  ScraperAPI [{label}]: HTTP {r.status_code}")
            if r.status_code == 200 and r.text.strip():
                try:
                    return r.json()
                except Exception:
                    print("  JSON parse failed")
            elif r.status_code == 429:
                print("  ScraperAPI rate limited — waiting 15s")
                time.sleep(15)
            else:
                time.sleep(5)
        except Exception as e:
            print(f"  ScraperAPI attempt {attempt+1}: {e}")
            time.sleep(5)
    return None

# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY 2 — Direct HTTP with cookie harvesting
# ═══════════════════════════════════════════════════════════════════════════

def make_session():
    s = requests.Session()
    s.headers.update(NAV_HEADERS)
    print("  Harvesting NSE cookies...")
    for attempt in range(3):
        try:
            r = s.get("https://www.nseindia.com/", timeout=25)
            print(f"  Homepage: {r.status_code}, cookies: {len(s.cookies)}")
            time.sleep(3)
            s.headers.update({"Referer": "https://www.nseindia.com/"})
            r2 = s.get("https://www.nseindia.com/market-data/live-equity-market", timeout=25)
            print(f"  Market page: {r2.status_code}")
            time.sleep(2)
            s.headers.update({"Referer": "https://www.nseindia.com/market-data/live-equity-market"})
            r3 = s.get("https://www.nseindia.com/option-chain", timeout=25)
            print(f"  OC page: {r3.status_code}, total cookies: {len(s.cookies)}")
            time.sleep(3)
            if len(s.cookies) >= 2:
                print(f"  Got {len(s.cookies)} cookies — ready")
                s.headers.update(API_HEADERS)
                return s
            print(f"  Attempt {attempt+1}: only {len(s.cookies)} cookies, retrying in 8s")
            time.sleep(8)
        except Exception as e:
            print(f"  Cookie attempt {attempt+1} failed: {e}")
            time.sleep(8)
    print("  Cookie harvest incomplete — trying anyway")
    s.headers.update(API_HEADERS)
    return s

def session_get(session, url, retries=4):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=25)
            label = url.split("/")[-1][:40]
            print(f"  GET [{label}]: {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                if data:
                    return data
                print("  Empty response body")
            elif r.status_code in (401, 403):
                print(f"  {r.status_code} — refreshing cookies")
                session.get("https://www.nseindia.com/option-chain", timeout=20)
                time.sleep(8)
            elif r.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f"  429 rate limit — waiting {wait}s")
                time.sleep(wait)
            else:
                time.sleep(6)
        except Exception as e:
            print(f"  Attempt {attempt+1}: {e}")
            time.sleep(6)
    return None

# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY 3 — nse Python package
# ═══════════════════════════════════════════════════════════════════════════

def try_nse_package():
    print("  [Strategy 3] nse Python package...")
    try:
        from nse import NSE
        with NSE(download_folder=OUT, server=True) as nse:
            raw = nse.listIndices()
            idx_list = raw.get("data", [])
            result = {}
            want = {
                "NIFTY 50": "nifty", "NIFTY BANK": "banknifty",
                "NIFTY FIN SERVICE": "finnifty", "Nifty Fin Service": "finnifty",
                "INDIA VIX": "vix", "India VIX": "vix",
            }
            for idx in idx_list:
                name = idx.get("index", idx.get("indexSymbol", ""))
                key = want.get(name)
                if key and key not in result:
                    result[key] = {
                        "name":    name,
                        "last":    gf0(idx, "last", "lastPrice"),
                        "change":  gf0(idx, "variation", "change"),
                        "pChange": gf0(idx, "percentChange", "pChange"),
                        "open":    gf0(idx, "open"),
                        "high":    gf0(idx, "high", "dayHigh"),
                        "low":     gf0(idx, "low", "dayLow"),
                        "prev":    gf0(idx, "previousClose"),
                    }
            if result:
                result["updatedAt"] = datetime.now(timezone.utc).isoformat()
                save("indices.json", result)
            time.sleep(2)

            oc_ok = 0
            for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                try:
                    raw_oc = nse.optionChain(sym)
                    if _process_and_save_oc(raw_oc, sym):
                        oc_ok += 1
                    time.sleep(3)
                except Exception as e:
                    print(f"  nse pkg OC {sym}: {e}")

            for method_name in ["fiiDII", "fiidii"]:
                method = getattr(nse, method_name, None)
                if not method:
                    continue
                try:
                    fii_raw = method()
                    fii_list = fii_raw if isinstance(fii_raw, list) else fii_raw.get("data", [])
                    if _process_and_save_fii(fii_list):
                        break
                except Exception as e:
                    print(f"  nse pkg FII {method_name}: {e}")

            return bool(result) and oc_ok > 0
    except Exception as e:
        print(f"  nse package failed: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY 4 — Yahoo Finance (indices only)
# ═══════════════════════════════════════════════════════════════════════════

def try_yahoo_indices():
    print("  [Strategy 4] Yahoo Finance fallback...")
    symbols = {
        "^NSEI":     ("nifty",     "NIFTY 50"),
        "^NSEBANK":  ("banknifty", "NIFTY BANK"),
        "^INDIAVIX": ("vix",       "INDIA VIX"),
    }
    result = {}
    for ticker, (key, name) in symbols.items():
        for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
            try:
                url = f"https://{host}/v8/finance/chart/{ticker}"
                r = requests.get(url, headers={
                    "User-Agent": UA,
                    "Accept": "application/json",
                    "Referer": "https://finance.yahoo.com/",
                }, timeout=15)
                if r.status_code == 200:
                    meta = r.json()["chart"]["result"][0]["meta"]
                    price = float(meta.get("regularMarketPrice") or 0)
                    prev  = float(meta.get("previousClose") or 0)
                    if price > 0:
                        result[key] = {
                            "name":    name,
                            "last":    round(price, 2),
                            "prev":    round(prev, 2),
                            "change":  round(price - prev, 2),
                            "pChange": round((price - prev) / prev * 100, 2) if prev else 0,
                            "open":    float(meta.get("regularMarketOpen") or price),
                            "high":    float(meta.get("regularMarketDayHigh") or price),
                            "low":     float(meta.get("regularMarketDayLow") or price),
                        }
                        print(f"  Yahoo {key}: {price}")
                        break
            except Exception as e:
                print(f"  Yahoo {ticker}/{host[:6]}: {e}")
        time.sleep(1)

    if result:
        # Preserve existing gift/other keys
        try:
            existing = json.loads((OUT / "indices.json").read_text())
            for k in ("gift",):
                if k in existing and k not in result:
                    result[k] = existing[k]
        except Exception:
            pass
        result["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save("indices.json", result)
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════
# OC + FII PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

def _process_and_save_oc(data, symbol):
    rec = data.get("records", data)
    expiries = rec.get("expiryDates", [])
    spot = gf0(rec, "underlyingValue")
    all_rows = rec.get("data", [])
    first_exp = expiries[0] if expiries else ""
    print(f"  {symbol}: spot={spot}, expiry={first_exp}, rows={len(all_rows)}")
    if not all_rows:
        save_error(f"oc_{symbol}", f"No rows — rec keys: {list(rec.keys())}")
        return False

    strikes_out, total_ce, total_pe = [], 0.0, 0.0
    for row in all_rows:
        sp = gf0(row, "strikePrice")
        if sp == 0:
            continue
        ce = row.get("CE", {}) or {}
        pe = row.get("PE", {}) or {}
        ce_oi = gf0(ce, "openInterest")
        pe_oi = gf0(pe, "openInterest")
        total_ce += ce_oi
        total_pe += pe_oi
        strikes_out.append({
            "strike":   sp,
            "ceOI":     ce_oi,
            "ceChgOI":  gf0(ce, "changeinOpenInterest"),
            "ceVol":    gf0(ce, "totalTradedVolume"),
            "ceIV":     gf0(ce, "impliedVolatility"),
            "ceLTP":    gf0(ce, "lastPrice"),
            "peOI":     pe_oi,
            "peChgOI":  gf0(pe, "changeinOpenInterest"),
            "peVol":    gf0(pe, "totalTradedVolume"),
            "peIV":     gf0(pe, "impliedVolatility"),
            "peLTP":    gf0(pe, "lastPrice"),
        })
    strikes_out.sort(key=lambda x: x["strike"])

    # Max pain
    max_pain = spot
    try:
        min_loss = float("inf")
        for tgt in strikes_out:
            loss = sum(
                (tgt["strike"] - s["strike"]) * s["ceOI"]
                for s in strikes_out if tgt["strike"] > s["strike"]
            )
            loss += sum(
                (s["strike"] - tgt["strike"]) * s["peOI"]
                for s in strikes_out if tgt["strike"] < s["strike"]
            )
            if loss < min_loss:
                min_loss = loss
                max_pain = tgt["strike"]
    except Exception:
        pass

    pcr = round(total_pe / total_ce, 3) if total_ce else 1.0
    atm = min(strikes_out, key=lambda s: abs(s["strike"] - spot)) if strikes_out else {}
    atm_iv = round((atm.get("ceIV", 0) + atm.get("peIV", 0)) / 2, 2)

    save(f"oc_{symbol.lower()}.json", {
        "symbol":    symbol,
        "spot":      spot,
        "expiry":    first_exp,
        "expiries":  expiries[:8],
        "pcr":       pcr,
        "maxPain":   max_pain,
        "atmIV":     atm_iv,
        "totalCeOI": total_ce,
        "totalPeOI": total_pe,
        "strikes":   strikes_out,
        "isLive":    True,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    })
    return True

def _parse_and_save_indices(data):
    idx_list = data.get("data", data if isinstance(data, list) else [])
    want = {
        "NIFTY 50": "nifty", "NIFTY BANK": "banknifty",
        "NIFTY FIN SERVICE": "finnifty", "Nifty Fin Service": "finnifty",
        "INDIA VIX": "vix", "India VIX": "vix",
        "NIFTY MIDCAP SELECT": "midcap", "NIFTY MIDCAP 100": "midcap",
    }
    result = {}
    for idx in idx_list:
        name = idx.get("index", idx.get("indexSymbol", ""))
        key = want.get(name)
        if key and key not in result:
            result[key] = {
                "name":    name,
                "last":    gf0(idx, "last", "lastPrice", "indexValue"),
                "change":  gf0(idx, "variation", "change", "pointChange"),
                "pChange": gf0(idx, "percentChange", "pChange"),
                "open":    gf0(idx, "open", "openValue"),
                "high":    gf0(idx, "high", "dayHigh"),
                "low":     gf0(idx, "low", "dayLow"),
                "prev":    gf0(idx, "previousClose", "previousDay"),
            }
    if not result:
        print(f"  No matching indices in {len(idx_list)} items")
        return False
    try:
        existing = json.loads((OUT / "indices.json").read_text())
        if "gift" in existing:
            result["gift"] = existing["gift"]
    except Exception:
        pass
    result["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save("indices.json", result)
    return True

# ── FII/DII 5-DAY HISTORY FILE ───────────────────────────────────────────
FII_HISTORY_FILE    = OUT / "fii_history.json"
MAX_FII_HISTORY_DAYS = 30   # keep 30 trading days (~6 weeks)


def load_fii_history() -> dict:
    if FII_HISTORY_FILE.exists():
        try:
            return json.loads(FII_HISTORY_FILE.read_text())
        except Exception:
            pass
    return {"days": {}, "updatedAt": None}


def save_fii_history(history: dict) -> None:
    history["updatedAt"] = datetime.now(timezone.utc).isoformat()
    FII_HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))


def _normalise_date(date_raw: str) -> str:
    """Convert any NSE date string to YYYY-MM-DD. Returns '' on failure."""
    s = str(date_raw or "").strip()
    if not s:
        return ""
    # Already YYYY-MM-DD
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # DD-Mon-YYYY  e.g. "27-Mar-2026"
    from datetime import datetime as _dt
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return _dt.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def merge_fii_into_history(rows: list) -> None:
    """Merge freshly fetched FII rows into fii_history.json."""
    history = load_fii_history()
    days = history.setdefault("days", {})

    for r in rows:
        date_key = _normalise_date(r.get("date", ""))
        if not date_key:
            continue
        # Always update (overwrite) so DII values improve if they were 0 before
        days[date_key] = {
            "date":    date_key,
            "fiiBuy":  r.get("fiiBuy",  0),
            "fiiSell": r.get("fiiSell", 0),
            "fiiNet":  r.get("fiiNet",  0),
            "diiBuy":  r.get("diiBuy",  0),
            "diiSell": r.get("diiSell", 0),
            "diiNet":  r.get("diiNet",  0),
        }

    # Prune old entries beyond MAX_FII_HISTORY_DAYS
    cutoff = (datetime.now(timezone.utc) - timedelta(days=MAX_FII_HISTORY_DAYS)).strftime("%Y-%m-%d")
    history["days"] = {k: v for k, v in days.items() if k >= cutoff}

    # Write back sorted newest-first list as well (for easy JS consumption)
    sorted_rows = sorted(history["days"].values(), key=lambda x: x["date"], reverse=True)
    history["sorted"] = sorted_rows
    save_fii_history(history)
    print(f"  FII history: {len(history['days'])} trading days stored")


def build_fii_predictor_data(history: dict) -> dict:
    """
    Compute FII/DII-based market predictor signals from 5-day history.
    Returns a dict that is embedded in fii_history.json under 'predictor'.
    """
    rows = sorted(history.get("days", {}).values(), key=lambda x: x["date"], reverse=True)
    if len(rows) < 2:
        return {"signal": "INSUFFICIENT_DATA", "confidence": 0, "factors": []}

    recent5  = rows[:5]
    recent10 = rows[:10]

    fii5  = sum(r.get("fiiNet", 0) for r in recent5)
    fii10 = sum(r.get("fiiNet", 0) for r in recent10)
    dii5  = sum(r.get("diiNet", 0) for r in recent5)

    # Streak: consecutive buying or selling days
    fii_streak = 0
    for r in recent5:
        if r.get("fiiNet", 0) > 0:
            if fii_streak >= 0: fii_streak += 1
            else: break
        else:
            if fii_streak <= 0: fii_streak -= 1
            else: break

    # Acceleration: is recent 3-day flow stronger than prior 2?
    fii3 = sum(r.get("fiiNet", 0) for r in recent5[:3])
    fii2 = sum(r.get("fiiNet", 0) for r in recent5[3:5]) if len(recent5) >= 5 else 0
    accelerating = (fii3 > 0 and fii3 > abs(fii2)) or (fii3 < 0 and fii3 < -abs(fii2))

    # DII counter: are DIIs absorbing or amplifying FII?
    dii_counter = (fii5 > 0 and dii5 < -fii5 * 0.3)  # DII selling into FII buying
    dii_support = (fii5 < 0 and dii5 > abs(fii5) * 0.4)  # DII buying while FII sells

    score = 0
    factors = []

    if fii5 > 8000:   score += 3; factors.append(f"FII 5d net +₹{int(fii5)}Cr (strong buy)")
    elif fii5 > 3000: score += 2; factors.append(f"FII 5d net +₹{int(fii5)}Cr (moderate buy)")
    elif fii5 > 500:  score += 1; factors.append(f"FII 5d mild buying")
    elif fii5 < -8000:  score -= 3; factors.append(f"FII 5d net -₹{abs(int(fii5))}Cr (heavy sell)")
    elif fii5 < -3000:  score -= 2; factors.append(f"FII 5d net -₹{abs(int(fii5))}Cr (selling)")
    elif fii5 < -500:   score -= 1; factors.append(f"FII 5d mild selling")

    if fii10 > 15000: score += 1; factors.append("10d cumulative FII bullish")
    elif fii10 < -15000: score -= 1; factors.append("10d cumulative FII bearish")

    if fii_streak >= 3:  score += 1; factors.append(f"FII buying {fii_streak} consecutive days")
    elif fii_streak <= -3: score -= 1; factors.append(f"FII selling {abs(fii_streak)} consecutive days")

    if accelerating and fii5 > 0:  score += 1; factors.append("FII flow accelerating (bullish momentum)")
    elif accelerating and fii5 < 0: score -= 1; factors.append("FII selling accelerating (bearish momentum)")

    if dii_support: score += 1; factors.append("DII absorbing FII selling (floor forming)")
    if dii_counter: score -= 1; factors.append("DII distributing into FII buying (caution)")

    score = max(-5, min(5, score))
    confidence = min(95, abs(score) * 18 + 10)

    if score >= 3:    signal, color = "STRONGLY BULLISH", "bull"
    elif score >= 1:  signal, color = "BULLISH BIAS",     "bull"
    elif score <= -3: signal, color = "STRONGLY BEARISH", "bear"
    elif score <= -1: signal, color = "BEARISH BIAS",     "bear"
    else:             signal, color = "NEUTRAL",          "neutral"

    return {
        "signal":       signal,
        "color":        color,
        "score":        score,
        "confidence":   confidence,
        "fii5":         round(fii5, 2),
        "fii10":        round(fii10, 2),
        "dii5":         round(dii5, 2),
        "fiiStreak":    fii_streak,
        "accelerating": accelerating,
        "diiSupport":   dii_support,
        "diiCounter":   dii_counter,
        "factors":      factors,
        "computedAt":   datetime.now(timezone.utc).isoformat(),
    }


def _parse_fii_list(fii_list: list) -> list:
    """
    NSE /api/fiidiiTradeReact sends rows in TWO formats — handle both:

    FORMAT A (category-based, current NSE default):
      Each date appears TWICE — once with category="FII/FPI", once with category="DII".
      Both rows carry "buyValue", "sellValue", "netValue" for their own category.
      Keys used: buyValue / sellValue / netValue  +  category / date

    FORMAT B (combined, older NSE / nse-package output):
      Each date appears ONCE with separate fii_* and dii_* prefixed fields.
      Keys used: fii_buy_value, fii_sell_value, fii_net_value,
                 dii_buy_value, dii_sell_value, dii_net_value  +  date

    Returns a list of merged dicts, one per trading date, with both FII and DII values.
    """
    if not fii_list:
        return []

    def get_date(row):
        return str(row.get("date") or row.get("Date") or row.get("tradeDate") or "").strip()

    # Detect format by checking if any row has a "category" field
    has_category = any("category" in row for row in fii_list[:4])

    if has_category:
        # ── FORMAT A: category-based rows ────────────────────────────────
        # Group by normalised date, pair FII row with DII row
        by_date: dict = {}
        for row in fii_list:
            date_raw = get_date(row)
            date = _normalise_date(date_raw) or date_raw[:10]
            if not date:
                continue
            cat = str(row.get("category", "")).upper().strip()
            val_buy  = gf0(row, "buyValue",  "buy_value",  "BUY_VALUE",  "buy")
            val_sell = gf0(row, "sellValue", "sell_value", "SELL_VALUE", "sell")
            val_net  = gf0(row, "netValue",  "net_value",  "NET_VALUE",  "net")
            if date not in by_date:
                by_date[date] = {"date": date,
                                 "fiiBuy": 0.0, "fiiSell": 0.0, "fiiNet": 0.0,
                                 "diiBuy": 0.0, "diiSell": 0.0, "diiNet": 0.0}
            if "FII" in cat or "FPI" in cat:
                by_date[date]["fiiBuy"]  = val_buy
                by_date[date]["fiiSell"] = val_sell
                by_date[date]["fiiNet"]  = val_net
            elif "DII" in cat:
                by_date[date]["diiBuy"]  = val_buy
                by_date[date]["diiSell"] = val_sell
                by_date[date]["diiNet"]  = val_net
        # Return sorted newest-first, only rows that have FII data
        return [v for v in sorted(by_date.values(), key=lambda x: x["date"], reverse=True)
                if v["fiiBuy"] or v["fiiSell"]]

    else:
        # ── FORMAT B: combined rows (older API / nse-package) ─────────────
        rows = []
        for row in fii_list:
            date_raw = get_date(row)
            date = _normalise_date(date_raw) or date_raw[:10]
            if not date:
                continue
            r = {
                "date":    date,
                "fiiBuy":  gf0(row, "fii_buy_value",  "FII_BUY_VAL",  "fiiBuyVal",  "buyValue"),
                "fiiSell": gf0(row, "fii_sell_value", "FII_SELL_VAL", "fiiSellVal", "sellValue"),
                "fiiNet":  gf0(row, "fii_net_value",  "FII_NET_VAL",  "fiiNetVal",  "netValue"),
                "diiBuy":  gf0(row, "dii_buy_value",  "DII_BUY_VAL",  "diiBuyVal"),
                "diiSell": gf0(row, "dii_sell_value", "DII_SELL_VAL", "diiSellVal"),
                "diiNet":  gf0(row, "dii_net_value",  "DII_NET_VAL",  "diiNetVal"),
            }
            if r["fiiBuy"] or r["fiiSell"]:
                rows.append(r)
        return rows


def _repair_fii_history() -> None:
    """
    One-time migration: fix truncated dates and missing DII values in fii_history.json
    left by the old parser. Runs automatically on every fetch — no-ops when already clean.
    """
    history = load_fii_history()
    days = history.get("days", {})
    if not days:
        return
    repaired = {}
    changed = False
    for key, val in days.items():
        good_key = _normalise_date(key)
        if not good_key:
            good_key = key          # leave as-is if we can't parse
        if good_key != key:
            changed = True          # date key was truncated/wrong
        val["date"] = good_key
        repaired[good_key] = val
    if changed:
        history["days"] = repaired
        history["sorted"] = sorted(repaired.values(), key=lambda x: x["date"], reverse=True)
        save_fii_history(history)
        print(f"  FII history: repaired {len(repaired)} date keys")


def _process_and_save_fii(fii_list):
    # ── Repair any stale history data from the old parser first ──────────
    _repair_fii_history()

    rows = _parse_fii_list(fii_list)
    if rows:
        save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
        print(f"  FII/DII: {len(rows)} days — sample: FII={rows[0]['fiiNet']:+.0f}Cr DII={rows[0]['diiNet']:+.0f}Cr ({rows[0]['date']})")
        # ── Persist into rolling 30-day history file ─────────────────────
        merge_fii_into_history(rows)
        # ── Build predictor snapshot ──────────────────────────────────────
        history = load_fii_history()
        history["predictor"] = build_fii_predictor_data(history)
        save_fii_history(history)
        print(f"  FII predictor: {history['predictor']['signal']} (score {history['predictor']['score']}, {history['predictor']['confidence']}% confidence)")
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════
# GIFT NIFTY
# ═══════════════════════════════════════════════════════════════════════════

def fetch_gift_nifty():
    print("\n--- GIFT NIFTY ---")
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        try:
            url = f"https://{host}/v8/finance/chart/%5EGIFTNIFTY"
            r = requests.get(url, headers={
                "User-Agent": UA, "Accept": "application/json",
                "Referer": "https://finance.yahoo.com/",
            }, timeout=15)
            if r.status_code == 200:
                meta = r.json()["chart"]["result"][0]["meta"]
                price = float(meta.get("regularMarketPrice") or 0)
                prev  = float(meta.get("previousClose") or 0)
                if price > 0:
                    gift = {
                        "name":    "GIFT NIFTY",
                        "last":    round(price, 2),
                        "prev":    round(prev, 2),
                        "change":  round(price - prev, 2),
                        "pChange": round((price - prev) / prev * 100, 2) if prev else 0,
                        "high":    float(meta.get("regularMarketDayHigh") or price),
                        "low":     float(meta.get("regularMarketDayLow") or price),
                        "source":  f"yahoo_{host[:6]}",
                    }
                    _inject_gift(gift)
                    print(f"  GIFT Nifty: {price} ({gift['pChange']:+.2f}%)")
                    return True
        except Exception as e:
            print(f"  Yahoo {host[:6]}: {e}")
    print("  GIFT Nifty: all sources failed")
    return False

def _inject_gift(gift_data):
    idx_path = OUT / "indices.json"
    try:
        existing = json.loads(idx_path.read_text()) if idx_path.exists() else {}
        existing["gift"] = gift_data
        existing["updatedAt"] = datetime.now(timezone.utc).isoformat()
        idx_path.write_text(json.dumps(existing, default=str, indent=2))
    except Exception as e:
        print(f"  Could not inject GIFT: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# 5-MIN CANDLE HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def candle_path(sym, date_key):
    return CANDLE_DIR / f"{sym}_{date_key}.json"

def load_candles(sym, date_key):
    p = candle_path(sym, date_key)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return []
    return []

def save_candles_file(sym, date_key, candles):
    candle_path(sym, date_key).write_text(json.dumps(candles, indent=2))

def record_candle(sym):
    """Read latest OC + indices data and save a 5-min candle."""
    try:
        oc_p = OUT / f"oc_{sym.lower()}.json"
        idx_p = OUT / "indices.json"
        if not oc_p.exists() or not idx_p.exists():
            print(f"  No data files for {sym} candle")
            return

        oc_d  = json.loads(oc_p.read_text())
        idx_d = json.loads(idx_p.read_text())
        idx_key = "nifty" if sym == "NIFTY" else "banknifty"
        idx_entry = idx_d.get(idx_key, {})

        spot  = oc_d.get("spot", 0)
        high  = idx_entry.get("high", spot)
        low   = idx_entry.get("low", spot)
        open_ = idx_entry.get("open", spot)

        if spot <= 0:
            print(f"  Candle skipped: spot={spot}")
            return

        ist = now_ist()
        date_key = ist.strftime("%Y-%m-%d")
        time_str = ist.strftime("%H:%M")

        candles = load_candles(sym, date_key)

        # Update or append
        if candles and candles[-1]["t"] == time_str:
            c = candles[-1]
            c["c"] = round(spot, 2)
            c["h"] = round(max(c["h"], high), 2)
            c["l"] = round(min(c["l"], low), 2)
        else:
            candles.append({
                "t": time_str,
                "o": round(open_, 2),
                "h": round(high, 2),
                "l": round(low, 2),
                "c": round(spot, 2),
            })

        save_candles_file(sym, date_key, candles)
        print(f"  Candle: {sym} {date_key} {time_str} O={open_} H={high} L={low} C={spot}  total={len(candles)}")

    except Exception as e:
        print(f"  Candle error for {sym}: {e}")

def prune_old_candles():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=MAX_CANDLE_DAYS)).strftime("%Y-%m-%d")
    removed = 0
    for f in CANDLE_DIR.glob("*_*.json"):
        if f.name == "index.json":
            continue
        try:
            date_part = f.stem.split("_", 1)[1]
            if date_part < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    if removed:
        print(f"  Pruned {removed} candle files older than {MAX_CANDLE_DAYS} days")

def build_candle_index():
    """Write data/candles/index.json for the dashboard to discover available files."""
    index = {}
    for f in sorted(CANDLE_DIR.glob("*_*.json")):
        if f.name == "index.json":
            continue
        try:
            sym, date_part = f.stem.split("_", 1)
            if sym not in index:
                index[sym] = []
            index[sym].append(date_part)
        except Exception:
            pass
    for sym in index:
        index[sym] = sorted(set(index[sym]))[-MAX_CANDLE_DAYS:]
    (CANDLE_DIR / "index.json").write_text(json.dumps({
        "symbols": index,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }, indent=2))
    print(f"  Candle index: {index}")

# ── TRADE HISTORY ──────────────────────────────────────────────────────────
TRADE_HISTORY_FILE    = OUT / "trade_history.json"
MAX_TRADE_HISTORY_DAYS = 7


def load_trade_history() -> dict:
    """Load existing trade_history.json, or return an empty structure."""
    if TRADE_HISTORY_FILE.exists():
        try:
            return json.loads(TRADE_HISTORY_FILE.read_text())
        except Exception:
            pass
    return {"days": {}, "updatedAt": None}


def prune_trade_history(history: dict) -> dict:
    """Remove day-buckets older than MAX_TRADE_HISTORY_DAYS."""
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=MAX_TRADE_HISTORY_DAYS)
    ).strftime("%Y-%m-%d")
    history["days"] = {
        k: v for k, v in history.get("days", {}).items() if k >= cutoff
    }
    return history


def record_trade(sym: str, direction: str, opt_type: str, strike: float,
                 premium: float | None, sl: float | None,
                 target: float | None, score: int = 0) -> None:
    """
    Append one VEB trade signal to data/trade_history.json.
    Idempotent per (sym, direction, strike, opt_type) per trading day.
    """
    ist_now  = now_ist()
    date_key = ist_now.strftime("%Y-%m-%d")
    time_str = ist_now.strftime("%H:%M")

    history    = prune_trade_history(load_trade_history())
    day_trades = history["days"].setdefault(date_key, [])

    sig_id = f"{sym}-{direction}-{strike}-{opt_type}"
    if any(t.get("id") == sig_id for t in day_trades):
        print(f"  Trade history: duplicate skipped ({sig_id})")
        return

    lot_size = 15 if sym == "BANKNIFTY" else 40 if sym == "FINNIFTY" else 50
    pnl_est  = round((target - premium) * lot_size, 2) if (target and premium) else None

    day_trades.append({
        "id":        sig_id,
        "date":      date_key,
        "time":      time_str,
        "sym":       sym,
        "direction": direction,
        "type":      opt_type,
        "strike":    strike,
        "premium":   premium,
        "sl":        sl,
        "target":    target,
        "pnlEst":    pnl_est,
        "lotSize":   lot_size,
        "score":     score,
        "status":    "ACTIVE",
    })

    history["updatedAt"] = datetime.now(timezone.utc).isoformat()
    TRADE_HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))
    print(f"  Trade history: saved {sym} {direction} {opt_type} {strike} @ {time_str}")


def get_trade_history_summary() -> dict:
    """Lightweight summary for embedding in fetch_status.json."""
    history = load_trade_history()
    total   = sum(len(v) for v in history.get("days", {}).values())
    return {
        "totalTrades": total,
        "days":        sorted(history.get("days", {}).keys()),
        "updatedAt":   history.get("updatedAt"),
    }


# ── FII/DII MULTI-ENDPOINT FETCHER ───────────────────────────────────────────
def _fetch_fii_via_session(session) -> bool:
    """
    Try all known NSE FII/DII endpoints in order.
    Returns True if any endpoint returned parseable FII data.
    """
    endpoints = [
        "https://www.nseindia.com/api/fiidiiTradeReact",   # category-based (current default)
        "https://www.nseindia.com/api/fii-statistics",     # alternate endpoint
        "https://www.nseindia.com/api/fiiStatistics",      # alternate spelling
    ]
    for url in endpoints:
        try:
            d = session_get(session, url)
            if not d:
                continue
            fii_list = d if isinstance(d, list) else d.get("data", [])
            if not fii_list:
                continue
            if _process_and_save_fii(fii_list):
                has_dii = True  # _process_and_save_fii already logged the sample
                return True
        except Exception as e:
            print(f"  FII endpoint {url.split('/')[-1]} failed: {e}")
    return False


def _fetch_fii_via_scraper(api_key: str) -> bool:
    """ScraperAPI path — try same endpoints in order."""
    endpoints = [
        "https://www.nseindia.com/api/fiidiiTradeReact",
        "https://www.nseindia.com/api/fii-statistics",
    ]
    for url in endpoints:
        try:
            d = scraper_get(url)
            if not d:
                continue
            fii_list = d if isinstance(d, list) else d.get("data", [])
            if fii_list:
                if _process_and_save_fii(fii_list):
                    return True
        except Exception as e:
            print(f"  ScraperAPI FII {url.split('/')[-1]} failed: {e}")
    return False


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    ist = now_ist()
    ist_mins = ist.hour * 60 + ist.minute
    is_market_hours = 0 <= ist.weekday() <= 4 and 555 <= ist_mins < 930  # Mon–Fri 9:15–15:30

    print(f"\n{'='*60}")
    print(f"NSE FETCH  {ist.strftime('%Y-%m-%d %H:%M IST')}  Market: {'OPEN' if is_market_hours else 'CLOSED'}")
    if SCRAPER_API_KEY:
        masked = "*" * (len(SCRAPER_API_KEY) - 4) + SCRAPER_API_KEY[-4:]
        print(f"ScraperAPI key: {masked}")
    else:
        print("ScraperAPI key: NOT SET")
        print("  -> Add SCRAPER_API_KEY secret for reliable fetching")
        print("  -> Free: https://www.scraperapi.com (1000 req/month)")
    print("=" * 60)

    ok = {"indices": False, "oc": False, "fii": False, "gift": False, "signals": False}

    # ── Strategy 1: ScraperAPI ───────────────────────────────────────────
    if SCRAPER_API_KEY:
        print("\n[Strategy 1] ScraperAPI proxy...")
        try:
            idx_data = scraper_get("https://www.nseindia.com/api/allIndices")
            if idx_data:
                ok["indices"] = _parse_and_save_indices(idx_data)

            time.sleep(2)
            oc_ok = 0
            for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                oc_data = scraper_get(
                    f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
                )
                if oc_data and _process_and_save_oc(oc_data, sym):
                    oc_ok += 1
                time.sleep(2)
            ok["oc"] = oc_ok > 0

            ok["fii"] = _fetch_fii_via_scraper(SCRAPER_API_KEY)

        except Exception as e:
            print(f"  ScraperAPI strategy failed: {e}")

    # ── Strategy 2: Direct HTTP ──────────────────────────────────────────
    if not ok["indices"] or not ok["oc"]:
        print("\n[Strategy 2] Direct HTTP with cookie harvest...")
        session = make_session()

        if not ok["indices"]:
            d = session_get(session, "https://www.nseindia.com/api/allIndices")
            if d:
                ok["indices"] = _parse_and_save_indices(d)
            time.sleep(2)

        if not ok["oc"]:
            oc_ok = 0
            for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
                url = f"https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
                d = session_get(session, url)
                if d and _process_and_save_oc(d, sym):
                    oc_ok += 1
                time.sleep(4)
            ok["oc"] = oc_ok > 0

        if not ok["fii"]:
            ok["fii"] = _fetch_fii_via_session(session)

    # ── Strategy 3: nse package ──────────────────────────────────────────
    if not ok["indices"] or not ok["oc"]:
        print("\n[Strategy 3] nse Python package...")
        if try_nse_package():
            ok["indices"] = True
            ok["oc"] = True
            ok["fii"] = True

    # ── Strategy 4: Yahoo Finance (indices only) ─────────────────────────
    if not ok["indices"]:
        print("\n[Strategy 4] Yahoo Finance fallback...")
        ok["indices"] = try_yahoo_indices()

    # ── GIFT Nifty (Yahoo — not blocked) ────────────────────────────────
    ok["gift"] = fetch_gift_nifty()

    # ── 5-min candle snapshot ────────────────────────────────────────────
    print("\n--- 5-MIN CANDLE SNAPSHOT ---")
    if is_market_hours and ok["oc"]:
        record_candle("NIFTY")
        record_candle("BANKNIFTY")
    else:
        reason = "market closed" if not is_market_hours else "no OC data"
        print(f"  Skipping ({reason})")

    prune_old_candles()
    build_candle_index()

    # ── Signal Engine ────────────────────────────────────────────────────
    if ok["oc"]:
        print(f"\n{'='*60}\nSIGNAL ENGINE...")
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "signal_engine", Path(__file__).parent / "signal_engine.py"
            )
            if spec:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                ok["signals"] = mod.run()
                print(f"Signal engine: {'OK' if ok['signals'] else 'FAILED'}")

                # ── Record VEB trade signals to 7-day trade_history.json ──
                if ok["signals"]:
                    try:
                        sig_data = json.loads((OUT / "signals.json").read_text())
                        for sym in ("NIFTY", "BANKNIFTY"):
                            veb = sig_data.get("veb", {}).get(sym, {})
                            direction = veb.get("direction")         # "BULLISH" / "BEARISH"
                            opt = veb.get("optionSelection", {})
                            strike  = opt.get("strike")
                            opt_type = opt.get("type")               # "CE" / "PE"
                            premium = opt.get("premium")
                            risk_obj = veb.get("risk", {})
                            sl      = risk_obj.get("sl")
                            target  = risk_obj.get("target")
                            score   = veb.get("score", 0)
                            if direction and strike and opt_type and is_market_hours:
                                record_trade(sym, direction, opt_type, strike,
                                             premium, sl, target, score)
                    except Exception as te:
                        print(f"  Trade history write error: {te}")
        except Exception as e:
            print(f"Signal engine error: {e}")
            traceback.print_exc()
    else:
        print("\nSkipping signal engine — no OC data")

    # ── Save status ──────────────────────────────────────────────────────
    save("fetch_status.json", {
        "lastRun":      datetime.now(timezone.utc).isoformat(),
        "istTime":      ist.strftime("%Y-%m-%d %H:%M IST"),
        "marketOpen":   is_market_hours,
        "success":      ok,
        "allOk":        all(v for k, v in ok.items() if k not in ("signals", "gift")),
        "tradeHistory": get_trade_history_summary(),
    })

    print(f"\n{'='*60}")
    print(
        f"RESULT: indices={ok['indices']} gift={ok['gift']} "
        f"oc={ok['oc']} fii={ok['fii']} signals={ok['signals']}"
    )
    if not ok["indices"] and not ok["oc"]:
        print("\nWARNING: All strategies failed.")
        print("FIX: Add SCRAPER_API_KEY to GitHub Secrets")
        print("  1. Sign up free: https://www.scraperapi.com")
        print("  2. Repo → Settings → Secrets → Actions → New secret")
        print("  3. Name: SCRAPER_API_KEY   Value: <your key>")


if __name__ == "__main__":
    main()
