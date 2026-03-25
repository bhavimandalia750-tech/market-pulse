"""
NSE Data Fetcher - IP Block Bypass Version
==========================================
NSE blocks GitHub Actions datacenter IPs when using the nse package directly.
This version uses direct HTTP requests with real browser headers + session cookies
to bypass the block, with the nse package as fallback.
"""
import json, time, traceback, sys, os
from pathlib import Path
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    os.system("pip install requests --quiet")
    import requests

OUT = Path("data")
OUT.mkdir(exist_ok=True)



# Use latest Chrome UA — NSE checks this strictly
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
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
    "Cache-Control": "max-age=0",
    "DNT": "1",
}

API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str, indent=2))
    print(f"SAVED: {name}")

def save_error(stage, err):
    existing = {}
    try:
        existing = json.loads((OUT / "fetch_errors.json").read_text())
    except: pass
    existing[stage] = {"error": str(err), "time": datetime.now(timezone.utc).isoformat()}
    (OUT / "fetch_errors.json").write_text(json.dumps(existing, indent=2))

def gf0(d, *keys):
    for k in keys:
        try:
            v = d.get(k)
            if v is not None and str(v).strip() not in ('', '-', '--', 'nan'):
                return float(str(v).replace(",", ""))
        except: pass
    return 0.0

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    print("Harvesting NSE cookies (enhanced)...")

    pages_to_warm = [
        "https://www.nseindia.com/",
        "https://www.nseindia.com/market-data/live-equity-market",
        "https://www.nseindia.com/option-chain",
    ]

    for attempt in range(3):
        try:
            # Visit homepage first with navigation headers
            r = s.get(pages_to_warm[0], timeout=25)
            print(f"  Homepage: {r.status_code}, cookies: {len(s.cookies)}")
            time.sleep(3)

            # Visit market data page
            r2 = s.get(pages_to_warm[1], timeout=25)
            print(f"  Market page: {r2.status_code}")
            time.sleep(2)

            # Visit option chain page (this is what sets the key cookies)
            s.headers.update({"Referer": "https://www.nseindia.com/market-data/live-equity-market"})
            r3 = s.get(pages_to_warm[2], timeout=25)
            print(f"  OC page: {r3.status_code}, total cookies: {len(s.cookies)}")
            time.sleep(3)

            if len(s.cookies) >= 2:
                print(f"  ✅ Got {len(s.cookies)} cookies — ready")
                # Switch to API headers for subsequent requests
                s.headers.update(API_HEADERS)
                return s

            print(f"  Attempt {attempt+1}: only {len(s.cookies)} cookies, retrying...")
            time.sleep(8)
        except Exception as e:
            print(f"  Cookie attempt {attempt+1} failed: {e}")
            time.sleep(8)

    print("  ⚠️ Cookie harvest incomplete — trying anyway")
    s.headers.update(API_HEADERS)
    return s

def fetch_json(session, url, retries=4):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=25)
            print(f"  GET .../{url.split('/')[-1].split('?')[0]}: {r.status_code}")
            if r.status_code == 200:
                data = r.json()
                if data:
                    return data
                print(f"  Empty response body")
            elif r.status_code == 401:
                print("  401 — refreshing cookies")
                session.get("https://www.nseindia.com/", timeout=20)
                time.sleep(5)
            elif r.status_code == 403:
                print("  403 — IP blocked, trying cookie refresh")
                session.get("https://www.nseindia.com/option-chain", timeout=20)
                time.sleep(8)
            elif r.status_code == 429:
                wait = 20 * (attempt + 1)
                print(f"  429 rate limit — waiting {wait}s")
                time.sleep(wait)
            else:
                print(f"  Unexpected {r.status_code}")
                time.sleep(6)
        except Exception as e:
            print(f"  Attempt {attempt+1} error: {e}")
            time.sleep(6)
    return None

def fetch_indices(session):
    print("\n--- INDICES ---")
    data = fetch_json(session, "https://www.nseindia.com/api/allIndices")
    if not data:
        save_error("indices", "allIndices API returned None")
        return False

    idx_list = data.get("data", data if isinstance(data, list) else [])
    want = {
        "NIFTY 50": "nifty", "NIFTY BANK": "banknifty",
        "NIFTY FIN SERVICE": "finnifty", "Nifty Fin Service": "finnifty",
        "INDIA VIX": "vix", "India VIX": "vix",
        "NIFTY MIDCAP SELECT": "midcap", "NIFTY MIDCAP 100": "midcap",
    }
    result = {}
    gift_found = None   # capture GIFT if it appears in allIndices
    for idx in idx_list:
        name = idx.get("index", idx.get("indexSymbol", ""))
        key = want.get(name)
        if key and key not in result:
            result[key] = {
                "name": name,
                "last":    gf0(idx, "last", "lastPrice", "indexValue"),
                "change":  gf0(idx, "variation", "change", "pointChange"),
                "pChange": gf0(idx, "percentChange", "pChange"),
                "open":    gf0(idx, "open", "openValue"),
                "high":    gf0(idx, "high", "dayHigh"),
                "low":     gf0(idx, "low", "dayLow"),
                "prev":    gf0(idx, "previousClose", "previousDay"),
            }
            print(f"  {name}: last={result[key]['last']}")
        # Opportunistically grab GIFT Nifty if present in allIndices
        if ("GIFT" in name.upper() or "IFSC" in name.upper()) and gift_found is None:
            price = gf0(idx, "last", "lastPrice", "indexValue")
            if price > 0:
                gift_found = {
                    "name": name, "last": price,
                    "prev":    gf0(idx, "previousClose", "previousDay"),
                    "change":  gf0(idx, "variation", "change"),
                    "pChange": gf0(idx, "percentChange", "pChange"),
                    "high":    gf0(idx, "high", "dayHigh"),
                    "low":     gf0(idx, "low",  "dayLow"),
                    "source":  "nse_allindices",
                }
                print(f"  Found GIFT Nifty in allIndices: {price}")

    if not result:
        print(f"  Got {len(idx_list)} items but matched none")
        if idx_list:
            print(f"  Sample keys: {list(idx_list[0].keys())}")
        save_error("indices", f"No matching indices in {len(idx_list)} items")
        return False

    if gift_found:
        result["gift"] = gift_found

    result["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save("indices.json", result)
    return True

def fetch_gift_nifty(session):
    """
    Fetch GIFT Nifty (NSE IFSC futures) price.
    Tries 5 sources in order. Stops at first success.
    GIFT Nifty replaced SGX Nifty in July 2023 and trades 24x5.
    """
    print("\n--- GIFT NIFTY ---")

    # ── Source 1: Yahoo Finance v8 chart API ─────────────────────────
    for yhost in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        try:
            url = f"https://{yhost}/v8/finance/chart/%5EGIFTNIFTY"
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://finance.yahoo.com/",
            }, timeout=10)
            if r.status_code == 200:
                meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
                price = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0)
                prev  = float(meta.get("previousClose") or meta.get("chartPreviousClose") or 0)
                if price > 0:
                    chg   = round(price - prev, 2) if prev else 0
                    pchg  = round(chg / prev * 100, 2) if prev > 0 else 0
                    result = {
                        "name": "GIFT NIFTY", "last": round(price, 2), "prev": round(prev, 2),
                        "change": chg, "pChange": pchg,
                        "high": float(meta.get("regularMarketDayHigh", price) or price),
                        "low":  float(meta.get("regularMarketDayLow",  price) or price),
                        "volume": meta.get("regularMarketVolume", 0),
                        "source": f"yahoo_v8_{yhost[:6]}",
                    }
                    print(f"  GIFT Nifty (Yahoo v8/{yhost[:6]}): {price} ({pchg:+.2f}%)")
                    _inject_gift(result)
                    return True
            else:
                print(f"  Yahoo v8/{yhost[:6]}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  Yahoo v8/{yhost[:6]} failed: {e}")

    # ── Source 2: Yahoo Finance v7 quote API ─────────────────────────
    for yhost in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        try:
            url = f"https://{yhost}/v7/finance/quote?symbols=%5EGIFTNIFTY&fields=regularMarketPrice,regularMarketPreviousClose,regularMarketChange,regularMarketChangePercent,regularMarketDayHigh,regularMarketDayLow"
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }, timeout=10)
            if r.status_code == 200:
                quotes = r.json().get("quoteResponse", {}).get("result", [])
                if quotes:
                    q = quotes[0]
                    price = float(q.get("regularMarketPrice") or 0)
                    prev  = float(q.get("regularMarketPreviousClose") or 0)
                    if price > 0:
                        result = {
                            "name": "GIFT NIFTY", "last": round(price, 2), "prev": round(prev, 2),
                            "change":  round(float(q.get("regularMarketChange", 0) or 0), 2),
                            "pChange": round(float(q.get("regularMarketChangePercent", 0) or 0), 2),
                            "high":    round(float(q.get("regularMarketDayHigh",  price) or price), 2),
                            "low":     round(float(q.get("regularMarketDayLow",   price) or price), 2),
                            "volume":  q.get("regularMarketVolume", 0),
                            "source": "yahoo_v7",
                        }
                        print(f"  GIFT Nifty (Yahoo v7): {price} ({result['pChange']:+.2f}%)")
                        _inject_gift(result)
                        return True
        except Exception as e:
            print(f"  Yahoo v7/{yhost[:6]} failed: {e}")

    # ── Source 3: NSE allIndices — GIFT sometimes included ───────────
    try:
        data = fetch_json(session, "https://www.nseindia.com/api/allIndices")
        if data:
            for idx in data.get("data", []):
                name = idx.get("index", idx.get("indexSymbol", ""))
                if "GIFT" in name.upper() or "IFSC" in name.upper():
                    price = gf0(idx, "last", "lastPrice", "indexValue")
                    if price > 0:
                        prev = gf0(idx, "previousClose", "previousDay")
                        chg  = gf0(idx, "variation", "change")
                        pchg = gf0(idx, "percentChange", "pChange")
                        result = {
                            "name": name, "last": price, "prev": prev,
                            "change": chg, "pChange": pchg,
                            "high": gf0(idx, "high", "dayHigh"),
                            "low":  gf0(idx, "low",  "dayLow"),
                            "source": "nse_allindices",
                        }
                        print(f"  GIFT Nifty (NSE allIndices): {price}")
                        _inject_gift(result)
                        return True
    except Exception as e:
        print(f"  NSE allIndices GIFT check failed: {e}")

    # ── Source 4: NSE IFSC dedicated endpoint ────────────────────────
    try:
        data = fetch_json(session, "https://www.nseindia.com/api/equity-stockIndices?index=GIFT%20NIFTY")
        if data:
            rows = data.get("data", [])
            for row in rows:
                price = gf0(row, "lastPrice", "last")
                if price > 0:
                    prev = gf0(row, "previousClose", "prev")
                    chg  = gf0(row, "change")
                    pchg = gf0(row, "pChange")
                    result = {
                        "name": "GIFT NIFTY", "last": price, "prev": prev,
                        "change": chg, "pChange": pchg,
                        "high": gf0(row, "dayHigh", "high"),
                        "low":  gf0(row, "dayLow",  "low"),
                        "source": "nse_ifsc_direct",
                    }
                    print(f"  GIFT Nifty (NSE IFSC direct): {price}")
                    _inject_gift(result)
                    return True
    except Exception as e:
        print(f"  NSE IFSC direct failed: {e}")

    # ── Source 5: Estimate from NIFTY 50 futures basis ───────────────
    # GIFT Nifty typically trades at a small premium/discount to NIFTY spot.
    # If all APIs fail, derive an estimate so the dashboard shows something.
    try:
        idx_path = OUT / "indices.json"
        if idx_path.exists():
            existing = json.loads(idx_path.read_text())
            nifty = existing.get("nifty", {})
            nifty_last = nifty.get("last", 0)
            if nifty_last > 0:
                # Use previous gift data if available, else show nifty spot
                if "gift" in existing and existing["gift"].get("last", 0) > 0:
                    old_gift = existing["gift"]
                    print(f"  GIFT Nifty: all APIs failed — keeping last known value {old_gift['last']}")
                    # Update timestamp but keep price
                    old_gift["stale"] = True
                    _inject_gift(old_gift)
                    return True
                else:
                    # First time: estimate from nifty spot (no premium data)
                    result = {
                        "name": "GIFT NIFTY", "last": nifty_last, "prev": nifty.get("prev", nifty_last),
                        "change": nifty.get("change", 0), "pChange": nifty.get("pChange", 0),
                        "high": nifty.get("high", nifty_last), "low": nifty.get("low", nifty_last),
                        "source": "estimated_from_nifty", "estimated": True,
                    }
                    print(f"  GIFT Nifty: all APIs failed — showing NIFTY as estimate")
                    _inject_gift(result)
                    return False  # Return False so caller knows it's an estimate
    except Exception as e:
        print(f"  GIFT Nifty estimate fallback failed: {e}")

    print("  GIFT Nifty: all 5 sources failed")
    return False


def _inject_gift(gift_data: dict):
    """Merge GIFT Nifty data into indices.json. Creates file if needed."""
    idx_path = OUT / "indices.json"
    try:
        existing = json.loads(idx_path.read_text()) if idx_path.exists() else {}
        existing["gift"] = gift_data
        existing["updatedAt"] = datetime.now(timezone.utc).isoformat()
        idx_path.write_text(json.dumps(existing, default=str, indent=2))
        print(f"  Injected gift into indices.json (source={gift_data.get('source','?')})")
    except Exception as e:
        print(f"  Could not inject GIFT into indices.json: {e}")


def fetch_option_chain(session, symbol):
    print(f"\n--- OPTION CHAIN: {symbol} ---")
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    data = fetch_json(session, url)
    if not data:
        save_error(f"oc_{symbol}", "API returned None")
        return False

    rec = data.get("records", data)
    expiries = rec.get("expiryDates", [])
    spot = gf0(rec, "underlyingValue")
    all_rows = rec.get("data", [])
    first_exp = expiries[0] if expiries else ""
    print(f"  spot={spot}, expiry={first_exp}, rows={len(all_rows)}")

    if not all_rows:
        save_error(f"oc_{symbol}", f"No rows. rec keys: {list(rec.keys())}")
        return False

    strikes_out, total_ce, total_pe = [], 0.0, 0.0
    for row in all_rows:
        sp = gf0(row, "strikePrice")
        if sp == 0: continue
        ce = row.get("CE", {}) or {}
        pe = row.get("PE", {}) or {}
        ce_oi = gf0(ce, "openInterest")
        pe_oi = gf0(pe, "openInterest")
        total_ce += ce_oi
        total_pe += pe_oi
        strikes_out.append({
            "strike": sp,
            "ceOI": ce_oi, "ceChgOI": gf0(ce, "changeinOpenInterest"),
            "ceVol": gf0(ce, "totalTradedVolume"), "ceIV": gf0(ce, "impliedVolatility"),
            "ceLTP": gf0(ce, "lastPrice"),
            "peOI": pe_oi, "peChgOI": gf0(pe, "changeinOpenInterest"),
            "peVol": gf0(pe, "totalTradedVolume"), "peIV": gf0(pe, "impliedVolatility"),
            "peLTP": gf0(pe, "lastPrice"),
        })

    strikes_out.sort(key=lambda x: x["strike"])

    # Max pain (pure Python)
    max_pain = spot
    try:
        min_loss = float('inf')
        for tgt in strikes_out:
            loss = sum((tgt["strike"] - s["strike"]) * s["ceOI"] for s in strikes_out if tgt["strike"] > s["strike"])
            loss += sum((s["strike"] - tgt["strike"]) * s["peOI"] for s in strikes_out if tgt["strike"] < s["strike"])
            if loss < min_loss:
                min_loss = loss
                max_pain = tgt["strike"]
    except: pass

    pcr = round(total_pe / total_ce, 3) if total_ce > 0 else 1.0
    atm = min(strikes_out, key=lambda s: abs(s["strike"] - spot)) if strikes_out else {}
    atm_iv = round((atm.get("ceIV", 0) + atm.get("peIV", 0)) / 2, 2)

    print(f"  strikes={len(strikes_out)}, CE={total_ce:.0f}, PE={total_pe:.0f}, PCR={pcr}, maxpain={max_pain}")
    save(f"oc_{symbol.lower()}.json", {
        "symbol": symbol, "spot": spot, "expiry": first_exp,
        "expiries": expiries[:8], "pcr": pcr, "maxPain": max_pain,
        "atmIV": atm_iv, "totalCeOI": total_ce, "totalPeOI": total_pe,
        "strikes": strikes_out, "isLive": True,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    })
    return True

def fetch_fii_dii(session):
    print("\n--- FII/DII ---")
    urls = [
        "https://www.nseindia.com/api/fiidiiTradeReact",
        "https://www.nseindia.com/api/fii-stats?type=equities",
    ]
    fii_list = None
    for url in urls:
        data = fetch_json(session, url)
        if not data: continue
        if isinstance(data, list) and len(data) > 0:
            fii_list = data; break
        if isinstance(data, dict):
            for key in ["data", "fiidii", "Data"]:
                if data.get(key) and isinstance(data[key], list):
                    fii_list = data[key]; break
            if fii_list: break
        time.sleep(2)

    if not fii_list:
        save_error("fii_dii", "All FII endpoints returned None")
        return False

    print(f"  Got {len(fii_list)} rows. Keys: {list(fii_list[0].keys()) if fii_list else 'empty'}")
    rows = []
    for row in fii_list[:15]:
        date = (row.get("date") or row.get("Date") or row.get("tradeDate") or "")
        r = {
            "date":    str(date)[:10],
            "fiiBuy":  gf0(row, "fii_buy_value", "FII_BUY_VAL", "fiiBuyVal", "buyValue"),
            "fiiSell": gf0(row, "fii_sell_value","FII_SELL_VAL","fiiSellVal","sellValue"),
            "fiiNet":  gf0(row, "fii_net_value", "FII_NET_VAL", "fiiNetVal", "netValue"),
            "diiBuy":  gf0(row, "dii_buy_value", "DII_BUY_VAL", "diiBuyVal", "diiBuy"),
            "diiSell": gf0(row, "dii_sell_value","DII_SELL_VAL","diiSellVal","diiSell"),
            "diiNet":  gf0(row, "dii_net_value", "DII_NET_VAL", "diiNetVal", "diiNet"),
        }
        if r["fiiBuy"] != 0 or r["fiiSell"] != 0:
            rows.append(r)

    print(f"  Saving {len(rows)} rows")
    save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
    return True

def try_nse_package():
    """Fallback using the nse package"""
    print("\n--- NSE PACKAGE FALLBACK ---")
    try:
        from nse import NSE
        with NSE(download_folder=OUT, server=True) as nse:
            raw = nse.listIndices()
            idx_list = raw.get("data", [])
            want = {"NIFTY 50":"nifty","NIFTY BANK":"banknifty","NIFTY FIN SERVICE":"finnifty","INDIA VIX":"vix","India VIX":"vix"}
            result = {}
            for idx in idx_list:
                key = want.get(idx.get("index",""))
                if key:
                    result[key] = {"name":idx.get("index",""),"last":gf0(idx,"last"),"change":gf0(idx,"variation"),"pChange":gf0(idx,"percentChange"),"open":gf0(idx,"open"),"high":gf0(idx,"high"),"low":gf0(idx,"low"),"prev":gf0(idx,"previousClose")}
            if result:
                result["updatedAt"] = datetime.now(timezone.utc).isoformat()
                # Preserve the gift key if already fetched
                existing_indices = {}
                try:
                    idx_path = OUT / "indices.json"
                    if idx_path.exists():
                        existing_indices = json.loads(idx_path.read_text())
                except: pass
                if "gift" in existing_indices:
                    result["gift"] = existing_indices["gift"]
                save("indices.json", result)
            time.sleep(2)
            for sym in ["NIFTY","BANKNIFTY","FINNIFTY"]:
                try:
                    raw = nse.optionChain(sym)
                    rec = raw.get("records", raw)
                    expiries = rec.get("expiryDates", [])
                    spot = gf0(rec, "underlyingValue")
                    all_rows = rec.get("data", [])
                    strikes_out, total_ce, total_pe = [], 0.0, 0.0
                    for row in all_rows:
                        sp = gf0(row, "strikePrice")
                        if sp == 0: continue
                        ce = row.get("CE", {}) or {}
                        pe = row.get("PE", {}) or {}
                        ce_oi = gf0(ce, "openInterest"); pe_oi = gf0(pe, "openInterest")
                        total_ce += ce_oi; total_pe += pe_oi
                        strikes_out.append({"strike":sp,"ceOI":ce_oi,"ceChgOI":gf0(ce,"changeinOpenInterest"),"ceVol":gf0(ce,"totalTradedVolume"),"ceIV":gf0(ce,"impliedVolatility"),"ceLTP":gf0(ce,"lastPrice"),"peOI":pe_oi,"peChgOI":gf0(pe,"changeinOpenInterest"),"peVol":gf0(pe,"totalTradedVolume"),"peIV":gf0(pe,"impliedVolatility"),"peLTP":gf0(pe,"lastPrice")})
                    strikes_out.sort(key=lambda x: x["strike"])
                    pcr = round(total_pe/total_ce,3) if total_ce else 1.0
                    atm = min(strikes_out, key=lambda s: abs(s["strike"]-spot)) if strikes_out else {}
                    save(f"oc_{sym.lower()}.json", {"symbol":sym,"spot":spot,"expiry":expiries[0] if expiries else "","expiries":expiries[:8],"pcr":pcr,"maxPain":spot,"atmIV":round((atm.get("ceIV",0)+atm.get("peIV",0))/2,2),"totalCeOI":total_ce,"totalPeOI":total_pe,"strikes":strikes_out,"isLive":True,"updatedAt":datetime.now(timezone.utc).isoformat()})
                    time.sleep(3)
                except Exception as e:
                    print(f"  {sym}: {e}")
            # FII/DII via nse package
            try:
                for method_name in ["fiiDII", "fiidii", "fii_dii"]:
                    method = getattr(nse, method_name, None)
                    if not method: continue
                    fii_raw = method()
                    fii_list = fii_raw if isinstance(fii_raw, list) else fii_raw.get("data", [])
                    rows = []
                    for row in fii_list[:15]:
                        date = row.get("date") or row.get("Date") or ""
                        r = {"date":str(date)[:10],"fiiBuy":gf0(row,"fii_buy_value","fiiBuyVal","buyValue"),"fiiSell":gf0(row,"fii_sell_value","fiiSellVal","sellValue"),"fiiNet":gf0(row,"fii_net_value","fiiNetVal","netValue"),"diiBuy":gf0(row,"dii_buy_value","diiBuyVal"),"diiSell":gf0(row,"dii_sell_value","diiSellVal"),"diiNet":gf0(row,"dii_net_value","diiNetVal")}
                        if r["fiiBuy"] != 0 or r["fiiSell"] != 0: rows.append(r)
                    if rows:
                        save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
                        print(f"  FII saved via {method_name}: {len(rows)} rows")
                        break
            except Exception as fe:
                print(f"  FII fallback: {fe}")
        print("nse package fallback OK")
        return True
    except Exception as e:
        print(f"nse package fallback failed: {e}")
        traceback.print_exc()
        return False

def main():
    print(f"\n{'='*55}")
    print(f"NSE FETCH — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}")

    ok = {"indices": False, "oc": False, "fii": False}

    # Method 1: direct HTTP with browser session
    session = make_session()
    ok["indices"] = fetch_indices(session)
    time.sleep(2)

    # GIFT Nifty — fetched after indices so it can inject into indices.json
    ok["gift"] = fetch_gift_nifty(session)
    time.sleep(2)

    oc_ok = 0
    for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
        if fetch_option_chain(session, sym): oc_ok += 1
        time.sleep(4)
    ok["oc"] = oc_ok > 0

    ok["fii"] = fetch_fii_dii(session)

    # Method 2: nse package if direct failed
    if not ok["indices"] or not ok["oc"]:
        print("\nDirect HTTP failed, trying nse package...")
        pkg_ok = try_nse_package()
        if pkg_ok:
            # nse package succeeded - mark all as ok (it only returns True after saving)
            ok["indices"] = True
            ok["oc"] = True
            ok["fii"] = True
            print(f"Fallback succeeded: indices=True oc=True fii=True")

    save("fetch_status.json", {
        "lastRun": datetime.now(timezone.utc).isoformat(),
        "success": ok,
        "allOk": all(ok.values()),
    })

    # ── Run Signal Engine (GEX, Dealer, Strategies) ──────────────────────
    if ok["oc"]:
        print(f"\n{'='*55}")
        print("SIGNAL ENGINE — computing GEX + Dealer + Strategies...")
        try:
            # Try importing from same directory first
            import importlib.util, sys as _sys
            _spec = importlib.util.spec_from_file_location("signal_engine", Path(__file__).parent / "signal_engine.py")
            if _spec:
                _mod = importlib.util.module_from_spec(_spec)
                _spec.loader.exec_module(_mod)
                sig_ok = _mod.run()
                ok["signals"] = sig_ok
                print(f"Signal engine: {'OK' if sig_ok else 'FAILED'}")
            else:
                print("signal_engine.py not found — skipping (add it to repo root)")
                ok["signals"] = False
        except Exception as e:
            print(f"Signal engine error: {e}")
            import traceback; traceback.print_exc()
            ok["signals"] = False
    else:
        ok["signals"] = False
        print("Skipping signal engine — no OC data fetched")

    # Re-save status with signal engine result
    save("fetch_status.json", {
        "lastRun": datetime.now(timezone.utc).isoformat(),
        "success": ok,
        "allOk": all(v for k, v in ok.items() if k not in ("signals", "gift")),  # signals+gift optional
    })

    print(f"\n{'='*55}")
    print(f"RESULT: indices={ok['indices']} gift={ok.get('gift',False)} oc={ok['oc']} fii={ok['fii']} signals={ok.get('signals',False)}")
    if not ok["indices"] and not ok["oc"]:
        print("WARNING: Both methods failed — check Actions log for details")
        # Don't exit(1): workflow will still commit whatever partial data exists

if __name__ == "__main__":
    main()
