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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
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
    print("Harvesting NSE cookies...")
    for attempt in range(3):
        try:
            r = s.get("https://www.nseindia.com/", timeout=20)
            print(f"  Homepage: {r.status_code}, cookies: {len(s.cookies)}")
            time.sleep(2)
            r2 = s.get("https://www.nseindia.com/market-data/live-equity-market", timeout=20)
            print(f"  Market page: {r2.status_code}")
            time.sleep(2)
            if s.cookies:
                return s
        except Exception as e:
            print(f"  Cookie attempt {attempt+1} failed: {e}")
            time.sleep(5)
    print("  No cookies harvested, proceeding anyway")
    return s

def fetch_json(session, url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            print(f"  GET .../{url.split('/')[-1].split('?')[0]}: {r.status_code}")
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 401:
                print("  401 - refreshing cookies")
                session.get("https://www.nseindia.com/", timeout=15)
                time.sleep(4)
            elif r.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"  429 rate limit - waiting {wait}s")
                time.sleep(wait)
            else:
                time.sleep(5)
        except Exception as e:
            print(f"  Attempt {attempt+1} error: {e}")
            time.sleep(5)
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

    if not result:
        print(f"  Got {len(idx_list)} items but matched none")
        if idx_list:
            print(f"  Sample keys: {list(idx_list[0].keys())}")
        save_error("indices", f"No matching indices in {len(idx_list)} items")
        return False

    result["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save("indices.json", result)
    return True

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
    time.sleep(3)

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
            # nse package saved the files - update ok flags
            from pathlib import Path as _P
            if (_P("data") / "indices.json").stat().st_size > 100:
                ok["indices"] = True
            if (_P("data") / "oc_nifty.json").stat().st_size > 1000:
                ok["oc"] = True
            if (_P("data") / "fii_dii.json").stat().st_size > 100:
                ok["fii"] = True
            print(f"After fallback: indices={ok['indices']} oc={ok['oc']} fii={ok['fii']}")

    save("fetch_status.json", {
        "lastRun": datetime.now(timezone.utc).isoformat(),
        "success": ok,
        "allOk": all(ok.values()),
    })

    print(f"\n{'='*55}")
    print(f"RESULT: indices={ok['indices']} oc={ok['oc']} fii={ok['fii']}")
    if not ok["indices"] and not ok["oc"]:
        print("CRITICAL FAIL: NSE blocking this IP")
        sys.exit(1)

if __name__ == "__main__":
    main()
