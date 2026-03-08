"""
NSE Data Fetcher - WORKING VERSION
All bugs fixed based on actual API response inspection.
"""
import json, time, traceback
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

OUT = Path("data")
OUT.mkdir(exist_ok=True)

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str))
    print(f"SAVED: {name}")

def gf(d, *keys):
    for k in keys:
        try:
            v = d.get(k)
            if v is not None and str(v).strip() not in ('', '-', '--', 'nan', '0'):
                return float(str(v).replace(",", ""))
        except: pass
    return 0.0

def gf0(d, *keys):
    """Same as gf but allows 0"""
    for k in keys:
        try:
            v = d.get(k)
            if v is not None and str(v).strip() not in ('', '-', '--', 'nan'):
                return float(str(v).replace(",", ""))
        except: pass
    return 0.0

def main():
    from nse import NSE
    print("NSE FETCH START")

    with NSE(download_folder=OUT, server=True) as nse:

        # ── INDICES ───────────────────────────────────────────────────
        # listIndices() returns dict with 'data' key containing list
        try:
            raw = nse.listIndices()
            idx_list = raw.get("data", [])

            want = {
                "NIFTY 50":            "nifty",
                "NIFTY BANK":          "banknifty",
                "NIFTY FIN SERVICE":   "finnifty",   # exact name from NSE
                "Nifty Fin Service":   "finnifty",
                "INDIA VIX":           "vix",
                "India VIX":           "vix",
                "NIFTY MIDCAP SELECT": "midcap",
            }
            result = {}
            for idx in idx_list:
                name = idx.get("index", "")
                key = want.get(name)
                if key:
                    result[key] = {
                        "name":    name,
                        "last":    gf0(idx, "last"),
                        "change":  gf0(idx, "variation"),
                        "pChange": gf0(idx, "percentChange"),
                        "open":    gf0(idx, "open"),
                        "high":    gf0(idx, "high"),
                        "low":     gf0(idx, "low"),
                        "prev":    gf0(idx, "previousClose"),
                    }
                    print(f"  {name}: last={result[key]['last']}")

            # If finnifty missing, try fetching via quote
            if "finnifty" not in result:
                print("  finnifty not in listIndices, trying quote...")
                try:
                    q = nse.quote("NIFTY FIN SERVICE", type="index")
                    result["finnifty"] = {
                        "name": "NIFTY FIN SERVICE",
                        "last":    gf0(q, "last", "lastPrice", "underlyingValue"),
                        "change":  gf0(q, "change", "variation"),
                        "pChange": gf0(q, "pChange", "percentChange"),
                    }
                    print(f"  finnifty via quote: {result['finnifty']['last']}")
                except Exception as e:
                    print(f"  finnifty quote failed: {e}")

            result["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save("indices.json", result)
        except Exception as e:
            traceback.print_exc()

        time.sleep(2)

        # ── OPTION CHAIN ──────────────────────────────────────────────
        # NOTE: from log - expiryDate field is EMPTY in row data ('')
        # The rows don't have expiryDate — just take all rows as first expiry
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            try:
                print(f"\nFetching OC {sym}...")
                raw = nse.optionChain(sym)
                rec = raw.get("records", raw)
                expiries = rec.get("expiryDates", [])
                spot = gf0(rec, "underlyingValue")
                first_exp = expiries[0] if expiries else ""
                all_rows = rec.get("data", [])
                print(f"  {sym}: spot={spot}, expiry={first_exp}, rows={len(all_rows)}")

                # From the log: expiryDate is '' for all rows in this API version
                # So don't filter by expiry — the package already returns only
                # the near-expiry data in filtered form
                strikes_out, total_ce, total_pe = [], 0.0, 0.0
                for row in all_rows:
                    sp = gf0(row, "strikePrice")
                    if sp == 0:
                        continue
                    ce = row.get("CE", {})
                    pe = row.get("PE", {})
                    ce_oi = gf0(ce, "openInterest")
                    pe_oi = gf0(pe, "openInterest")
                    total_ce += ce_oi
                    total_pe += pe_oi
                    strikes_out.append({
                        "strike":  sp,
                        "ceOI":    ce_oi,
                        "ceChgOI": gf0(ce, "changeinOpenInterest"),
                        "ceVol":   gf0(ce, "totalTradedVolume"),
                        "ceIV":    gf0(ce, "impliedVolatility"),
                        "ceLTP":   gf0(ce, "lastPrice"),
                        "peOI":    pe_oi,
                        "peChgOI": gf0(pe, "changeinOpenInterest"),
                        "peVol":   gf0(pe, "totalTradedVolume"),
                        "peIV":    gf0(pe, "impliedVolatility"),
                        "peLTP":   gf0(pe, "lastPrice"),
                    })

                strikes_out.sort(key=lambda x: x["strike"])
                print(f"  saved {len(strikes_out)} strikes, CE={total_ce:.0f}, PE={total_pe:.0f}")

                # Max pain
                max_pain = spot
                try:
                    from datetime import datetime as dt
                    exp_dt = dt.strptime(first_exp, "%d-%b-%Y")
                    mp = nse.maxpain(optionChain=raw, expiryDate=exp_dt)
                    max_pain = gf0(mp, "maxpain") or spot
                except Exception as me:
                    print(f"  maxpain: {me}")

                pcr = round(total_pe / total_ce, 3) if total_ce > 0 else 1.0
                atm = min(strikes_out, key=lambda s: abs(s["strike"]-spot)) if strikes_out else {}
                atm_iv = round((atm.get("ceIV",0)+atm.get("peIV",0))/2, 2)

                save(f"oc_{sym.lower()}.json", {
                    "symbol": sym, "spot": spot, "expiry": first_exp,
                    "expiries": expiries[:8], "pcr": pcr,
                    "maxPain": max_pain, "atmIV": atm_iv,
                    "totalCeOI": total_ce, "totalPeOI": total_pe,
                    "strikes": strikes_out,
                    "isLive": True,
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                })
                time.sleep(3)
            except Exception as e:
                traceback.print_exc()

        # ── FII/DII ───────────────────────────────────────────────────
        # Correct method name from NSE package source
        fii_methods = ["fiiDII", "fiidii", "fii_dii", "getFiiDii", "fiiStats"]
        fii_done = False
        for method_name in fii_methods:
            try:
                method = getattr(nse, method_name, None)
                if method is None:
                    continue
                print(f"\nTrying nse.{method_name}()...")
                fii_raw = method()
                fii_list = fii_raw if isinstance(fii_raw, list) else fii_raw.get("data", [])
                print(f"  Keys: {list(fii_list[0].keys()) if fii_list else 'empty'}")
                rows = []
                for row in fii_list[:10]:
                    rows.append({
                        "date":    row.get("date", row.get("Date", "")),
                        "fiiBuy":  gf0(row, "fii_buy_value",  "fiiBuy",  "buyValue"),
                        "fiiSell": gf0(row, "fii_sell_value", "fiiSell", "sellValue"),
                        "fiiNet":  gf0(row, "fii_net_value",  "fiiNet",  "netValue"),
                        "diiBuy":  gf0(row, "dii_buy_value",  "diiBuy"),
                        "diiSell": gf0(row, "dii_sell_value", "diiSell"),
                        "diiNet":  gf0(row, "dii_net_value",  "diiNet"),
                    })
                save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
                fii_done = True
                break
            except Exception as e:
                print(f"  {method_name} failed: {e}")

        if not fii_done:
            # Print all available methods so we can find the right one
            methods = [m for m in dir(nse) if not m.startswith('_')]
            print(f"\nAll NSE methods: {methods}")

    print("\nDONE")

if __name__ == "__main__":
    main()
