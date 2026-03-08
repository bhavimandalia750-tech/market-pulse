"""
NSE Data Fetcher - FIXED
Bugs fixed from log:
1. listIndices() returns dict with 'data' key, not a plain list
2. expiryDate string format mismatch - don't filter, take first expiry rows differently
3. FII method is nse.fiiDII() not fiidiiTradeReact()
"""
import json, time, traceback
from pathlib import Path
from datetime import datetime, timezone

OUT = Path("data")
OUT.mkdir(exist_ok=True)

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str))
    print(f"SAVED: {name}")

def gf(d, *keys):
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
        # listIndices() returns a DICT like {"advance": {...}, "data": [...]}
        try:
            raw = nse.listIndices()
            print(f"listIndices type: {type(raw)}, keys: {list(raw.keys()) if isinstance(raw, dict) else 'list'}")

            # Extract the list — it's under 'data' key
            idx_list = raw.get("data", []) if isinstance(raw, dict) else raw

            print(f"First index item: {json.dumps(idx_list[0] if idx_list else {}, default=str)[:400]}")

            want = {
                "NIFTY 50":            "nifty",
                "NIFTY BANK":          "banknifty",
                "Nifty Bank":          "banknifty",
                "NIFTY FIN SERVICE":   "finnifty",
                "Nifty Fin Service":   "finnifty",
                "INDIA VIX":           "vix",
                "India VIX":           "vix",
                "NIFTY MIDCAP SELECT": "midcap",
                "Nifty Midcap Select": "midcap",
            }
            result = {}
            for idx in idx_list:
                name = idx.get("index") or idx.get("indexSymbol", "")
                key = want.get(name)
                if key:
                    last = gf(idx, "last", "lastPrice", "indexValue", "currentValue")
                    print(f"  {name} -> last={last}")
                    result[key] = {
                        "name":    name,
                        "last":    last,
                        "change":  gf(idx, "variation", "change", "netChange"),
                        "pChange": gf(idx, "percentChange", "pChange"),
                        "open":    gf(idx, "open"),
                        "high":    gf(idx, "high"),
                        "low":     gf(idx, "low"),
                        "prev":    gf(idx, "previousClose", "prev"),
                    }
            result["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save("indices.json", result)
            print(f"Indices saved: {[k for k in result if k != 'updatedAt']}")
        except Exception as e:
            traceback.print_exc()

        time.sleep(2)

        # ── OPTION CHAIN ──────────────────────────────────────────────
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            try:
                print(f"\nFetching OC {sym}...")
                raw = nse.optionChain(sym)
                rec = raw.get("records", raw)
                expiries = rec.get("expiryDates", [])
                spot = gf(rec, "underlyingValue")
                sel = expiries[0] if expiries else ""
                all_rows = rec.get("data", [])

                print(f"  spot={spot}, expiry[0]={sel!r}, total_rows={len(all_rows)}")

                # Show sample expiryDate value from actual data to diagnose mismatch
                if all_rows:
                    sample_dates = list({r.get("expiryDate","") for r in all_rows[:20]})
                    print(f"  expiryDate values in data: {sample_dates[:5]}")

                # KEY FIX: collect strikes for the FIRST expiry only
                # Group by expiryDate then take first group
                from collections import defaultdict
                by_expiry = defaultdict(list)
                for row in all_rows:
                    by_expiry[row.get("expiryDate", "")].append(row)

                print(f"  Expiries in data: {list(by_expiry.keys())[:5]}")

                # Take the earliest expiry that has data
                first_exp = expiries[0] if expiries else (list(by_expiry.keys())[0] if by_expiry else "")
                # Try exact match first, then fallback to first key in data
                rows_for_exp = by_expiry.get(first_exp) or list(by_expiry.values())[0] if by_expiry else []
                print(f"  Using expiry={first_exp!r}, rows={len(rows_for_exp)}")

                strikes_out, total_ce, total_pe = [], 0.0, 0.0
                for row in rows_for_exp:
                    sp = gf(row, "strikePrice")
                    ce = row.get("CE", {})
                    pe = row.get("PE", {})
                    ce_oi = gf(ce, "openInterest")
                    pe_oi = gf(pe, "openInterest")
                    total_ce += ce_oi
                    total_pe += pe_oi
                    strikes_out.append({
                        "strike":  sp,
                        "ceOI":    ce_oi,
                        "ceChgOI": gf(ce, "changeinOpenInterest"),
                        "ceVol":   gf(ce, "totalTradedVolume"),
                        "ceIV":    gf(ce, "impliedVolatility"),
                        "ceLTP":   gf(ce, "lastPrice"),
                        "peOI":    pe_oi,
                        "peChgOI": gf(pe, "changeinOpenInterest"),
                        "peVol":   gf(pe, "totalTradedVolume"),
                        "peIV":    gf(pe, "impliedVolatility"),
                        "peLTP":   gf(pe, "lastPrice"),
                    })

                strikes_out.sort(key=lambda x: x["strike"])
                print(f"  strikes={len(strikes_out)}, CE_OI={total_ce:.0f}, PE_OI={total_pe:.0f}")

                # Max pain - pass datetime object if needed
                max_pain = spot
                try:
                    from datetime import datetime as dt
                    # maxpain needs a datetime or the string — try both
                    try:
                        exp_dt = dt.strptime(first_exp, "%d-%b-%Y")
                        mp = nse.maxpain(optionChain=raw, expiryDate=exp_dt)
                    except:
                        mp = nse.maxpain(optionChain=raw, expiryDate=first_exp)
                    max_pain = gf(mp, "maxpain") or spot
                    print(f"  maxpain={max_pain}")
                except Exception as me:
                    print(f"  maxpain skipped: {me}")

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
        # Correct method: nse.fiiDII() — NOT fiidiiTradeReact()
        try:
            print("\nFetching FII/DII...")
            fii_raw = nse.fiiDII()
            print(f"fiiDII type={type(fii_raw)}, sample: {json.dumps(fii_raw[0] if isinstance(fii_raw,list) else fii_raw, default=str)[:400]}")

            rows = []
            fii_list = fii_raw if isinstance(fii_raw, list) else fii_raw.get("data", [])
            for row in fii_list[:10]:
                rows.append({
                    "date":    row.get("date", row.get("Date", "")),
                    "fiiBuy":  gf(row, "fii_buy_value",  "fiiBuy",  "BUY_VALUE", "buyValue"),
                    "fiiSell": gf(row, "fii_sell_value", "fiiSell", "SELL_VALUE","sellValue"),
                    "fiiNet":  gf(row, "fii_net_value",  "fiiNet",  "NET_VALUE", "netValue"),
                    "diiBuy":  gf(row, "dii_buy_value",  "diiBuy"),
                    "diiSell": gf(row, "dii_sell_value", "diiSell"),
                    "diiNet":  gf(row, "dii_net_value",  "diiNet"),
                })
            save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
        except Exception as e:
            traceback.print_exc()
            print(f"FII/DII error: {e}")

    print("\nDONE")

if __name__ == "__main__":
    main()
