"""
NSE Data Fetcher
Uses `nse` Python package with server=True (HTTP/2 via httpx).
Tested to work on GitHub Actions / AWS / cloud servers.

Install: pip install "nse[server]"
"""

import json
import time
import os
from pathlib import Path
from datetime import datetime, timezone

OUT = Path("data")
OUT.mkdir(exist_ok=True)

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str))
    print(f"  saved {name}")

def gf(d, k):
    try: return float(str(d.get(k, 0) or 0).replace(",", ""))
    except: return 0.0

def main():
    from nse import NSE

    print("Starting NSE fetch (server=True / HTTP2)...")

    with NSE(download_folder=OUT, server=True) as nse:

        # ── INDICES ───────────────────────────────────────────────────────
        try:
            result = {}
            indices_raw = nse.listIndices()          # returns list of index dicts
            want = {
                "NIFTY 50": "nifty",
                "NIFTY BANK": "banknifty",
                "NIFTY FIN SERVICE": "finnifty",
                "INDIA VIX": "vix",
                "NIFTY MIDCAP SELECT": "midcap",
            }
            for idx in indices_raw:
                name = idx.get("indexSymbol") or idx.get("index", "")
                key = want.get(name)
                if key:
                    result[key] = {
                        "name": name,
                        "last":    gf(idx, "last"),
                        "change":  gf(idx, "variation"),
                        "pChange": gf(idx, "percentChange"),
                    }
            result["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save("indices.json", result)
        except Exception as e:
            print(f"  WARN indices: {e}")

        time.sleep(2)

        # ── OPTION CHAIN ──────────────────────────────────────────────────
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            try:
                print(f"  fetching OC {sym}...")
                raw = nse.optionChain(sym)
                records = raw["records"]
                expiries = records["expiryDates"]
                spot = float(str(records["underlyingValue"]).replace(",", ""))
                sel = expiries[0]

                strikes_out = []
                total_ce = 0.0
                total_pe = 0.0

                for row in records["data"]:
                    if row.get("expiryDate") != sel:
                        continue
                    sp = float(row["strikePrice"])
                    ce = row.get("CE", {})
                    pe = row.get("PE", {})
                    ce_oi = gf(ce, "openInterest")
                    pe_oi = gf(pe, "openInterest")
                    total_ce += ce_oi
                    total_pe += pe_oi
                    strikes_out.append({
                        "strike":   sp,
                        "ceOI":     ce_oi,
                        "ceChgOI":  gf(ce, "changeinOpenInterest"),
                        "ceVol":    gf(ce, "totalTradedVolume"),
                        "ceIV":     gf(ce, "impliedVolatility"),
                        "ceLTP":    gf(ce, "lastPrice"),
                        "peOI":     pe_oi,
                        "peChgOI":  gf(pe, "changeinOpenInterest"),
                        "peVol":    gf(pe, "totalTradedVolume"),
                        "peIV":     gf(pe, "impliedVolatility"),
                        "peLTP":    gf(pe, "lastPrice"),
                    })

                strikes_out.sort(key=lambda x: x["strike"])

                # Max pain using built-in
                try:
                    mp_data = nse.maxpain(optionChain=raw, expiryDate=sel)
                    max_pain = float(mp_data.get("maxpain", spot))
                except:
                    max_pain = spot

                pcr = round(total_pe / total_ce, 3) if total_ce > 0 else 1.0

                atm = min(strikes_out, key=lambda s: abs(s["strike"] - spot), default={})
                atm_iv = round((atm.get("ceIV", 0) + atm.get("peIV", 0)) / 2, 2)

                save(f"oc_{sym.lower()}.json", {
                    "symbol":     sym,
                    "spot":       spot,
                    "expiry":     sel,
                    "expiries":   expiries[:8],
                    "pcr":        pcr,
                    "maxPain":    max_pain,
                    "atmIV":      atm_iv,
                    "totalCeOI":  total_ce,
                    "totalPeOI":  total_pe,
                    "strikes":    strikes_out,
                    "isLive":     True,
                    "updatedAt":  datetime.now(timezone.utc).isoformat(),
                })
                time.sleep(3)   # NSE rate limit
            except Exception as e:
                import traceback; traceback.print_exc()
                print(f"  WARN OC {sym}: {e}")

        # ── FII / DII ─────────────────────────────────────────────────────
        try:
            raw_fii = nse.fiidiiTradeReact()   # returns list
            rows = []
            for row in raw_fii[:10]:
                rows.append({
                    "date":    row.get("date", ""),
                    "fiiBuy":  gf(row, "fii_buy_value"),
                    "fiiSell": gf(row, "fii_sell_value"),
                    "fiiNet":  gf(row, "fii_net_value"),
                    "diiBuy":  gf(row, "dii_buy_value"),
                    "diiSell": gf(row, "dii_sell_value"),
                    "diiNet":  gf(row, "dii_net_value"),
                })
            save("fii_dii.json", {"data": rows, "updatedAt": datetime.now(timezone.utc).isoformat()})
        except Exception as e:
            print(f"  WARN FII/DII: {e}")

    print("Done.")

if __name__ == "__main__":
    main()
