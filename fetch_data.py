"""
NSE Data Fetcher — fixed field names
pip install "nse[server]"
"""

import json, time, traceback
from pathlib import Path
from datetime import datetime, timezone

OUT = Path("data")
OUT.mkdir(exist_ok=True)

def save(name, obj):
    (OUT / name).write_text(json.dumps(obj, default=str))
    print(f"  ✅ saved {name}")

def gf(d, *keys):
    """Try multiple key names, return first float found."""
    for k in keys:
        try:
            v = d.get(k)
            if v is not None and v != '':
                return float(str(v).replace(",", "").replace("-", "0") or 0)
        except: pass
    return 0.0

def main():
    from nse import NSE
    print("Starting NSE fetch (server=True)...")

    with NSE(download_folder=OUT, server=True) as nse:

        # ── INDICES ───────────────────────────────────────────────────────
        # NSE listIndices() returns list of dicts with keys:
        # 'index', 'indexSymbol', 'last', 'variation', 'percentChange'
        try:
            raw = nse.listIndices()
            print(f"  listIndices sample: {json.dumps(raw[0] if raw else {}, default=str)[:300]}")

            want = {
                "NIFTY 50":          "nifty",
                "NIFTY BANK":        "banknifty",
                "NIFTY FIN SERVICE": "finnifty",
                "INDIA VIX":         "vix",
                "NIFTY MIDCAP SELECT":"midcap",
            }
            result = {}
            for idx in raw:
                # Try both 'index' and 'indexSymbol' as key
                name = idx.get("index") or idx.get("indexSymbol", "")
                key = want.get(name)
                if key:
                    result[key] = {
                        "name":    name,
                        "last":    gf(idx, "last", "lastPrice"),
                        "change":  gf(idx, "variation", "change"),
                        "pChange": gf(idx, "percentChange", "pChange"),
                        "open":    gf(idx, "open"),
                        "high":    gf(idx, "high"),
                        "low":     gf(idx, "low"),
                        "prev":    gf(idx, "previousClose", "prev"),
                    }
            result["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save("indices.json", result)
            print(f"  Indices found: {list(result.keys())}")
        except Exception as e:
            traceback.print_exc()
            print(f"  WARN indices: {e}")

        time.sleep(2)

        # ── OPTION CHAIN ──────────────────────────────────────────────────
        for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
            try:
                print(f"  Fetching OC {sym}...")
                raw = nse.optionChain(sym)

                records = raw.get("records", raw)  # some versions wrap differently
                expiries = records.get("expiryDates", [])
                spot_raw = records.get("underlyingValue", 0)
                spot = float(str(spot_raw).replace(",", ""))
                sel = expiries[0] if expiries else ""
                print(f"    {sym}: spot={spot}, expiry={sel}, rows={len(records.get('data',[]))}")

                strikes_out = []
                total_ce = 0.0
                total_pe = 0.0

                for row in records.get("data", []):
                    if sel and row.get("expiryDate") != sel:
                        continue
                    sp = float(str(row.get("strikePrice", 0)).replace(",", ""))
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

                # Max pain
                try:
                    mp_data = nse.maxpain(optionChain=raw, expiryDate=sel)
                    max_pain = float(str(mp_data.get("maxpain", spot)).replace(",",""))
                except Exception as me:
                    print(f"    maxpain fallback: {me}")
                    max_pain = spot

                pcr = round(total_pe / total_ce, 3) if total_ce > 0 else 1.0
                atm = min(strikes_out, key=lambda s: abs(s["strike"]-spot)) if strikes_out else {}
                atm_iv = round((atm.get("ceIV",0) + atm.get("peIV",0)) / 2, 2)

                save(f"oc_{sym.lower()}.json", {
                    "symbol":    sym,
                    "spot":      spot,
                    "expiry":    sel,
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
                print(f"    {sym}: {len(strikes_out)} strikes, PCR={pcr}, MaxPain={max_pain}")
                time.sleep(3)

            except Exception as e:
                traceback.print_exc()
                print(f"  WARN OC {sym}: {e}")

        # ── FII / DII ─────────────────────────────────────────────────────
        try:
            raw_fii = nse.fiidiiTradeReact()
            print(f"  FII sample: {json.dumps(raw_fii[0] if raw_fii else {}, default=str)[:300]}")
            rows = []
            for row in raw_fii[:10]:
                rows.append({
                    "date":    row.get("date", ""),
                    # Try all possible key formats NSE uses
                    "fiiBuy":  gf(row, "fii_buy_value",  "fiiBuy",  "FII_BUY"),
                    "fiiSell": gf(row, "fii_sell_value", "fiiSell", "FII_SELL"),
                    "fiiNet":  gf(row, "fii_net_value",  "fiiNet",  "FII_NET"),
                    "diiBuy":  gf(row, "dii_buy_value",  "diiBuy",  "DII_BUY"),
                    "diiSell": gf(row, "dii_sell_value", "diiSell", "DII_SELL"),
                    "diiNet":  gf(row, "dii_net_value",  "diiNet",  "DII_NET"),
                })
            save("fii_dii.json", {
                "data": rows,
                "updatedAt": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            traceback.print_exc()
            print(f"  WARN FII/DII: {e}")

    print("✅ All done!")

if __name__ == "__main__":
    main()
