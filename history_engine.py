#!/usr/bin/env python3
"""
Market Pulse — History Signal Engine  v2
==========================================
19 institutional-grade signals from stored 7-day history.
See inline comments for full documentation of each signal.
"""

import json, math, re, statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA = Path("data")
HIST = Path("data/history")
TS_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2}T\d{4})\.json$")

# ── LOADER ────────────────────────────────────────────────────────────────────

def _parse_ts(name):
    m = TS_PATTERN.search(name)
    if not m: return None
    try: return datetime.strptime(m.group(1), "%Y-%m-%dT%H%M").replace(tzinfo=timezone.utc)
    except ValueError: return None

def load_history(symbol, max_days=7):
    if not HIST.exists(): return []
    prefix = f"oc_{symbol.lower()}_"
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    rows = []
    for f in sorted(HIST.glob(f"{prefix}*.json")):
        ts = _parse_ts(f.name)
        if ts is None or ts < cutoff: continue
        try:
            raw    = json.loads(f.read_text())
            gex    = raw.get("gex",    {}) or {}
            dealer = raw.get("dealer", {}) or {}
            spot   = float(raw.get("spot", 0) or 0)
            if spot <= 0: continue
            rows.append({
                "t":            ts.isoformat(), "ts": ts,
                "spot":         spot,
                "pcr":          float(raw.get("pcr",       1)   or 1),
                "atmIV":        float(raw.get("atmIV",     0)   or 0),
                "maxPain":      float(raw.get("maxPain",   0)   or 0),
                "totalCeOI":    float(raw.get("totalCeOI", 0)   or 0),
                "totalPeOI":    float(raw.get("totalPeOI", 0)   or 0),
                "gexNet":       float(gex.get("netGEX",   0)    or 0),
                "gexRegime":    gex.get("regime", ""),
                "gexZeroGamma": float(gex.get("zeroGamma") or 0),
                "dealerDelta":  float(dealer.get("netDealerDelta", 0) or 0),
                "dealerStance": dealer.get("stance", "neutral"),
                "dealerFlip":   float(dealer.get("flipLevel") or 0),
            })
        except Exception: continue
    rows.sort(key=lambda r: r["ts"])
    return rows

def load_indices_history(max_days=7):
    if not HIST.exists(): return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    rows = []
    for f in sorted(HIST.glob("indices_*.json")):
        ts = _parse_ts(f.name)
        if ts is None or ts < cutoff: continue
        try:
            raw = json.loads(f.read_text())
            vix = float((raw.get("vix") or {}).get("last", 0) or 0)
            if vix > 0: rows.append({"ts": ts, "t": ts.isoformat(), "vix": vix})
        except Exception: continue
    rows.sort(key=lambda r: r["ts"])
    return rows

# ── UTILITY ───────────────────────────────────────────────────────────────────

def pct_rank(series, value):
    if not series: return 50.0
    return round(sum(1 for x in series if x < value) / len(series) * 100, 1)

def smean(s): return statistics.mean(s) if s else 0.0
def sstdev(s): return statistics.stdev(s) if len(s) >= 2 else 1.0
def zscore(val, series):
    if len(series) < 2: return 0.0
    return round((val - smean(series)) / (sstdev(series) or 1), 2)

def dedupe(signals, key_fn, n=10):
    seen, out = {}, []
    for s in sorted(signals, key=lambda x: -x.get("strength", 0)):
        k = key_fn(s)
        if k not in seen:
            seen[k] = True; out.append(s)
    return out[:n]

# ── SIGNAL 1: OI ABSORPTION ───────────────────────────────────────────────────
# Price moves but OI barely changes = large institution absorbing flow as
# counterparty. They are not adding positions — they're taking the other side.
# Classic dark pool / FPI footprint. Expect reversal after absorption exhausts.

def sig_oi_absorption(rows, window=6):
    out = []
    for i in range(window, len(rows)):
        c, p = rows[i], rows[i-window]
        if p["spot"] == 0: continue
        sc = abs(c["spot"]-p["spot"])/p["spot"]*100
        oi_p = p["totalCeOI"]+p["totalPeOI"]
        if oi_p == 0: continue
        oc = abs((c["totalCeOI"]+c["totalPeOI"])-oi_p)/oi_p*100
        if sc > 0.28 and oc < 0.42:
            d = "bull" if c["spot"] > p["spot"] else "bear"
            out.append({"type":"oi_absorption","t":c["t"],
                "strength":min(95,int(sc*75+(0.42-oc)*55)), "direction":d,
                "title":f"Institutional Absorption — {'Buy' if d=='bull' else 'Sell'} Flow",
                "desc":(f"Price {sc:.2f}% over {window*5}m but OI changed only {oc:.2f}%. "
                        f"Large institution absorbing {'buy' if d=='bull' else 'sell'} flow as counterparty. "
                        f"Expect {'reversal lower' if d=='bull' else 'reversal higher'}. Do not chase."),
                "tag":"ABSORPTION","spotChg":round(sc,2),"oiChg":round(oc,2)})
    return dedupe(out, lambda s: s["direction"]+s["t"][:13])

# ── SIGNAL 2: PCR DIVERGENCE ──────────────────────────────────────────────────
# Price/PCR decoupling = smart money positioning AGAINST price direction.
# Price UP + PCR DOWN = call writers building ceiling = institutional resistance.
# Price DOWN + PCR UP = put writers building floor = institutional support.

def sig_pcr_divergence(rows, window=12):
    out = []
    for i in range(window, len(rows)):
        c, p = rows[i], rows[i-window]
        if p["spot"] == 0: continue
        sp = (c["spot"]-p["spot"])/p["spot"]*100
        pc = c["pcr"]-p["pcr"]
        if sp > 0.22 and pc < -0.07:
            out.append({"type":"pcr_bearish_div","t":c["t"],
                "strength":min(92,int(abs(sp)*28+abs(pc)*160)), "direction":"bear",
                "title":"PCR Bearish Divergence — Institutions Capping Rally",
                "desc":(f"Price +{sp:.2f}% but PCR fell {pc:.3f}. Call writing accelerating into strength. "
                        f"Institutional resistance ceiling forming. Fade rally, sell CE spreads."),
                "tag":"PCR DIV","spotPct":round(sp,2),"pcrChg":round(pc,3),"pcrNow":round(c["pcr"],3)})
        elif sp < -0.22 and pc > 0.07:
            out.append({"type":"pcr_bullish_div","t":c["t"],
                "strength":min(92,int(abs(sp)*28+pc*160)), "direction":"bull",
                "title":"PCR Bullish Divergence — Institutions Buying Dip",
                "desc":(f"Price {sp:.2f}% but PCR rose +{pc:.3f}. Put writing accelerating into weakness. "
                        f"Institutional support floor forming. Buy dips, sell PE spreads."),
                "tag":"PCR DIV","spotPct":round(sp,2),"pcrChg":round(pc,3),"pcrNow":round(c["pcr"],3)})
    return dedupe(out, lambda s: s["type"]+s["t"][:13])

# ── SIGNAL 3: OI BUILDUP VELOCITY ─────────────────────────────────────────────
# Acceleration of total OI = new institutional positions being built aggressively.
# Deceleration = unwinding phase = uncertainty or expiry rollover.

def sig_oi_velocity(rows, window=6):
    if len(rows) < window*2+1: return {}
    total = lambda r: r["totalCeOI"]+r["totalPeOI"]
    rn = [total(r) for r in rows[-window:]]
    ea = [total(r) for r in rows[-window*2:-window]]
    vn = (rn[-1]-rn[0])/window if len(rn)>1 else 0
    vp = (ea[-1]-ea[0])/window if len(ea)>1 else 0
    oi = total(rows[-1])
    ce_pct = rows[-1]["totalCeOI"]/oi*100 if oi else 50
    return {
        "currentOI":round(oi), "velocity":round(vn,0),
        "acceleration":round(vn-vp,0),
        "cePct":round(ce_pct,1), "pePct":round(100-ce_pct,1),
        "verdict":(
            "OI accumulating fast — new institutional positions being built"
            if vn>8000 else
            "OI declining — positions unwinding, uncertainty rising"
            if vn<-8000 else
            "OI stable — consolidation, wait for breakout confirmation"
        )
    }

# ── SIGNAL 4: CE/PE OI SKEW SHIFT ────────────────────────────────────────────
# Rapid CE OI growth vs PE = call writers dominating = resistance being built.
# Rapid PE OI growth vs CE = put writers dominating = support being built.
# The DIRECTION of institutional writing is more informative than PCR alone.

def sig_skew_shift(rows, window=12):
    out = []
    for i in range(window, len(rows)):
        c, p = rows[i], rows[i-window]
        if p["totalCeOI"]==0 or p["totalPeOI"]==0: continue
        cg = (c["totalCeOI"]-p["totalCeOI"])/p["totalCeOI"]*100
        pg = (c["totalPeOI"]-p["totalPeOI"])/p["totalPeOI"]*100
        sn = c["totalCeOI"]/(c["totalPeOI"] or 1)
        sp_ = p["totalCeOI"]/(p["totalPeOI"] or 1)
        sc = sn-sp_
        if cg-pg > 5.0 and sc > 0.06:
            out.append({"type":"skew_bearish","t":c["t"],
                "strength":min(88,int((cg-pg)*5)), "direction":"bear",
                "title":"OI Skew Shift — Call Writers Dominating",
                "desc":(f"CE OI grew {cg:.1f}% vs PE OI {pg:.1f}% over {window*5}m. "
                        f"Institutions building resistance ceiling aggressively. Sell CE spreads, avoid longs."),
                "tag":"SKEW","ceGrowth":round(cg,1),"peGrowth":round(pg,1)})
        elif pg-cg > 5.0 and sc < -0.06:
            out.append({"type":"skew_bullish","t":c["t"],
                "strength":min(88,int((pg-cg)*5)), "direction":"bull",
                "title":"OI Skew Shift — Put Writers Dominating",
                "desc":(f"PE OI grew {pg:.1f}% vs CE OI {cg:.1f}% over {window*5}m. "
                        f"Institutions building support floor aggressively. Sell PE spreads, avoid shorts."),
                "tag":"SKEW","ceGrowth":round(cg,1),"peGrowth":round(pg,1)})
    return dedupe(out, lambda s: s["type"]+s["t"][:13])

# ── SIGNAL 5: IV RANK (7-day) ─────────────────────────────────────────────────
# NOT what IV is — WHERE it sits in its 7-day range.
# IV Rank >70: sell premium (options expensive). <30: buy options (cheap).

def compute_iv_rank(rows):
    ivs = [r["atmIV"] for r in rows if r["atmIV"]>0]
    if len(ivs)<5: return {}
    curr = ivs[-1]; mn=min(ivs); mx=max(ivs); rng=mx-mn
    rnk  = round((curr-mn)/rng*100,1) if rng>0 else 50.0
    return {
        "rank":rnk, "pctRank":pct_rank(ivs[:-1],curr),
        "current":round(curr,2), "min7d":round(mn,2), "max7d":round(mx,2),
        "mean7d":round(smean(ivs),2),
        "verdict":("SELL PREMIUM — IV expensive vs 7-day range" if rnk>70 else
                   "BUY OPTIONS — IV cheap vs 7-day range" if rnk<30 else "NEUTRAL — IV mid-range")
    }

# ── SIGNAL 6: IV ACCELERATION ─────────────────────────────────────────────────
# Second derivative of IV. Acceleration = institutions hedging NOW.
# Deceleration = vol regime normalising = sell premium window opening.

def sig_iv_acceleration(rows, window=18):
    out = []; ivs = [r["atmIV"] for r in rows]
    for i in range(window*2, len(rows)):
        rn = ivs[i-window:i]; ea = ivs[i-window*2:i-window]
        if not rn or not ea: continue
        accel = smean(rn)-smean(ea); curr_iv=ivs[i]
        iv_rnk = pct_rank(ivs[:i], curr_iv)
        if accel > 1.5:
            out.append({"type":"iv_accel","t":rows[i]["t"],
                "strength":min(90,int(accel*22)), "direction":"watch",
                "title":"IV Accelerating — Institutional Hedging Surge",
                "desc":(f"ATM IV +{accel:.1f}% vs prior {window*5}m avg (now {curr_iv:.1f}%, {iv_rnk:.0f}th pct). "
                        f"Institutions buying protection. Avoid naked premium sales. Use defined risk."),
                "tag":"IV REGIME","currIV":round(curr_iv,2),"ivPctRank":iv_rnk,"accel":round(accel,2)})
        elif accel < -1.5:
            out.append({"type":"iv_compress","t":rows[i]["t"],
                "strength":min(90,int(abs(accel)*22)), "direction":"neutral",
                "title":"IV Compressing — Premium Selling Window",
                "desc":(f"ATM IV {accel:.1f}% vs prior avg (now {curr_iv:.1f}%, {iv_rnk:.0f}th pct). "
                        f"Vol normalising. Sell iron condors, covered calls, straddles."),
                "tag":"IV REGIME","currIV":round(curr_iv,2),"ivPctRank":iv_rnk,"accel":round(accel,2)})
    return dedupe(out, lambda s: s["type"]+s["t"][:11], n=6)

# ── SIGNAL 7: VIX-ADJUSTED PCR ───────────────────────────────────────────────
# Raw PCR is inflated in high-vol regimes. Divide by sqrt(VIX/15) to normalise.
# Gives the TRUE institutional lean stripped of fear-driven noise.

def compute_vix_adj_pcr(oc_rows, idx_rows):
    if not oc_rows or not idx_rows: return {}
    merged = []
    for oc in oc_rows:
        best = min(idx_rows, key=lambda v: abs((v["ts"]-oc["ts"]).total_seconds()))
        if abs((best["ts"]-oc["ts"]).total_seconds()) < 600:
            adj = oc["pcr"]/math.sqrt(max(best["vix"],5)/15.0)
            merged.append({"pcr":oc["pcr"],"vix":best["vix"],"adj":adj})
    if len(merged)<5: return {}
    adjs = [m["adj"] for m in merged]
    curr = adjs[-1]; rnk = pct_rank(adjs[:-1],curr)
    return {
        "rawPCR":round(oc_rows[-1]["pcr"],3),
        "vix":round(idx_rows[-1]["vix"],2),
        "adjustedPCR":round(curr,3), "rank7d":rnk,
        "verdict":("Vol-adj PCR high — bullish even stripping out fear" if rnk>75 else
                   "Vol-adj PCR low — bearish bias dominates, vol-normalised" if rnk<25 else
                   "Vol-adj PCR neutral")
    }

# ── SIGNAL 8: IV MEAN-REVERSION SETUP ────────────────────────────────────────
# IV >1.5σ above mean: sell premium (IV will revert). <-1.5σ: buy options.
# Stronger signal than IV rank alone because it uses distribution shape.

def sig_iv_mr(rows):
    ivs=[r["atmIV"] for r in rows if r["atmIV"]>0]
    if len(ivs)<20: return []
    out = []
    for i in range(20, len(rows)):
        wivs = ivs[max(0,i-40):i]
        if len(wivs)<10: continue
        mu=smean(wivs); sd=sstdev(wivs); z=(ivs[i]-mu)/(sd or 1)
        if abs(z)<1.5: continue
        if z>1.5:
            out.append({"type":"iv_mr_sell","t":rows[i]["t"],
                "strength":min(88,int(z*25)), "direction":"neutral",
                "title":f"IV Mean-Reversion — Sell Setup (z={z:.2f}σ)",
                "desc":(f"ATM IV {ivs[i]:.1f}% is {z:.2f}σ above mean ({mu:.1f}%). "
                        f"IV historically reverts sharply. Sell straddles / iron condors."),
                "tag":"IV MR","iv":round(ivs[i],2),"zScore":round(z,2),"ivMean":round(mu,2)})
        else:
            out.append({"type":"iv_mr_buy","t":rows[i]["t"],
                "strength":min(88,int(abs(z)*25)), "direction":"neutral",
                "title":f"IV Mean-Reversion — Buy Setup (z={z:.2f}σ)",
                "desc":(f"ATM IV {ivs[i]:.1f}% is {abs(z):.2f}σ BELOW mean ({mu:.1f}%). "
                        f"Options cheap vs history. Buy straddles / long gamma before vol expands."),
                "tag":"IV MR","iv":round(ivs[i],2),"zScore":round(z,2),"ivMean":round(mu,2)})
    return dedupe(out, lambda s: s["type"]+s["t"][:11], n=4)

# ── SIGNAL 9: GEX REGIME TRANSITION ──────────────────────────────────────────
# When net GEX crosses zero the entire dealer hedging regime flips.
# Long→Short Gamma: dealers amplify every move, vol expands, breakouts run.
# Short→Long Gamma: dealers suppress every move, vol compresses, mean-revert.

def sig_gex_transitions(rows):
    out = []
    for i in range(1, len(rows)):
        pp=rows[i-1]["gexNet"]>=0; cp=rows[i]["gexNet"]>=0
        if pp==cp: continue
        new = "LONG" if cp else "SHORT"
        out.append({"type":"gex_flip","t":rows[i]["t"],"strength":95,
            "direction":"bull" if cp else "bear",
            "title":f"GEX Regime Flip → {new} GAMMA",
            "desc":(f"Net GEX: {rows[i-1]['gexNet']:.0f} → {rows[i]['gexNet']:.0f} Cr. "
                   +(f"LONG gamma: dealers suppress vol, buy dips, sell rips. Range-bound. Sell straddles."
                     if cp else
                     f"SHORT gamma: dealers amplify every move. Vol expansion. Buy options, avoid short premium.")),
            "tag":"GEX FLIP","fromGEX":round(rows[i-1]["gexNet"],1),"toGEX":round(rows[i]["gexNet"],1)})
    return out[-10:]

# ── SIGNAL 10: GEX TREND ─────────────────────────────────────────────────────

def compute_gex_trend(rows, window=12):
    if len(rows)<window*2: return {}
    gexes=[r["gexNet"] for r in rows]
    rn=smean(gexes[-window:]); pv=smean(gexes[-window*2:-window])
    trend=rn-pv; curr=gexes[-1]
    return {
        "current":round(curr,1),"trend":round(trend,1),"recentAvg":round(rn,1),
        "pctRank":pct_rank(gexes[:-1],curr),"regime":rows[-1]["gexRegime"],
        "direction":"rising" if trend>10 else "falling" if trend<-10 else "flat",
        "verdict":("GEX rising → vol calming, range-bound action expected" if trend>10 else
                   "GEX falling → vol expanding, directional breakout building" if trend<-10 else
                   "GEX stable — current vol regime likely to persist")
    }

# ── SIGNAL 11: DEALER DELTA EXHAUSTION ───────────────────────────────────────
# When dealer aggregate delta hits a historical extreme, they MUST reduce risk.
# Creates forced flows that reverse price. Highest-precision reversal signal.

def sig_dealer_exhaustion(rows, lookback=36):
    out=[]; deltas=[r["dealerDelta"] for r in rows]
    for i in range(lookback, len(rows)):
        wd=deltas[i-lookback:i]; curr=deltas[i]; rnk=pct_rank(wd,curr)
        if rnk>=90:
            out.append({"type":"dealer_exh_long","t":rows[i]["t"],
                "strength":min(95,int(rnk)), "direction":"bear",
                "title":"Dealer Exhaustion — Forced Selling Imminent",
                "desc":(f"Dealer delta {curr/1e6:.2f}M at {rnk:.0f}th pct of {lookback}-snapshot range. "
                        f"Maximally long → must sell into rallies. Fade strength."),
                "tag":"DEALER EXH","delta":round(curr),"pctRank":round(rnk,1)})
        elif rnk<=10:
            out.append({"type":"dealer_exh_short","t":rows[i]["t"],
                "strength":min(95,int(100-rnk)), "direction":"bull",
                "title":"Dealer Exhaustion — Forced Buying Imminent",
                "desc":(f"Dealer delta {curr/1e6:.2f}M at {rnk:.0f}th pct of {lookback}-snapshot range. "
                        f"Maximally short → must buy every dip. Buy weakness."),
                "tag":"DEALER EXH","delta":round(curr),"pctRank":round(rnk,1)})
    seen,result=[],[]
    for s in sorted(out, key=lambda x: x["t"], reverse=True):
        if s["type"] not in seen: seen.append(s["type"]); result.append(s)
    return result

# ── SIGNAL 12: DEALER STANCE FLIP ────────────────────────────────────────────

def sig_dealer_stance_flip(rows):
    out=[]
    for i in range(1, len(rows)):
        ps=rows[i-1]["dealerStance"]; cs=rows[i]["dealerStance"]
        if ps==cs or not ps or not cs: continue
        out.append({"type":"dealer_stance_flip","t":rows[i]["t"],"strength":88,
            "direction":"bull" if cs=="net_short" else "bear",
            "title":f"Dealer Stance Flip: {ps.replace('_',' ').upper()} → {cs.replace('_',' ').upper()}",
            "desc":(f"Dealer positioning reversed at ₹{rows[i]['spot']:,.0f}. "
                   +("Now NET SHORT — must buy every dip. Built-in bid under market. Buy pullbacks."
                     if cs=="net_short" else
                     "Now NET LONG — must sell every rally. Built-in offer above market. Sell strength.")),
            "tag":"DEALER FLIP","fromStance":ps,"toStance":cs,"spot":rows[i]["spot"]})
    return out[-8:]

# ── SIGNAL 13: DEALER FLIP LEVEL WATCH ───────────────────────────────────────
# Spot approaching dealer flip level = acceleration risk.
# When spot crosses this level, entire dealer hedging direction reverses.

def sig_dealer_flip_proximity(rows, window=6):
    out=[]
    for i in range(window, len(rows)):
        c=rows[i]; flip=c["dealerFlip"]; spot=c["spot"]
        if flip<=0 or spot<=0: continue
        dist=(abs(flip-spot)/spot)*100
        p=rows[i-window]; pf=p["dealerFlip"]; ps=p["spot"]
        if pf<=0 or ps<=0: continue
        pdist=(abs(pf-ps)/ps)*100
        if dist<0.8 and dist<pdist:
            d="bull" if flip>spot else "bear"
            out.append({"type":"dealer_flip_watch","t":c["t"],
                "strength":min(90,int((1-dist/0.8)*90)), "direction":d,
                "title":f"Dealer Flip Level Approaching — ₹{flip:,.0f} ({dist:.2f}% away)",
                "desc":(f"Spot ₹{spot:,.0f} approaching dealer flip ₹{flip:,.0f} ({dist:.2f}%). "
                        f"Crossing triggers forced {'buy' if d=='bull' else 'sell'} cascade. Expect sharp move."),
                "tag":"DEALER FLIP","flipLevel":flip,"distPct":round(dist,2)})
    return dedupe(out, lambda s: s["t"][:13])

# ── SIGNAL 14: MAX PAIN DRIFT & VELOCITY ─────────────────────────────────────
# Converging toward spot = expiry pin strengthening. Diverging = breakout likely.

def compute_mp_drift(rows, window=12):
    if len(rows)<window+2: return {}
    c=rows[-1]; p=rows[-(window+1)]
    mn=c["maxPain"]; mp=p["maxPain"]
    if mp==0: return {}
    sn=c["spot"]; sp=p["spot"]
    dist_n=mn-sn; dist_p=mp-sp
    conv=abs(dist_n)<abs(dist_p)
    mh=[r["maxPain"] for r in rows[-window:] if r["maxPain"]>0]
    vel=(mh[-1]-mh[0])/len(mh) if len(mh)>1 else 0
    return {
        "current":round(mn),"drift":round(mn-mp,0),"velocity":round(vel,2),
        "distFromSpot":round(dist_n,0),"converging":conv,
        "verdict":(
            f"Max pain converging (Δ={dist_n:+.0f}) — pin risk HIGH, range-bound expiry likely"
            if conv and abs(dist_n)<100 else
            f"Max pain diverging (Δ={dist_n:+.0f}) — breakout favoured before expiry"
            if not conv else
            f"Max pain ₹{mn:,.0f}, spot ₹{sn:,.0f} (Δ={dist_n:+.0f})")
    }

# ── SIGNAL 15: SPOT vs MAX PAIN Z-SCORE ──────────────────────────────────────
# How stretched is spot from max pain in standard deviation terms over 7-day history?

def compute_spot_z(rows):
    dists=[r["spot"]-r["maxPain"] for r in rows if r["maxPain"]>0]
    if len(dists)<10: return {}
    curr=dists[-1]; z=zscore(curr,dists[:-1])
    return {
        "zScore":z,"distNow":round(curr,0),
        "mean":round(smean(dists),0),"stdev":round(sstdev(dists),0),
        "verdict":(
            f"Spot very stretched ABOVE max pain (z={z:.2f}σ) — reversion likely"
            if z>2 else
            f"Spot very stretched BELOW max pain (z={z:.2f}σ) — bounce likely"
            if z<-2 else
            f"Spot within normal range of max pain (z={z:.2f}σ)")
    }

# ── SIGNAL 16: PCR TREND REVERSAL ────────────────────────────────────────────
# PCR moving-average crossover = momentum regime change in INSTITUTIONAL positioning.
# Unlike price MAs, this tells you about conviction, not retail price chasing.

def sig_pcr_reversal(rows, fast=6, slow=18):
    pcrs=[r["pcr"] for r in rows]
    # Need at least slow+fast+1 rows to safely compute prev window
    if len(pcrs) < slow+fast+1: return []
    out=[]
    for i in range(slow+fast, len(rows)):
        fn=smean(pcrs[i-fast:i]);       sn=smean(pcrs[i-slow:i])
        fp=smean(pcrs[i-fast-1:i-1]);   sp=smean(pcrs[i-slow-1:i-1])
        if fp<=sp and fn>sn:
            out.append({"type":"pcr_bull_xover","t":rows[i]["t"],"strength":75,"direction":"bull",
                "title":"PCR Momentum Flip — Bullish Crossover",
                "desc":(f"PCR fast MA ({fn:.3f}) crossed above slow MA ({sn:.3f}). "
                        f"Put writing accelerating. Institutions building floor. Follow-through buying likely."),
                "tag":"PCR TREND","maFast":round(fn,3),"maSlow":round(sn,3)})
        elif fp>=sp and fn<sn:
            out.append({"type":"pcr_bear_xover","t":rows[i]["t"],"strength":75,"direction":"bear",
                "title":"PCR Momentum Flip — Bearish Crossover",
                "desc":(f"PCR fast MA ({fn:.3f}) crossed below slow MA ({sn:.3f}). "
                        f"Call writing accelerating. Institutions building ceiling. Sell strength."),
                "tag":"PCR TREND","maFast":round(fn,3),"maSlow":round(sn,3)})
    return dedupe(out, lambda s: s["type"]+s["t"][:10], n=6)

# ── SIGNAL 17: MOMENTUM vs OI CONFIRMATION ────────────────────────────────────
# Price trend + OI confirmation = real institutional move.
# Price trend + OI declining = short-covering / stop-hunt trap.

def sig_momentum_oi_conf(rows, window=12):
    out=[]
    for i in range(window, len(rows)):
        c,p=rows[i],rows[i-window]
        if p["spot"]==0: continue
        sp=(c["spot"]-p["spot"])/p["spot"]*100
        ce_c=c["totalCeOI"]-p["totalCeOI"]; pe_c=c["totalPeOI"]-p["totalPeOI"]
        oi_p=p["totalCeOI"]+p["totalPeOI"]
        if oi_p==0: continue
        oi_chg=(ce_c+pe_c)/oi_p*100
        if sp>0.25 and pe_c>0 and p["totalPeOI"]>0 and pe_c/p["totalPeOI"]>0.01:
            out.append({"type":"bull_conf","t":c["t"],
                "strength":min(85,int(abs(sp)*25+pe_c/(p["totalPeOI"] or 1)*500)),
                "direction":"bull",
                "title":"Bullish Momentum Confirmed by OI",
                "desc":(f"Price +{sp:.2f}% AND put writers adding simultaneously. "
                        f"Genuine institutional floor — not just price drift. Add on pullbacks."),
                "tag":"CONFIRMED","spotPct":round(sp,2)})
        elif sp<-0.25 and ce_c>0 and p["totalCeOI"]>0 and ce_c/p["totalCeOI"]>0.01:
            out.append({"type":"bear_conf","t":c["t"],
                "strength":min(85,int(abs(sp)*25+ce_c/(p["totalCeOI"] or 1)*500)),
                "direction":"bear",
                "title":"Bearish Momentum Confirmed by OI",
                "desc":(f"Price {sp:.2f}% AND call writers adding simultaneously. "
                        f"Genuine institutional ceiling — not stop-hunt. Sell strength."),
                "tag":"CONFIRMED","spotPct":round(sp,2)})
        elif abs(sp)>0.30 and oi_chg<-0.5:
            td="bear" if sp>0 else "bull"
            out.append({"type":"trap","t":c["t"],
                "strength":min(80,int(abs(sp)*20+abs(oi_chg)*30)),
                "direction":td,
                "title":f"{'Bull' if sp>0 else 'Bear'} Trap — OI Not Confirming Move",
                "desc":(f"Price {sp:.2f}% but total OI dropped {oi_chg:.2f}%. "
                        f"{'Short-covering, not real buying' if sp>0 else 'Long liquidation, not real selling'}. "
                        f"Expect reversal. Do not chase."),
                "tag":"TRAP","spotPct":round(sp,2),"oiChgPct":round(oi_chg,2)})
    return dedupe(out, lambda s: s["type"]+s["t"][:13])

# ── SIGNAL 18: ROLLING REGIME SCORE ──────────────────────────────────────────
# Per-snapshot bull/bear mini-score rolled over a window. Filters noise.
# Compare today's rolling score vs prior window for momentum.

def compute_rolling_regime(rows, window=12):
    if len(rows)<window*2: return {}
    def score(r):
        s=50+min(20,max(-20,(r["pcr"]-1.0)*15))
        s+= 5 if r["gexNet"]>0 else -5
        s+= 5 if r["dealerStance"]=="net_short" else (-5 if r["dealerStance"]=="net_long" else 0)
        if r["maxPain"]>0: s+=min(10,max(-10,(r["maxPain"]-r["spot"])/r["spot"]*200))
        return max(0,min(100,s))
    scores=[score(r) for r in rows]
    rn=smean(scores[-window:]); pv=smean(scores[-window*2:-window]); tr=rn-pv
    return {
        "score":round(rn,1),"prevScore":round(pv,1),"trend":round(tr,1),
        "series":[round(s,1) for s in scores[-50:]],
        "verdict":(
            "Rolling regime BULLISH and strengthening"  if rn>55 and tr>3 else
            "Rolling regime BULLISH but losing momentum" if rn>55 else
            "Rolling regime BEARISH and weakening"      if rn<45 and tr<-3 else
            "Rolling regime BEARISH but recovering"     if rn<45 else
            "Rolling regime NEUTRAL — conflicting signals"
        )
    }

# ── SIGNAL 19: COMPOSITE INSTITUTIONAL SCORE ─────────────────────────────────
# Weighted 0–100 combining all static indicators. >65 = inst. bull. <35 = inst. bear.

def compute_composite(iv_rank, gex_trend, mp_drift, rolling, rows, spot_z):
    scores, factors = [], []
    if iv_rank.get("rank") is not None:
        s=100-iv_rank["rank"]; scores.append((s,0.12))
        factors.append({"name":"IV Rank","score":round(s),"weight":12,"note":f"{iv_rank['rank']:.0f}% — {iv_rank.get('verdict','')[:35]}"})
    if rows:
        pcrs=[r["pcr"] for r in rows]; s=min(100,max(0,(pcrs[-1]-0.5)/1.5*100))
        scores.append((s,0.18)); factors.append({"name":"PCR Level","score":round(s),"weight":18,"note":f"PCR {pcrs[-1]:.3f}"})
    if gex_trend:
        s=70 if gex_trend.get("regime")=="long_gamma" else 30
        s+=12 if gex_trend.get("direction")=="rising" else (-12 if gex_trend.get("direction")=="falling" else 0)
        s=max(0,min(100,s)); scores.append((s,0.20))
        factors.append({"name":"GEX Regime","score":round(s),"weight":20,"note":gex_trend.get("verdict","")[:40]})
    if rows:
        st=rows[-1]["dealerStance"]; s=35 if st=="net_long" else 65 if st=="net_short" else 50
        scores.append((s,0.20)); factors.append({"name":"Dealer Stance","score":round(s),"weight":20,"note":st.replace("_"," ")})
    if mp_drift:
        s=55 if mp_drift.get("converging") else 45
        s+=8 if mp_drift.get("distFromSpot",0)>0 else -8; s=max(0,min(100,s))
        scores.append((s,0.10)); factors.append({"name":"Max Pain Pull","score":round(s),"weight":10,"note":mp_drift.get("verdict","")[:40]})
    if rolling:
        s=rolling.get("score",50); scores.append((s,0.12))
        factors.append({"name":"Rolling Regime","score":round(s),"weight":12,"note":rolling.get("verdict","")[:40]})
    if spot_z and spot_z.get("zScore") is not None:
        s=max(0,min(100,50-spot_z["zScore"]*14)); scores.append((s,0.08))
        factors.append({"name":"Mean Reversion","score":round(s),"weight":8,"note":f"z={spot_z['zScore']:.2f}σ"})
    if not scores: return {"score":50,"verdict":"Insufficient data","factors":[]}
    tw=sum(w for _,w in scores); comp=sum(s*w for s,w in scores)/tw
    return {
        "score":round(comp,1),
        "verdict":("STRONGLY BULLISH — multiple institutional signals aligned" if comp>72 else
                   "BULLISH — institutional positioning favours upside"         if comp>58 else
                   "SLIGHTLY BULLISH — mild institutional preference for longs" if comp>52 else
                   "NEUTRAL — conflicting signals, reduce size"                 if comp>=48 else
                   "SLIGHTLY BEARISH — mild institutional preference for shorts"if comp>42 else
                   "BEARISH — institutional positioning favours downside"       if comp>28 else
                   "STRONGLY BEARISH — multiple institutional signals aligned"),
        "factors":factors, "bullish":comp>52, "bearish":comp<48
    }

# ── CHART SERIES ─────────────────────────────────────────────────────────────

def build_chart(rows, max_pts=120):
    if not rows: return {}
    step=max(1,len(rows)//max_pts); s=rows[::step][-max_pts:]
    def lbl(i,r):
        d=r["t"][:10]; prev=s[i-1]["t"][:10] if i>0 else ""
        return r["t"][11:16]+(f" {d[5:]}" if d!=prev else "")
    return {
        "labels":     [lbl(i,r) for i,r in enumerate(s)],
        "timestamps": [r["t"]                        for r in s],
        "spot":       [r["spot"]                      for r in s],
        "pcr":        [round(r["pcr"],3)              for r in s],
        "atmIV":      [round(r["atmIV"],2)            for r in s],
        "gexNet":     [round(r["gexNet"],1)           for r in s],
        "dealerDelta":[round(r["dealerDelta"]/1e6,3) for r in s],
        "maxPain":    [r["maxPain"]                   for r in s],
        "totalCeOI":  [round(r["totalCeOI"])          for r in s],
        "totalPeOI":  [round(r["totalPeOI"])          for r in s],
    }

# ── MASTER RUN ────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"HISTORY ENGINE v2 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    if not HIST.exists() or not any(HIST.glob("*.json")):
        print("  data/history/ empty — history accumulates after first 5-10 fetches.")
        return True
    idx_rows=load_indices_history()
    print(f"  VIX snapshots: {len(idx_rows)}")
    output={"updatedAt":datetime.now(timezone.utc).isoformat(),"symbols":{},"dataPoints":0}
    total=0
    for sym in ["NIFTY","BANKNIFTY","FINNIFTY"]:
        rows=load_history(sym); n=len(rows)
        print(f"\n  [{sym}] {n} snapshots")
        if n<5:
            output["symbols"][sym]={"error":"insufficient_history","count":n}; continue
        total+=n
        # Run all 19 algorithms
        ab=sig_oi_absorption(rows)
        pd=sig_pcr_divergence(rows)
        ov=sig_oi_velocity(rows)
        sk=sig_skew_shift(rows)
        ir=compute_iv_rank(rows)
        ia=sig_iv_acceleration(rows)
        va=compute_vix_adj_pcr(rows,idx_rows)
        im=sig_iv_mr(rows)
        gt=sig_gex_transitions(rows)
        gtr=compute_gex_trend(rows)
        de=sig_dealer_exhaustion(rows)
        df=sig_dealer_stance_flip(rows)
        dp=sig_dealer_flip_proximity(rows)
        md=compute_mp_drift(rows)
        sz=compute_spot_z(rows)
        pr=sig_pcr_reversal(rows)
        mc=sig_momentum_oi_conf(rows)
        rr=compute_rolling_regime(rows)
        co=compute_composite(ir,gtr,md,rr,rows,sz)
        ch=build_chart(rows)
        events=ab+pd+sk+ia+im+gt+de+df+dp+pr+mc
        for e in events: e.pop("ts",None)
        events.sort(key=lambda x:(-x.get("strength",0),x.get("t","")))
        print(f"    ab={len(ab)} pcr_div={len(pd)} skew={len(sk)} iv={len(ia)+len(im)} "
              f"gex={len(gt)} dlr={len(de)+len(df)+len(dp)} pcr_rev={len(pr)} mom={len(mc)}")
        print(f"    IV rank={ir.get('rank')}% | composite={co.get('score')} | {rr.get('verdict','')[:40]}")
        output["symbols"][sym]={
            "count":n,"oldest":rows[0]["t"],"newest":rows[-1]["t"],
            "ivRank":ir,"gexTrend":gtr,"maxPainDrift":md,
            "oiVelocity":ov,"spotMPZScore":sz,"vixAdjPCR":va,
            "rollingRegime":rr,"composite":co,
            "events":events[:30],"gexTransitions":gt[-5:],
            "chartData":ch
        }
    output["dataPoints"]=total
    out=DATA/"history_signals.json"
    out.write_text(json.dumps(output,default=str,indent=2))
    print(f"\n  SAVED: data/history_signals.json ({out.stat().st_size/1024:.1f} KB, {total} pts)")
    return True

if __name__=="__main__":
    import sys; sys.exit(0 if run() else 1)
