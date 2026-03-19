#!/usr/bin/env python3
"""
Market Pulse — Intelligence Engine  v1.0
==========================================
12-module trade-ready system built on top of stored history.
Runs after history_engine.py and outputs data/intelligence.json

MODULES:
  1.  Market Structure         — swing highs/lows, BOS, CHOCH, trend/range
  2.  Time Context             — session tagging, opening range, HTF bias
  3.  Liquidity & Trap         — equal H/L, stop hunts, false breakouts
  4.  Volatility Intelligence  — compression, expansion, IV regime
  5.  Pattern Memory           — setup scoring from historical outcomes
  6.  Probability Engine       — confidence score per setup (0–1)
  7.  Feature Engineering      — candle body, wick ratio, momentum bursts
  8.  Market State Detection   — trend/range/volatile/compression classifier
  9.  Event Trigger System     — condition chains, no-noise activation
  10. Signal Fusion            — price + options combined signals
  11. Risk Intelligence        — position sizing, avoid-trade zones
  12. Trade Decision Engine    — entry/SL/TP/confidence/reason output

Output: data/intelligence.json
"""

import json
import math
import statistics
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

DATA = Path("data")
HIST = Path("data/history")
JOURNAL = DATA / "trade_journal.json"
TS_PAT  = re.compile(r"_(\d{4}-\d{2}-\d{2}T\d{4})\.json$")

# ════════════════════════════════════════════════════════════════════════════
# LOADER  — same pattern as history_engine
# ════════════════════════════════════════════════════════════════════════════

def _ts(name):
    m = TS_PAT.search(name)
    if not m: return None
    try: return datetime.strptime(m.group(1), "%Y-%m-%dT%H%M").replace(tzinfo=timezone.utc)
    except: return None

def load_snapshots(symbol: str, days: int = 7) -> list:
    if not HIST.exists(): return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = []
    for f in sorted(HIST.glob(f"oc_{symbol.lower()}_*.json")):
        ts = _ts(f.name)
        if ts is None or ts < cutoff: continue
        try:
            r = json.loads(f.read_text())
            gex    = r.get("gex",    {}) or {}
            dealer = r.get("dealer", {}) or {}
            spot   = float(r.get("spot", 0) or 0)
            if spot <= 0: continue
            rows.append({
                "t": ts, "tstr": ts.isoformat(),
                "spot":       spot,
                "open":       float(r.get("open",  spot) or spot),
                "high":       float(r.get("high",  spot) or spot),
                "low":        float(r.get("low",   spot) or spot),
                "pcr":        float(r.get("pcr",   1)    or 1),
                "atmIV":      float(r.get("atmIV", 0)    or 0),
                "maxPain":    float(r.get("maxPain",0)   or 0),
                "ceOI":       float(r.get("totalCeOI",0) or 0),
                "peOI":       float(r.get("totalPeOI",0) or 0),
                "gexNet":     float(gex.get("netGEX",0)  or 0),
                "gexRegime":  gex.get("regime",""),
                "zeroGamma":  float(gex.get("zeroGamma") or 0),
                "dealerDelta":float(dealer.get("netDealerDelta",0) or 0),
                "dealerStance":dealer.get("stance","neutral"),
                "dealerFlip": float(dealer.get("flipLevel") or 0),
                "strikes":    r.get("strikes", []),
            })
        except: continue
    rows.sort(key=lambda x: x["t"])
    return rows

def load_journal() -> list:
    if not JOURNAL.exists(): return []
    try:
        raw = json.loads(JOURNAL.read_text())
        # Support {"trades":[...]} dict format or plain list
        if isinstance(raw, list): return raw
        if isinstance(raw, dict): return raw.get("trades", [])
        return []
    except: return []

def save_journal(records: list):
    JOURNAL.write_text(json.dumps(records, default=str, indent=2))

# ════════════════════════════════════════════════════════════════════════════
# MODULE 1 — MARKET STRUCTURE ENGINE
# Derives swing highs/lows, BOS, CHOCH, trend/range purely from spot series.
# Uses 5-min spot as a price series. Pivot detection via n-bar lookback.
# ════════════════════════════════════════════════════════════════════════════

def detect_structure(rows: list, pivot_n: int = 3) -> dict:
    """
    Pivot detection: a swing high is a bar where price is higher than
    pivot_n bars on each side. Same for swing lows.
    BOS  = price breaks above last swing high (bullish) or below swing low (bearish)
    CHOCH = Break of Structure in the OPPOSITE direction of prevailing trend.
    """
    if len(rows) < pivot_n * 2 + 2:
        return {"error": "insufficient_data"}

    spots = [r["spot"] for r in rows]
    highs, lows = [], []

    for i in range(pivot_n, len(spots) - pivot_n):
        window = spots[i - pivot_n: i + pivot_n + 1]
        if spots[i] == max(window):
            highs.append({"idx": i, "price": spots[i], "t": rows[i]["tstr"]})
        if spots[i] == min(window):
            lows.append({"idx": i, "price": spots[i], "t": rows[i]["tstr"]})

    # Current structure
    last_high = highs[-1]["price"] if highs else max(spots[-20:])
    last_low  = lows[-1]["price"]  if lows  else min(spots[-20:])
    prev_high = highs[-2]["price"] if len(highs) >= 2 else last_high
    prev_low  = lows[-2]["price"]  if len(lows)  >= 2 else last_low
    curr      = spots[-1]

    # Higher highs/lows = bullish structure, lower = bearish
    hh = last_high > prev_high
    hl = last_low  > prev_low
    lh = last_high < prev_high
    ll = last_low  < prev_low

    if hh and hl:   structure = "bullish_trend"
    elif lh and ll: structure = "bearish_trend"
    else:           structure = "ranging"

    # BOS detection
    bos = None
    if highs and curr > last_high:  bos = "bullish_bos"
    elif lows and curr < last_low:  bos = "bearish_bos"

    # CHOCH: break in OPPOSITE direction of current structure
    choch = None
    if structure == "bullish_trend" and bos == "bearish_bos":
        choch = "bearish_choch"    # first sign of reversal
    elif structure == "bearish_trend" and bos == "bullish_bos":
        choch = "bullish_choch"    # first sign of reversal

    # Range detection: price oscillating between last pivot high/low
    range_size = (last_high - last_low) / last_low * 100 if last_low > 0 else 0
    is_ranging = structure == "ranging" or range_size < 0.5

    # Momentum: last 5 bars slope
    recent_spots = spots[-5:]
    momentum_pct  = (recent_spots[-1] - recent_spots[0]) / recent_spots[0] * 100 if recent_spots[0] > 0 else 0
    momentum = "strong_bull" if momentum_pct > 0.3 else "strong_bear" if momentum_pct < -0.3 else "weak"

    return {
        "structure":   structure,
        "bos":         bos,
        "choch":       choch,
        "lastHigh":    round(last_high, 2),
        "lastLow":     round(last_low,  2),
        "prevHigh":    round(prev_high, 2),
        "prevLow":     round(prev_low,  2),
        "swingHighs":  [{"price": h["price"], "t": h["t"]} for h in highs[-5:]],
        "swingLows":   [{"price": l["price"], "t": l["t"]} for l in lows[-5:]],
        "isRanging":   is_ranging,
        "rangeSize":   round(range_size, 2),
        "momentum":    momentum,
        "momentumPct": round(momentum_pct, 3),
        "currentSpot": curr,
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 2 — TIME CONTEXT ENGINE
# Session tagging (IST), opening range, higher timeframe bias.
# ════════════════════════════════════════════════════════════════════════════

IST = timezone(timedelta(hours=5, minutes=30))

def get_session(t: datetime) -> str:
    ist = t.astimezone(IST)
    h, m = ist.hour, ist.minute
    mins = h * 60 + m
    if mins < 9*60+15:   return "pre_market"
    if mins < 9*60+45:   return "opening"       # first 30 min — highest volume
    if mins < 11*60:     return "morning"        # trend usually set here
    if mins < 13*60+30:  return "midday"         # consolidation / low vol
    if mins < 14*60+30:  return "afternoon"      # re-activation
    if mins < 15*60+30:  return "closing"        # expiry/closing moves
    return "post_market"

def compute_time_context(rows: list) -> dict:
    if not rows: return {}
    curr_t    = rows[-1]["t"]
    session   = get_session(curr_t)
    ist_now   = curr_t.astimezone(IST)

    # Opening range = first 30-min high/low (first 6 snapshots)
    today_rows = [r for r in rows if r["t"].astimezone(IST).date() == ist_now.date()]
    opening_rows = today_rows[:6] if today_rows else []
    or_high = max(r["spot"] for r in opening_rows) if opening_rows else None
    or_low  = min(r["spot"] for r in opening_rows) if opening_rows else None

    curr_spot = rows[-1]["spot"]
    or_bias   = None
    if or_high and or_low:
        if curr_spot > or_high:   or_bias = "above_opening_range"
        elif curr_spot < or_low:  or_bias = "below_opening_range"
        else:                     or_bias = "inside_opening_range"

    # HTF bias: use last 48 snapshots (~4 hours) as the "higher timeframe"
    htf_rows = rows[-48:] if len(rows) >= 48 else rows
    htf_spots = [r["spot"] for r in htf_rows]
    htf_change = (htf_spots[-1] - htf_spots[0]) / htf_spots[0] * 100 if htf_spots[0] > 0 else 0
    htf_bias  = "bullish" if htf_change > 0.25 else "bearish" if htf_change < -0.25 else "neutral"

    # Previous day high/low
    yesterday = (ist_now - timedelta(days=1)).date()
    prev_rows = [r for r in rows if r["t"].astimezone(IST).date() == yesterday]
    prev_high = max(r["spot"] for r in prev_rows) if prev_rows else None
    prev_low  = min(r["spot"] for r in prev_rows) if prev_rows else None

    # Edge score by session (best trading times)
    session_edge = {
        "opening": 0.85, "morning": 0.75, "afternoon": 0.70,
        "closing": 0.65, "midday": 0.40, "pre_market": 0.20, "post_market": 0.10
    }

    return {
        "session":         session,
        "sessionEdge":     session_edge.get(session, 0.5),
        "htfBias":         htf_bias,
        "htfChange":       round(htf_change, 3),
        "openingRangeHigh": round(or_high, 2) if or_high else None,
        "openingRangeLow":  round(or_low,  2) if or_low  else None,
        "openingRangeBias": or_bias,
        "prevDayHigh":     round(prev_high, 2) if prev_high else None,
        "prevDayLow":      round(prev_low,  2) if prev_low  else None,
        "istTime":         ist_now.strftime("%H:%M IST"),
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 3 — LIQUIDITY & TRAP DETECTION
# Equal highs/lows, stop hunts, false breakouts, liquidity sweeps.
# In options data: OI at strikes acts as liquidity. Heavy OI = stop cluster.
# ════════════════════════════════════════════════════════════════════════════

def detect_liquidity(rows: list, tolerance_pct: float = 0.08) -> dict:
    if len(rows) < 10: return {}
    spots = [r["spot"] for r in rows]
    curr  = spots[-1]

    # Equal highs: two swing highs within tolerance = liquidity magnet
    eq_highs, eq_lows = [], []
    window = spots[-40:]
    for i in range(2, len(window)-2):
        for j in range(i+1, len(window)-1):
            hi, hj = window[i], window[j]
            if hi > window[i-1] and hi > window[i+1] and hj > window[j-1] and hj > window[j+1]:
                if abs(hi - hj) / hi * 100 < tolerance_pct:
                    eq_highs.append(round((hi+hj)/2, 2))
            li, lj = window[i], window[j]
            if li < window[i-1] and li < window[i+1] and lj < window[j-1] and lj < window[j+1]:
                if abs(li - lj) / li * 100 < tolerance_pct:
                    eq_lows.append(round((li+lj)/2, 2))

    # Dedupe
    eq_highs = list(dict.fromkeys(eq_highs))[:3]
    eq_lows  = list(dict.fromkeys(eq_lows))[:3]

    # Stop hunt: sharp spike beyond recent high/low followed by rejection
    stop_hunt = None
    if len(spots) >= 5:
        prev_max = max(spots[-6:-1])
        prev_min = min(spots[-6:-1])
        last_bar = spots[-1]
        prev_bar = spots[-2]
        spike_up   = prev_bar > prev_max and last_bar < prev_max   # wick up rejected
        spike_down = prev_bar < prev_min and last_bar > prev_min   # wick down rejected
        if spike_up:   stop_hunt = {"direction": "sell_stop_hunt",  "level": round(prev_max, 2)}
        elif spike_down: stop_hunt = {"direction": "buy_stop_hunt", "level": round(prev_min, 2)}

    # False breakout: break beyond level then reversal within 3 bars
    false_breakout = None
    recent_high = max(spots[-20:-3]) if len(spots) >= 20 else None
    recent_low  = min(spots[-20:-3]) if len(spots) >= 20 else None
    if recent_high and recent_low:
        broke_high = any(s > recent_high for s in spots[-3:]) and spots[-1] < recent_high
        broke_low  = any(s < recent_low  for s in spots[-3:]) and spots[-1] > recent_low
        if broke_high: false_breakout = {"type": "bull_trap",  "level": round(recent_high, 2)}
        elif broke_low:false_breakout = {"type": "bear_trap",  "level": round(recent_low,  2)}

    # OI-based liquidity: heavy OI strikes = stop clusters (use latest snapshot strikes)
    oi_levels = []
    latest_strikes = rows[-1].get("strikes", []) if rows else []
    if latest_strikes:
        spot_now = curr
        near = [s for s in latest_strikes if abs(s.get("strike",0) - spot_now)/spot_now < 0.03]
        for s in near:
            total = s.get("ceOI", 0) + s.get("peOI", 0)
            if total > 50000:
                oi_levels.append({"strike": s["strike"], "oi": round(total), "type": "oi_cluster"})
        oi_levels.sort(key=lambda x: -x["oi"])

    # Proximity to liquidity levels
    nearest_eq_high = min(eq_highs, key=lambda x: abs(x-curr)) if eq_highs else None
    nearest_eq_low  = min(eq_lows,  key=lambda x: abs(x-curr)) if eq_lows  else None

    return {
        "equalHighs":      eq_highs,
        "equalLows":       eq_lows,
        "stopHunt":        stop_hunt,
        "falseBreakout":   false_breakout,
        "oiClusters":      oi_levels[:5],
        "nearestEqHigh":   nearest_eq_high,
        "nearestEqLow":    nearest_eq_low,
        "liquiditySweep":  stop_hunt is not None,
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 4 — VOLATILITY INTELLIGENCE
# IV compression, expansion, ATR-equivalent from spot history.
# ════════════════════════════════════════════════════════════════════════════

def compute_vol_intelligence(rows: list) -> dict:
    if len(rows) < 12: return {}
    spots = [r["spot"] for r in rows]
    ivs   = [r["atmIV"] for r in rows if r["atmIV"] > 0]

    # True range from 5-min spot series
    tr_list = [abs(spots[i] - spots[i-1]) for i in range(1, len(spots))]
    atr_5   = statistics.mean(tr_list[-12:])  if len(tr_list) >= 12 else statistics.mean(tr_list)
    atr_long= statistics.mean(tr_list[-48:])  if len(tr_list) >= 48 else atr_5
    atr_ratio = atr_5 / atr_long if atr_long > 0 else 1.0

    # Compression: ATR below 50% of long avg
    compression = atr_ratio < 0.6
    expansion   = atr_ratio > 1.5

    # IV percentile rank
    iv_now  = ivs[-1] if ivs else 0
    iv_rank = round(sum(1 for x in ivs[:-1] if x < iv_now) / max(len(ivs)-1, 1) * 100, 1)
    iv_mean = statistics.mean(ivs) if ivs else 0
    iv_trend = "rising" if len(ivs) >= 6 and statistics.mean(ivs[-3:]) > statistics.mean(ivs[-6:-3]) else \
               "falling" if len(ivs) >= 6 and statistics.mean(ivs[-3:]) < statistics.mean(ivs[-6:-3]) else "flat"

    # Expected move (based on ATM IV)
    spot_now = spots[-1]
    dte_proxy = 1  # 1-day equivalent for intraday
    expected_move = spot_now * (iv_now / 100) * math.sqrt(dte_proxy / 365) if iv_now > 0 else 0

    # Volatility state
    if compression:     vol_state = "compression"
    elif expansion:     vol_state = "expansion"
    elif iv_rank > 70:  vol_state = "elevated"
    elif iv_rank < 30:  vol_state = "subdued"
    else:               vol_state = "normal"

    return {
        "volState":     vol_state,
        "atr5":         round(atr_5, 2),
        "atrLong":      round(atr_long, 2),
        "atrRatio":     round(atr_ratio, 3),
        "compression":  compression,
        "expansion":    expansion,
        "ivNow":        round(iv_now, 2),
        "ivRank":       iv_rank,
        "ivTrend":      iv_trend,
        "ivMean":       round(iv_mean, 2),
        "expectedMove": round(expected_move, 2),
        "verdict": (
            "Vol COMPRESSION → breakout imminent, buy options"  if compression else
            "Vol EXPANSION   → trend in motion, ride with stops" if expansion   else
            "Vol ELEVATED    → sell premium, avoid option buys"  if iv_rank>70  else
            "Vol SUBDUED     → options cheap, buy before catalyst"if iv_rank<30  else
            "Vol NORMAL      → standard conditions"
        ),
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 5 — PATTERN MEMORY ENGINE
# Score setups based on historical outcomes stored in trade_journal.json.
# Each time a signal fires, we look up its past win rate and avg RR.
# ════════════════════════════════════════════════════════════════════════════

def build_pattern_memory(journal: list) -> dict:
    """
    Compute per-setup statistics from journal.
    Returns dict keyed by setup_type → {win_rate, avg_rr, count, best_session}
    """
    memory = {}
    for rec in journal:
        setup   = rec.get("setup_type", rec.get("setup", "unknown"))
        result  = rec.get("result", "")
        rr      = rec.get("rr_achieved", 0)
        session = rec.get("session", "")
        if setup not in memory:
            memory[setup] = {"wins":0,"total":0,"rr_sum":0,"sessions":{}}
        memory[setup]["total"] += 1
        memory[setup]["rr_sum"] += rr
        if result == "win":
            memory[setup]["wins"] += 1
        s = memory[setup]["sessions"]
        s[session] = s.get(session, 0) + (1 if result == "win" else 0)

    stats = {}
    for setup, d in memory.items():
        if d["total"] > 0:
            wr   = d["wins"] / d["total"]
            avg_rr = d["rr_sum"] / d["total"]
            best_ses = max(d["sessions"], key=d["sessions"].get) if d["sessions"] else "unknown"
            stats[setup] = {
                "winRate":     round(wr, 3),
                "avgRR":       round(avg_rr, 2),
                "count":       d["total"],
                "bestSession": best_ses,
                "edge":        round(wr * avg_rr, 3),   # expected value proxy
            }
    return stats

def get_setup_score(setup_type: str, memory: dict, default: float = 0.5) -> float:
    """Return historical win rate for a setup type, or default if no history."""
    if setup_type in memory:
        return min(0.95, max(0.1, memory[setup_type]["winRate"]))
    return default


# ════════════════════════════════════════════════════════════════════════════
# MODULE 6 — PROBABILITY ENGINE
# Weighted confidence score: options data + structure + time + history.
# ════════════════════════════════════════════════════════════════════════════

def compute_confidence(
    structure:   dict,
    time_ctx:    dict,
    liquidity:   dict,
    vol_intel:   dict,
    features:    dict,
    setup_type:  str,
    memory:      dict,
) -> float:
    """
    Confidence = weighted sum of all module scores, clamped to [0.05, 0.95].
    Weights tuned to options-driven NIFTY intraday.
    """
    score = 0.0

    # Options signal quality (0.30 weight)
    gex    = features.get("gexScore",    0.5)
    dealer = features.get("dealerScore", 0.5)
    pcr    = features.get("pcrScore",    0.5)
    options_score = gex * 0.40 + dealer * 0.35 + pcr * 0.25
    score += options_score * 0.30

    # Price structure quality (0.25 weight)
    struct_score = 0.5
    if structure.get("choch"):         struct_score = 0.85
    elif structure.get("bos"):         struct_score = 0.70
    elif structure.get("isRanging"):   struct_score = 0.40
    mom = structure.get("momentum", "weak")
    if mom in ("strong_bull", "strong_bear"): struct_score = min(0.95, struct_score + 0.10)
    score += struct_score * 0.25

    # Liquidity quality (0.15 weight)
    liq_score = 0.5
    if liquidity.get("liquiditySweep"):  liq_score = 0.80
    if liquidity.get("falseBreakout"):   liq_score = 0.75
    score += liq_score * 0.15

    # Time context (0.10 weight)
    score += time_ctx.get("sessionEdge", 0.5) * 0.10

    # Volatility match (0.10 weight) — compression before breakout is best
    vol_score = 0.5
    vs = vol_intel.get("volState", "")
    if vs == "compression":  vol_score = 0.80   # breakout setup
    elif vs == "expansion":  vol_score = 0.75   # momentum setup
    elif vs == "elevated":   vol_score = 0.60   # sell premium ok
    elif vs == "subdued":    vol_score = 0.55
    score += vol_score * 0.10

    # Historical pattern score (0.10 weight)
    hist_score = get_setup_score(setup_type, memory)
    score += hist_score * 0.10

    return round(min(0.95, max(0.05, score)), 3)


# ════════════════════════════════════════════════════════════════════════════
# MODULE 7 — FEATURE ENGINEERING
# Derive tradeable features from raw spot + options series.
# ════════════════════════════════════════════════════════════════════════════

def extract_features(rows: list) -> dict:
    if len(rows) < 6: return {}
    spots = [r["spot"] for r in rows]
    ivs   = [r["atmIV"] for r in rows if r["atmIV"] > 0]

    # Candle-equivalent: body size = |close - open| over 5 bars
    bar_moves = [abs(spots[i] - spots[i-1]) / spots[i-1] * 100 for i in range(1, len(spots))]
    avg_move  = statistics.mean(bar_moves) if bar_moves else 0
    last_move = bar_moves[-1] if bar_moves else 0
    body_ratio = last_move / avg_move if avg_move > 0 else 1.0

    # Wick ratio: max range vs close move (proxy from spot extremes over window)
    w = spots[-5:]
    wick_range = (max(w) - min(w)) / min(w) * 100 if min(w) > 0 else 0
    close_move = abs(w[-1] - w[0]) / w[0] * 100 if w[0] > 0 else 0
    wick_ratio = wick_range / close_move if close_move > 0 else 2.0  # high = indecision

    # Momentum burst: 3-bar expansion
    burst_3 = abs(spots[-1] - spots[-4]) / spots[-4] * 100 if len(spots) >= 4 and spots[-4] > 0 else 0
    burst_5 = abs(spots[-1] - spots[-6]) / spots[-6] * 100 if len(spots) >= 6 and spots[-6] > 0 else 0

    # GEX/dealer features
    gex_now   = rows[-1]["gexNet"]
    dealer_d  = rows[-1]["dealerDelta"]
    flip_lvl  = rows[-1]["dealerFlip"]
    spot_now  = rows[-1]["spot"]
    flip_dist = abs(flip_lvl - spot_now) / spot_now * 100 if flip_lvl > 0 and spot_now > 0 else 99

    # GEX score: short gamma + falling trend = most bullish for breakout
    gex_score = 0.3 if rows[-1]["gexRegime"] == "long_gamma" else 0.7
    gex_series = [r["gexNet"] for r in rows[-12:]]
    if len(gex_series) >= 6:
        gex_trend = statistics.mean(gex_series[-3:]) - statistics.mean(gex_series[-6:-3])
        gex_score += -0.15 if gex_trend > 0 else 0.15

    # Dealer score
    dealer_score = 0.4 if rows[-1]["dealerStance"] == "net_long" else 0.7 if rows[-1]["dealerStance"] == "net_short" else 0.5
    if flip_dist < 0.5: dealer_score = min(0.9, dealer_score + 0.2)  # near flip = high score

    # PCR score
    pcr = rows[-1]["pcr"]
    pcr_score = min(0.9, max(0.1, (pcr - 0.4) / 1.6))  # 0.4→0.0, 2.0→1.0

    # IV skew: CE IV vs PE IV divergence (if strike data available)
    strikes = rows[-1].get("strikes", [])
    atm_s = min(strikes, key=lambda x: abs(x.get("strike",0) - spot_now)) if strikes else {}
    ce_iv = atm_s.get("ceIV", 0)
    pe_iv = atm_s.get("peIV", 0)
    skew  = pe_iv - ce_iv  # positive = put skew = bearish fear premium

    return {
        "bodyRatio":    round(body_ratio, 3),
        "wickRatio":    round(wick_ratio, 3),
        "burst3":       round(burst_3, 3),
        "burst5":       round(burst_5, 3),
        "avgBarMove":   round(avg_move, 4),
        "lastBarMove":  round(last_move, 4),
        "gexScore":     round(min(1, max(0, gex_score)), 3),
        "dealerScore":  round(dealer_score, 3),
        "pcrScore":     round(pcr_score, 3),
        "flipDist":     round(flip_dist, 3),
        "nearFlip":     flip_dist < 0.8,
        "ivSkew":       round(skew, 2) if ce_iv and pe_iv else None,
        "skewBias":     "bearish" if skew > 2 else "bullish" if skew < -2 else "neutral",
        "strongMomentum": burst_3 > 0.4,
        "indecision":   wick_ratio > 2.5,
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 8 — MARKET STATE DETECTION
# Classify into: trend | range | volatile | compression
# Uses options + price features jointly.
# ════════════════════════════════════════════════════════════════════════════

def detect_market_state(structure: dict, vol_intel: dict, rows: list) -> dict:
    if not rows: return {"state": "unknown"}

    gex_regime = rows[-1]["gexRegime"]
    vs         = vol_intel.get("volState", "normal")
    is_ranging = structure.get("isRanging", True)
    bos        = structure.get("bos")
    momentum   = structure.get("momentum", "weak")

    # State decision tree
    if vs == "compression":
        state = "compression"
        desc  = "Market coiling before breakout — wait for trigger, then trade aggressively"
        allowed = ["breakout_long", "breakout_short"]
        avoided = ["mean_reversion"]

    elif gex_regime == "short_gamma" and vs == "expansion":
        state = "volatile_trend"
        desc  = "Short gamma + vol expansion — dealers amplifying every move. Ride the direction with tight stops."
        allowed = ["breakout_long", "breakout_short", "momentum"]
        avoided = ["mean_reversion", "range_fade"]

    elif gex_regime == "long_gamma" and is_ranging:
        state = "range"
        desc  = "Long gamma + ranging — dealers suppressing vol. Sell premium at boundaries, fade extremes."
        allowed = ["mean_reversion", "range_fade", "sell_straddle"]
        avoided = ["breakout_long", "breakout_short"]

    elif bos and momentum in ("strong_bull", "strong_bear"):
        state = "trending"
        desc  = "Structure breakout + strong momentum — trend in motion. Follow BOS direction."
        allowed = ["breakout_long", "breakout_short", "momentum"]
        avoided = ["mean_reversion"]

    elif vs == "elevated" and gex_regime == "long_gamma":
        state = "iv_elevated_range"
        desc  = "High IV + long gamma — best environment to sell premium. Iron condors, straddle sells."
        allowed = ["sell_straddle", "iron_condor", "range_fade"]
        avoided = ["breakout_long", "breakout_short"]

    else:
        state = "neutral"
        desc  = "No dominant regime — mixed signals. Reduce size, wait for clarity."
        allowed = []
        avoided = []

    return {
        "state":          state,
        "description":    desc,
        "allowedSetups":  allowed,
        "avoidedSetups":  avoided,
        "gexRegime":      gex_regime,
        "volState":       vs,
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 9 — EVENT TRIGGER SYSTEM
# Condition-chain activation. Only fires when ≥3 conditions align.
# Eliminates noise from single-condition signals.
# ════════════════════════════════════════════════════════════════════════════

TRIGGERS = [
    {
        "name":    "institutional_breakout",
        "desc":    "Short gamma + liquidity sweep + BOS — institutional breakout with dealer amplification",
        "setup":   "breakout",
        "conditions": [
            lambda r, s, l, v, f: r[-1]["gexRegime"] == "short_gamma",
            lambda r, s, l, v, f: s.get("bos") is not None,
            lambda r, s, l, v, f: l.get("liquiditySweep", False),
        ],
        "min_match": 3,
    },
    {
        "name":    "choch_reversal",
        "desc":    "CHOCH + liquidity sweep + PCR divergence — institutional reversal setup",
        "setup":   "reversal",
        "conditions": [
            lambda r, s, l, v, f: s.get("choch") is not None,
            lambda r, s, l, v, f: l.get("liquiditySweep", False) or l.get("falseBreakout") is not None,
            lambda r, s, l, v, f: r[-1]["pcr"] > 1.2 or r[-1]["pcr"] < 0.7,
        ],
        "min_match": 2,
    },
    {
        "name":    "compression_breakout",
        "desc":    "Vol compression + OI buildup + near flip level — spring-loaded move incoming",
        "setup":   "breakout",
        "conditions": [
            lambda r, s, l, v, f: v.get("compression", False),
            lambda r, s, l, v, f: f.get("nearFlip", False),
            lambda r, s, l, v, f: (r[-1]["ceOI"] + r[-1]["peOI"]) > (r[-6]["ceOI"] + r[-6]["peOI"]) * 1.02 if len(r) >= 6 else False,
        ],
        "min_match": 2,
    },
    {
        "name":    "dealer_exhaustion_reversal",
        "desc":    "Dealer at delta extreme + CHOCH — forced reversal with structure confirmation",
        "setup":   "reversal",
        "conditions": [
            lambda r, s, l, v, f: abs(r[-1]["dealerDelta"]) > 5000000,
            lambda r, s, l, v, f: s.get("choch") is not None,
            lambda r, s, l, v, f: f.get("strongMomentum", False),
        ],
        "min_match": 2,
    },
    {
        "name":    "range_premium_sell",
        "desc":    "Long gamma + range + IV elevated — textbook premium selling setup",
        "setup":   "sell_premium",
        "conditions": [
            lambda r, s, l, v, f: r[-1]["gexRegime"] == "long_gamma",
            lambda r, s, l, v, f: s.get("isRanging", False),
            lambda r, s, l, v, f: v.get("ivRank", 0) > 65,
        ],
        "min_match": 3,
    },
    {
        "name":    "opening_range_breakout",
        "desc":    "Opening session + above/below OR + momentum — ORB setup",
        "setup":   "breakout",
        "conditions": [
            lambda r, s, l, v, f: get_session(r[-1]["t"]) in ("opening", "morning"),
            lambda r, s, l, v, f: s.get("bos") is not None,
            lambda r, s, l, v, f: f.get("burst3", 0) > 0.3,
        ],
        "min_match": 2,
    },
    {
        "name":    "no_trade_conflict",
        "desc":    "Conflicting signals — GEX and dealer pointing opposite directions",
        "setup":   "no_trade",
        "conditions": [
            lambda r, s, l, v, f: r[-1]["gexRegime"] == "long_gamma" and r[-1]["dealerStance"] == "net_short",
            lambda r, s, l, v, f: f.get("indecision", False),
            lambda r, s, l, v, f: v.get("volState") == "normal",
        ],
        "min_match": 2,
    },
]

def evaluate_triggers(rows, structure, liquidity, vol_intel, features) -> list:
    if not rows: return []
    active = []
    for trig in TRIGGERS:
        matches = sum(
            1 for cond in trig["conditions"]
            if _safe_eval(cond, rows, structure, liquidity, vol_intel, features)
        )
        fired = matches >= trig["min_match"]
        if fired:
            active.append({
                "name":       trig["name"],
                "desc":       trig["desc"],
                "setup":      trig["setup"],
                "matches":    matches,
                "total_conds":len(trig["conditions"]),
                "strength":   round(matches / len(trig["conditions"]), 2),
            })
    active.sort(key=lambda x: -x["strength"])
    return active

def _safe_eval(cond, *args):
    try: return bool(cond(*args))
    except: return False


# ════════════════════════════════════════════════════════════════════════════
# MODULE 10 — SIGNAL FUSION (Price + Options Combined)
# Merges structure signals with options signals into composite setups.
# ════════════════════════════════════════════════════════════════════════════

def fuse_signals(structure, time_ctx, liquidity, vol_intel, features, triggers, market_state) -> list:
    fused = []

    spot = features.get("gexScore", 0.5)  # reuse as proxy

    for trig in triggers:
        if trig["setup"] == "no_trade":
            fused.append({
                "type":    "no_trade",
                "title":   "NO TRADE — Conflicting Signals",
                "desc":    trig["desc"],
                "direction":"neutral",
                "confidence": 0.0,
                "reason": [trig["desc"]],
                "action": "STAND ASIDE",
            })
            continue

        # Build reason list
        reasons = [trig["desc"]]
        if structure.get("bos"):
            reasons.append(f"BOS: {structure['bos'].replace('_',' ')}")
        if structure.get("choch"):
            reasons.append(f"CHOCH: {structure['choch'].replace('_',' ')}")
        if liquidity.get("liquiditySweep"):
            sh = liquidity.get("stopHunt",{})
            reasons.append(f"Liquidity sweep: {sh.get('direction','').replace('_',' ')}")
        if features.get("nearFlip"):
            reasons.append(f"Near dealer flip level (dist={features.get('flipDist',0):.2f}%)")
        reasons.append(f"Session: {time_ctx.get('session','?')} | HTF: {time_ctx.get('htfBias','?')}")
        reasons.append(f"Vol state: {vol_intel.get('volState','?')} | IV rank: {vol_intel.get('ivRank',0):.0f}%")

        # Direction
        direction = "bull"
        if structure.get("choch") == "bearish_choch": direction = "bear"
        elif structure.get("bos")  == "bearish_bos":  direction = "bear"
        elif structure.get("bos")  == "bullish_bos":  direction = "bull"
        elif time_ctx.get("htfBias") == "bearish":    direction = "bear"

        # Setup type for action
        setup = trig["setup"]
        if setup in ("breakout", "momentum"):
            action = "BUY" if direction == "bull" else "SELL"
        elif setup == "reversal":
            action = "BUY" if direction == "bull" else "SELL"
        elif setup == "sell_premium":
            action = "SELL STRADDLE / IRON CONDOR"
        else:
            action = "WAIT"

        fused.append({
            "type":      trig["name"],
            "title":     trig["name"].replace("_"," ").upper(),
            "desc":      trig["desc"],
            "setup":     setup,
            "direction": direction,
            "action":    action,
            "reason":    reasons,
            "triggerStrength": trig["strength"],
            "marketState": market_state.get("state","?"),
        })

    return fused


# ════════════════════════════════════════════════════════════════════════════
# MODULE 11 — RISK INTELLIGENCE
# Position sizing, avoid-trade filters, max daily exposure.
# ════════════════════════════════════════════════════════════════════════════

def compute_risk(
    rows:        list,
    vol_intel:   dict,
    confidence:  float,
    capital:     float = 500000,  # ₹5 lakh default
    risk_pct:    float = 0.01,    # 1% per trade
) -> dict:
    if not rows: return {}
    spot   = rows[-1]["spot"]
    atr    = vol_intel.get("atr5", spot * 0.003)

    # Dynamic SL = 1.5× ATR (scales with actual volatility)
    sl_distance = round(atr * 1.5, 2)

    # Confidence-adjusted risk (reduce size when uncertain)
    adj_risk_pct = risk_pct * min(1.0, confidence / 0.65)
    risk_amount  = capital * adj_risk_pct

    # Position size in NIFTY lots (lot size = 25 for NIFTY)
    lot_size     = 25
    point_value  = lot_size  # ₹1 move × 25 = ₹25/lot
    lots         = risk_amount / (sl_distance * point_value) if sl_distance > 0 else 0
    lots         = max(0, min(round(lots), 10))  # cap at 10 lots

    # R:R targets
    tp1 = round(atr * 2.0, 2)   # 1:1.3 R:R
    tp2 = round(atr * 3.5, 2)   # 1:2.3 R:R

    # Avoid-trade conditions
    avoid = []
    if confidence < 0.50:           avoid.append("confidence_too_low")
    if vol_intel.get("volState") == "elevated" and confidence < 0.65:
        avoid.append("high_iv_low_confidence")
    if rows[-1]["gexRegime"] == "short_gamma" and rows[-1]["dealerStance"] == "net_long":
        avoid.append("gex_dealer_conflict")
    session = get_session(rows[-1]["t"])
    if session == "midday":         avoid.append("midday_low_edge")

    should_trade = len(avoid) == 0 and confidence >= 0.50

    return {
        "shouldTrade":    should_trade,
        "avoidReasons":   avoid,
        "confidence":     confidence,
        "lots":           lots,
        "riskAmount":     round(risk_amount, 0),
        "slDistance":     sl_distance,
        "tp1Distance":    tp1,
        "tp2Distance":    tp2,
        "adjRiskPct":     round(adj_risk_pct * 100, 2),
        "note": f"Risk ₹{risk_amount:,.0f} → {lots} lot(s) | SL {sl_distance:.0f}pts | TP1 {tp1:.0f}pts | TP2 {tp2:.0f}pts",
    }


# ════════════════════════════════════════════════════════════════════════════
# MODULE 12 — TRADE DECISION ENGINE
# Final output: entry/SL/TP/confidence/reason — one clear, actionable decision.
# ════════════════════════════════════════════════════════════════════════════

def generate_decision(
    rows:         list,
    fused_signals:list,
    market_state: dict,
    risk:         dict,
    confidence:   float,
    structure:    dict,
    time_ctx:     dict,
    vol_intel:    dict,
    features:     dict,
    memory:       dict,
) -> dict:
    if not rows or not fused_signals:
        return {
            "action":     "NO_TRADE",
            "reason":     ["No active triggers"],
            "confidence": 0.0,
        }

    spot = rows[-1]["spot"]

    # Pick best non-no_trade signal
    tradeable = [s for s in fused_signals if s.get("action") not in ("STAND ASIDE", "WAIT")]
    if not tradeable:
        return {
            "action":     "NO_TRADE",
            "reason":     [s.get("desc","") for s in fused_signals],
            "confidence": 0.0,
        }

    # If risk engine says avoid, output NO_TRADE
    if not risk.get("shouldTrade", True):
        return {
            "action":      "NO_TRADE",
            "reason":      risk.get("avoidReasons", []),
            "confidence":  round(confidence, 3),
            "avoidDetail": risk.get("avoidReasons"),
        }

    best = tradeable[0]
    action   = best["action"]
    direction= best["direction"]
    sl_dist  = risk.get("slDistance", spot * 0.005)
    tp1_dist = risk.get("tp1Distance", sl_dist * 1.5)
    tp2_dist = risk.get("tp2Distance", sl_dist * 2.5)

    if direction == "bull":
        entry   = round(spot, 2)
        sl      = round(spot - sl_dist, 2)
        target1 = round(spot + tp1_dist, 2)
        target2 = round(spot + tp2_dist, 2)
    else:
        entry   = round(spot, 2)
        sl      = round(spot + sl_dist, 2)
        target1 = round(spot - tp1_dist, 2)
        target2 = round(spot - tp2_dist, 2)

    rr1 = round(tp1_dist / sl_dist, 2) if sl_dist > 0 else 0
    rr2 = round(tp2_dist / sl_dist, 2) if sl_dist > 0 else 0

    # Invalid if: market reverses against SL
    invalid_if = (
        f"Close below ₹{sl:,.0f}" if direction == "bull"
        else f"Close above ₹{sl:,.0f}"
    )

    # Setup type from best trigger
    setup_type = best.get("setup", "unknown")

    # Memory lookup
    hist_stats = memory.get(setup_type, {})
    hist_note  = (
        f"Historical: {hist_stats['count']} trades, {hist_stats['winRate']*100:.0f}% WR, {hist_stats['avgRR']:.1f}R avg"
        if hist_stats else "No historical data yet — first occurrence"
    )

    return {
        "action":       action,
        "setup":        setup_type,
        "direction":    direction,
        "confidence":   round(confidence, 3),
        "entry":        entry,
        "stopLoss":     sl,
        "target1":      target1,
        "target2":      target2,
        "rr1":          rr1,
        "rr2":          rr2,
        "lots":         risk.get("lots", 1),
        "riskAmount":   risk.get("riskAmount", 0),
        "invalidIf":    invalid_if,
        "reason":       best.get("reason", []),
        "historyNote":  hist_note,
        "session":      time_ctx.get("session", "?"),
        "htfBias":      time_ctx.get("htfBias", "?"),
        "marketState":  market_state.get("state", "?"),
        "context": {
            "gexRegime":    rows[-1]["gexRegime"],
            "dealerStance": rows[-1]["dealerStance"],
            "pcr":          round(rows[-1]["pcr"], 3),
            "ivNow":        vol_intel.get("ivNow", 0),
            "ivRank":       vol_intel.get("ivRank", 0),
            "volState":     vol_intel.get("volState", ""),
            "structure":    structure.get("structure", ""),
            "momentum":     structure.get("momentum", ""),
        },
    }


# ════════════════════════════════════════════════════════════════════════════
# JOURNALING — log every decision for feedback loop
# ════════════════════════════════════════════════════════════════════════════

def append_to_journal(decision: dict, sym: str):
    """Append this decision to trade_journal.json for pattern memory building."""
    if decision.get("action") == "NO_TRADE":
        return   # don't log no-trades (too many)
    journal = load_journal()
    entry = {
        "id":         f"{sym}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}",
        "symbol":     sym,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "setup_type": decision.get("setup", "unknown"),
        "action":     decision.get("action"),
        "direction":  decision.get("direction"),
        "confidence": decision.get("confidence"),
        "entry":      decision.get("entry"),
        "stopLoss":   decision.get("stopLoss"),
        "target1":    decision.get("target1"),
        "session":    decision.get("session"),
        "htfBias":    decision.get("htfBias"),
        "marketState":decision.get("marketState"),
        "result":     "pending",   # filled by outcome tracker later
        "rr_achieved":0,           # filled by outcome tracker later
    }
    journal.append(entry)
    # Keep last 500 entries
    journal = journal[-500:]
    save_journal(journal)


# ════════════════════════════════════════════════════════════════════════════
# OUTCOME TRACKER — fills in results for pending journal entries
# ════════════════════════════════════════════════════════════════════════════

def update_outcomes(rows: list, sym: str):
    """
    For any 'pending' journal entries for this symbol, check if TP or SL was hit
    using subsequent snapshots in history.
    """
    journal = load_journal()
    updated = False
    spots   = {r["tstr"]: r["spot"] for r in rows}
    spot_series = [(r["tstr"], r["spot"]) for r in rows]

    for rec in journal:
        if rec.get("symbol") != sym: continue
        if rec.get("result") != "pending": continue
        entry_time = rec.get("timestamp", "")
        entry_price = rec.get("entry", 0)
        sl    = rec.get("stopLoss", 0)
        tp    = rec.get("target1", 0)
        direction = rec.get("direction", "bull")

        # Find spots AFTER entry time
        future = [(t, p) for t, p in spot_series if t > entry_time]
        if not future: continue

        for _, price in future:
            if direction == "bull":
                if sl > 0 and price <= sl:
                    rec["result"] = "loss"
                    rec["rr_achieved"] = round(-(entry_price - price) / (entry_price - sl), 2) if sl < entry_price else -1
                    updated = True; break
                if tp > 0 and price >= tp:
                    rec["result"] = "win"
                    rec["rr_achieved"] = round((price - entry_price) / (entry_price - sl), 2) if sl < entry_price else 1.5
                    updated = True; break
            else:
                if sl > 0 and price >= sl:
                    rec["result"] = "loss"
                    rec["rr_achieved"] = -1
                    updated = True; break
                if tp > 0 and price <= tp:
                    rec["result"] = "win"
                    rec["rr_achieved"] = round((entry_price - price) / (sl - entry_price), 2) if sl > entry_price else 1.5
                    updated = True; break

    if updated:
        save_journal(journal)
    return updated


# ════════════════════════════════════════════════════════════════════════════
# STRATEGY PERFORMANCE TABLE (from journal)
# ════════════════════════════════════════════════════════════════════════════

def build_performance_table(memory: dict) -> list:
    rows = []
    for setup, stats in sorted(memory.items(), key=lambda x: -x[1].get("edge", 0)):
        rows.append({
            "setup":       setup.replace("_"," ").title(),
            "count":       stats["count"],
            "winRate":     f"{stats['winRate']*100:.0f}%",
            "avgRR":       f"{stats['avgRR']:.2f}",
            "edge":        f"{stats['edge']:.3f}",
            "bestSession": stats["bestSession"],
        })
    return rows


# ════════════════════════════════════════════════════════════════════════════
# MASTER RUN
# ════════════════════════════════════════════════════════════════════════════

def run() -> bool:
    print(f"\n{'='*65}")
    print(f"INTELLIGENCE ENGINE v1 — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*65}")

    if not HIST.exists() or not any(HIST.glob("*.json")):
        print("  data/history/ empty — intelligence needs at least 1 day of history.")
        return True

    journal  = load_journal()
    memory   = build_pattern_memory(journal)
    print(f"  Journal: {len(journal)} trades | Memory: {len(memory)} setup types")

    output = {
        "updatedAt":   datetime.now(timezone.utc).isoformat(),
        "symbols":     {},
        "performance": build_performance_table(memory),
    }

    for sym in ["NIFTY", "BANKNIFTY", "FINNIFTY"]:
        rows = load_snapshots(sym)
        n    = len(rows)
        print(f"\n  [{sym}] {n} snapshots")
        if n < 12:
            output["symbols"][sym] = {"error": "need_12_snapshots", "count": n}
            continue

        # Update outcomes for pending journal entries
        update_outcomes(rows, sym)

        # Run all 12 modules
        structure    = detect_structure(rows)
        time_ctx     = compute_time_context(rows)
        liquidity    = detect_liquidity(rows)
        vol_intel    = compute_vol_intelligence(rows)
        features     = extract_features(rows)
        market_state = detect_market_state(structure, vol_intel, rows)
        triggers     = evaluate_triggers(rows, structure, liquidity, vol_intel, features)
        fused        = fuse_signals(structure, time_ctx, liquidity, vol_intel, features, triggers, market_state)

        # Pick best trigger for confidence
        best_trig    = triggers[0]["name"] if triggers else "none"
        confidence   = compute_confidence(structure, time_ctx, liquidity, vol_intel, features, best_trig, memory)
        risk         = compute_risk(rows, vol_intel, confidence)
        decision     = generate_decision(rows, fused, market_state, risk, confidence, structure, time_ctx, vol_intel, features, memory)

        # Log to journal
        append_to_journal({**decision, "setup": best_trig}, sym)

        print(f"    State: {market_state['state']} | Triggers: {len(triggers)} | Confidence: {confidence}")
        print(f"    Decision: {decision.get('action')} | Setup: {decision.get('setup')} | Lots: {decision.get('lots')}")
        print(f"    Structure: {structure.get('structure')} | BOS: {structure.get('bos')} | CHOCH: {structure.get('choch')}")
        print(f"    Session: {time_ctx.get('session')} | HTF: {time_ctx.get('htfBias')} | Vol: {vol_intel.get('volState')}")

        output["symbols"][sym] = {
            "count":        n,
            # Module outputs
            "structure":    structure,
            "timeContext":  time_ctx,
            "liquidity":    liquidity,
            "volIntelligence": vol_intel,
            "features":     features,
            "marketState":  market_state,
            "triggers":     triggers,
            "fusedSignals": fused,
            "confidence":   confidence,
            "risk":         risk,
            "decision":     decision,
        }

    output["journal"]     = journal[-20:]           # last 20 trades in output
    output["performance"] = build_performance_table(memory)

    out = DATA / "intelligence.json"
    out.write_text(json.dumps(output, default=str, indent=2))
    kb = out.stat().st_size / 1024
    print(f"\n  SAVED: data/intelligence.json ({kb:.1f} KB)")
    return True


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
