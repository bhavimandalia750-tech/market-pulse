#!/usr/bin/env python3
"""
Market Pulse — Signal Engine
=============================
Reads option chain JSON files from data/ directory and outputs:
  • GEX (Gamma Exposure) per strike + aggregate
  • Dealer positioning model (long/short gamma)
  • Automatic option strategy recommendations
  • Full signal summary JSON → data/signals.json

Run manually:   python signal_engine.py
Run in CI:      Add to fetch_data.py by importing and calling run()
Output:         data/signals.json  (consumed by dashboard)

Theory
------
GEX = gamma × OI × spot² × 0.01
  Positive GEX (net) → dealers long gamma → volatility suppressed, mean-reversion
  Negative GEX (net) → dealers short gamma → volatility amplified, trending

Dealer Positioning:
  Dealers are SHORT calls (they sell) and SHORT puts → they are:
    Short CE OI → need to BUY underlying as price RISES (delta hedge)
    Short PE OI → need to SELL underlying as price FALLS
  So net dealer delta = -ceOI × delta_ce + peOI × delta_pe (approx)
  Flip sign: positive = dealers need to buy → bullish pressure
             negative = dealers need to sell → bearish pressure
"""

import json
import math
import sys
from pathlib import Path
from datetime import datetime, timezone

DATA = Path("data")


# ── MATHS ──────────────────────────────────────────────────────────────────

def norm_cdf(x: float) -> float:
    """Abramowitz & Stegun approximation of the standard normal CDF."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = -1 if x < 0 else 1
    x = abs(x) / math.sqrt(2)
    t = 1 / (1 + p * x)
    y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x)
    return 0.5 * (1 + sign * y)


def norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def black_scholes_greeks(strike: float, spot: float, iv: float, dte: int, opt_type: str) -> dict:
    """Return delta and gamma for a European option (no dividends, r=0 approx)."""
    T = max(dte, 1) / 365.0
    sigma = max(iv, 0.01) / 100.0
    if spot <= 0 or strike <= 0:
        return {"delta": 0.0, "gamma": 0.0}
    try:
        d1 = (math.log(spot / strike) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        nd1 = norm_cdf(d1)
        npd1 = norm_pdf(d1)
        delta = nd1 if opt_type == "CE" else nd1 - 1.0
        gamma = npd1 / (spot * sigma * math.sqrt(T))
        return {"delta": round(delta, 4), "gamma": round(gamma, 6)}
    except (ValueError, ZeroDivisionError):
        return {"delta": 0.0, "gamma": 0.0}


# ── GEX ENGINE ─────────────────────────────────────────────────────────────

def compute_gex(oc: dict) -> dict:
    """
    Gamma Exposure (GEX) per strike and aggregate.

    GEX (single option) = gamma × OI × spot² × 0.01 × lot_size
    Sign convention  :
        Market makers are SHORT calls → their GEX is NEGATIVE for calls
        Market makers are SHORT puts  → their GEX is POSITIVE for puts
    Net GEX > 0 → dealers long gamma → they sell rallies / buy dips  → suppresses vol
    Net GEX < 0 → dealers short gamma → they chase moves              → amplifies vol
    """
    spot = oc.get("spot", 0)
    if spot <= 0:
        return {}

    symbol = oc.get("symbol", "")
    lot = 15 if symbol == "BANKNIFTY" else 40 if symbol == "FINNIFTY" else 50
    atm_iv = oc.get("atmIV", 15) or 15

    # Days to expiry (rough: use 7 as default if no expiry date)
    expiry_str = oc.get("expiry", "")
    dte = 7
    try:
        months = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
                  "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        parts = expiry_str.split("-")
        if len(parts) == 3:
            exp_date = datetime(int(parts[2]), months[parts[1]], int(parts[0]))
            dte = max(1, (exp_date.date() - datetime.utcnow().date()).days)
    except Exception:
        dte = 7

    gex_by_strike = []
    total_ce_gex = 0.0
    total_pe_gex = 0.0

    for s in oc.get("strikes", []):
        strike = s["strike"]
        ce_oi = s.get("ceOI", 0) or 0
        pe_oi = s.get("peOI", 0) or 0
        ce_iv = s.get("ceIV", atm_iv) or atm_iv
        pe_iv = s.get("peIV", atm_iv) or atm_iv

        ce_greeks = black_scholes_greeks(strike, spot, ce_iv, dte, "CE")
        pe_greeks = black_scholes_greeks(strike, spot, pe_iv, dte, "PE")

        # GEX in dollar-equivalent (crore-equivalent for Indian market: use raw)
        # Negative sign for calls: dealers short calls
        ce_gex = -ce_greeks["gamma"] * ce_oi * lot * spot * spot * 0.01
        # Positive sign for puts: dealers short puts → long gamma on put side
        pe_gex = +pe_greeks["gamma"] * pe_oi * lot * spot * spot * 0.01

        net_gex = ce_gex + pe_gex
        total_ce_gex += ce_gex
        total_pe_gex += pe_gex

        # Only include strikes within 5% of spot for GEX profile
        if abs(strike - spot) / spot <= 0.05:
            gex_by_strike.append({
                "strike": strike,
                "ceGEX": round(ce_gex / 1e7, 2),   # in Crore-equivalent units
                "peGEX": round(pe_gex / 1e7, 2),
                "netGEX": round(net_gex / 1e7, 2),
                "ceGamma": ce_greeks["gamma"],
                "pGamma": pe_greeks["gamma"],
                "ceDelta": ce_greeks["delta"],
                "peDelta": pe_greeks["delta"],
            })

    net_total = total_ce_gex + total_pe_gex
    gex_by_strike.sort(key=lambda x: x["strike"])

    # Zero-gamma level: strike where net GEX crosses from + to -
    zero_gamma = None
    if len(gex_by_strike) > 1:
        for i in range(len(gex_by_strike) - 1):
            a = gex_by_strike[i]["netGEX"]
            b = gex_by_strike[i + 1]["netGEX"]
            if a * b < 0:
                # Linear interpolation
                t = abs(a) / (abs(a) + abs(b))
                zero_gamma = round(
                    gex_by_strike[i]["strike"] + t * (gex_by_strike[i + 1]["strike"] - gex_by_strike[i]["strike"])
                )
                break

    regime = "long_gamma" if net_total > 0 else "short_gamma"
    return {
        "netGEX": round(net_total / 1e7, 2),
        "ceGEX": round(total_ce_gex / 1e7, 2),
        "peGEX": round(total_pe_gex / 1e7, 2),
        "regime": regime,
        "zeroGamma": zero_gamma,
        "strikes": gex_by_strike,
        "dte": dte,
        "lot": lot,
    }


# ── DEALER POSITIONING ─────────────────────────────────────────────────────

def compute_dealer_positioning(oc: dict, gex_data: dict) -> dict:
    """
    Dealer delta exposure and net positioning model.

    Assumption: market makers wrote most of the options (standard model).
    Dealer delta per strike = -(ceOI × ceDelta) + (peOI × |peDelta|)
    Aggregate positive → dealers net long delta → will sell into rallies
    Aggregate negative → dealers net short delta → will buy dips
    """
    spot = oc.get("spot", 0)
    if spot <= 0:
        return {}

    gex_strikes = {g["strike"]: g for g in gex_data.get("strikes", [])}
    atm_iv = oc.get("atmIV", 15) or 15
    dte = gex_data.get("dte", 7)
    lot = gex_data.get("lot", 50)

    total_dealer_delta = 0.0
    total_long_delta = 0.0
    total_short_delta = 0.0
    positioning_strikes = []

    for s in oc.get("strikes", []):
        strike = s["strike"]
        ce_oi = s.get("ceOI", 0) or 0
        pe_oi = s.get("peOI", 0) or 0
        ce_iv = s.get("ceIV", atm_iv) or atm_iv
        pe_iv = s.get("peIV", atm_iv) or atm_iv

        if gs := gex_strikes.get(strike):
            ce_delta = gs["ceDelta"]
            pe_delta = abs(gs["peDelta"])  # put delta is negative, take abs
        else:
            g = black_scholes_greeks(strike, spot, ce_iv, dte, "CE")
            p = black_scholes_greeks(strike, spot, pe_iv, dte, "PE")
            ce_delta = g["delta"]
            pe_delta = abs(p["delta"])

        # Dealers are SHORT calls: negative delta exposure
        # Dealers are SHORT puts: positive delta exposure (puts have negative delta)
        dealer_delta = (-ce_oi * ce_delta + pe_oi * pe_delta) * lot

        total_dealer_delta += dealer_delta
        if dealer_delta > 0:
            total_long_delta += dealer_delta
        else:
            total_short_delta += dealer_delta

        if abs(strike - spot) / spot <= 0.04:
            positioning_strikes.append({
                "strike": strike,
                "dealerDelta": round(dealer_delta, 0),
                "ceDelta": round(ce_delta, 3),
                "peDelta": round(-pe_delta, 3),
                "ceOI": ce_oi,
                "peOI": pe_oi,
            })

    positioning_strikes.sort(key=lambda x: x["strike"])

    # Classify dealer stance
    net = total_dealer_delta
    if net > 500000:
        stance = "net_long"
        stance_desc = "Dealers net LONG delta — they will sell into strength (bearish pressure above)"
    elif net < -500000:
        stance = "net_short"
        stance_desc = "Dealers net SHORT delta — they will buy dips (bullish pressure below)"
    else:
        stance = "neutral"
        stance_desc = "Dealers delta-neutral — no strong directional flow from hedging"

    # Flip wall: price level where dealer hedging flips from buy to sell
    flip_level = None
    for i in range(len(positioning_strikes) - 1):
        a = positioning_strikes[i]["dealerDelta"]
        b = positioning_strikes[i + 1]["dealerDelta"]
        if a * b < 0:
            flip_level = int((positioning_strikes[i]["strike"] + positioning_strikes[i + 1]["strike"]) / 2)
            break

    return {
        "netDealerDelta": round(net, 0),
        "longDelta": round(total_long_delta, 0),
        "shortDelta": round(total_short_delta, 0),
        "stance": stance,
        "stanceDesc": stance_desc,
        "flipLevel": flip_level,
        "strikes": positioning_strikes,
    }


# ── SIGNAL ENGINE ──────────────────────────────────────────────────────────

def compute_signals(oc: dict, gex: dict, dealer: dict, fii_data: dict) -> list:
    """
    Generate trading signals from all data sources.
    Returns list of signal dicts with keys: type, symbol, title, desc, strength, direction
    """
    signals = []
    sym = oc.get("symbol", "?")
    spot = oc.get("spot", 0)
    pcr = oc.get("pcr", 1.0)
    atm_iv = oc.get("atmIV", 15) or 15
    max_pain = oc.get("maxPain", spot)
    strikes = oc.get("strikes", [])

    # ── GEX Regime ──
    net_gex = gex.get("netGEX", 0)
    regime = gex.get("regime", "long_gamma")
    zero_gamma = gex.get("zeroGamma")

    if abs(net_gex) > 50:
        if regime == "short_gamma":
            signals.append({
                "type": "gex_regime", "symbol": sym,
                "title": f"Short Gamma Regime — {sym}",
                "desc": f"Net GEX = {net_gex:.1f}Cr. Dealers SHORT gamma: volatility amplification. Expect larger moves; avoid selling naked options.",
                "strength": min(100, int(abs(net_gex) / 2)),
                "direction": "bear", "tag": "GEX"
            })
        else:
            signals.append({
                "type": "gex_regime", "symbol": sym,
                "title": f"Long Gamma Regime — {sym}",
                "desc": f"Net GEX = +{net_gex:.1f}Cr. Dealers LONG gamma: volatility suppression. Expect range-bound price; premium selling favored.",
                "strength": min(100, int(abs(net_gex) / 2)),
                "direction": "bull", "tag": "GEX"
            })

    if zero_gamma and spot > 0:
        dist_pct = abs(zero_gamma - spot) / spot * 100
        if dist_pct < 1.5:
            signals.append({
                "type": "zero_gamma", "symbol": sym,
                "title": f"Zero-Gamma Level Close — {sym} @ ₹{zero_gamma:,}",
                "desc": f"Spot within {dist_pct:.1f}% of zero-gamma ({zero_gamma:,}). Crossing this level could trigger accelerated moves as dealer hedging flips direction.",
                "strength": max(50, int(100 - dist_pct * 30)),
                "direction": "watch", "tag": "GEX"
            })

    # ── Dealer Positioning ──
    dealer_delta = dealer.get("netDealerDelta", 0)
    stance = dealer.get("stance", "neutral")
    flip = dealer.get("flipLevel")

    if stance == "net_long" and abs(dealer_delta) > 200000:
        signals.append({
            "type": "dealer_delta", "symbol": sym,
            "title": f"Dealers Long Delta — {sym}",
            "desc": f"Dealer net delta: +{dealer_delta:,.0f} contracts. Dealers will sell into rallies — capping upside. Best to sell CEs near resistance.",
            "strength": min(100, int(abs(dealer_delta) / 50000)),
            "direction": "bear", "tag": "DEALER"
        })
    elif stance == "net_short" and abs(dealer_delta) > 200000:
        signals.append({
            "type": "dealer_delta", "symbol": sym,
            "title": f"Dealers Short Delta — {sym}",
            "desc": f"Dealer net delta: {dealer_delta:,.0f} contracts. Dealers must buy dips — providing support. Buy PEs only on strong breakdown confirmation.",
            "strength": min(100, int(abs(dealer_delta) / 50000)),
            "direction": "bull", "tag": "DEALER"
        })

    if flip and spot > 0:
        dist = abs(flip - spot) / spot * 100
        if dist < 1.0:
            signals.append({
                "type": "dealer_flip", "symbol": sym,
                "title": f"Dealer Flip Level @ ₹{flip:,} — {sym}",
                "desc": f"Dealer delta flips at ₹{flip:,} ({dist:.1f}% away). Crossing this triggers forced directional hedging — potential for sharp acceleration.",
                "strength": 85,
                "direction": "watch", "tag": "DEALER"
            })

    # ── PCR Signals ──
    if pcr > 1.5:
        signals.append({
            "type": "pcr", "symbol": sym,
            "title": f"PCR Extreme Bull — {sym}: {pcr:.2f}",
            "desc": f"PCR {pcr:.2f} — very heavy put writing. Institutions see strong support. High probability of upside or at least strong floor.",
            "strength": 80, "direction": "bull", "tag": "PCR"
        })
    elif pcr < 0.6:
        signals.append({
            "type": "pcr", "symbol": sym,
            "title": f"PCR Extreme Bear — {sym}: {pcr:.2f}",
            "desc": f"PCR {pcr:.2f} — heavy call writing dominates. Institutions capping upside aggressively. Bearish/range-bound bias.",
            "strength": 75, "direction": "bear", "tag": "PCR"
        })

    # ── IV Signals ──
    if atm_iv > 20:
        signals.append({
            "type": "iv_spike", "symbol": sym,
            "title": f"IV Elevated — {sym}: {atm_iv:.1f}%",
            "desc": f"ATM IV at {atm_iv:.1f}% — options richly priced. Sell premium via iron condors or spreads. Avoid naked option buying.",
            "strength": min(100, int((atm_iv - 15) * 8)),
            "direction": "neutral", "tag": "IV"
        })
    elif atm_iv < 11 and atm_iv > 0:
        signals.append({
            "type": "iv_low", "symbol": sym,
            "title": f"IV Crushed — {sym}: {atm_iv:.1f}%",
            "desc": f"ATM IV at {atm_iv:.1f}% — historically low. Buy straddle/strangle cheaply before anticipated catalyst or breakout.",
            "strength": min(100, int((12 - atm_iv) * 12)),
            "direction": "neutral", "tag": "IV"
        })

    # ── Max Pain ──
    mp_dist = (max_pain - spot) / spot * 100 if spot > 0 else 0
    if abs(mp_dist) > 0.8:
        direction = "bull" if mp_dist > 0 else "bear"
        signals.append({
            "type": "max_pain", "symbol": sym,
            "title": f"Max Pain Pull — {sym}: ₹{max_pain:,} ({mp_dist:+.1f}%)",
            "desc": f"Max pain at ₹{max_pain:,} is {abs(mp_dist):.1f}% {'above' if mp_dist>0 else 'below'} spot. Expiry gravity pulls price toward this level. Strongest within 3 days of expiry.",
            "strength": min(90, int(abs(mp_dist) * 25)),
            "direction": direction, "tag": "MAX PAIN"
        })

    # ── OI Wall Signals ──
    near = [s for s in strikes if abs(s["strike"] - spot) / spot <= 0.04]
    ce_walls = sorted([s for s in near if s["ceOI"] > 0], key=lambda x: -x["ceOI"])
    pe_walls = sorted([s for s in near if s["peOI"] > 0], key=lambda x: -x["peOI"])

    if ce_walls:
        top = ce_walls[0]
        fresh_pct = top["ceChgOI"] / top["ceOI"] * 100 if top["ceOI"] > 0 else 0
        if fresh_pct > 15:
            signals.append({
                "type": "ce_wall", "symbol": sym,
                "title": f"Fresh Call Writing — {sym} @ ₹{int(top['strike']):,}",
                "desc": f"CE OI at ₹{int(top['strike']):,} growing +{fresh_pct:.0f}% today ({int(top['ceOI']):,} OI). Strong resistance wall forming — sell CE spreads above this level.",
                "strength": min(95, int(fresh_pct * 2)),
                "direction": "bear", "tag": "OI WALL"
            })

    if pe_walls:
        top = pe_walls[0]
        fresh_pct = top["peChgOI"] / top["peOI"] * 100 if top["peOI"] > 0 else 0
        if fresh_pct > 15:
            signals.append({
                "type": "pe_wall", "symbol": sym,
                "title": f"Fresh Put Writing — {sym} @ ₹{int(top['strike']):,}",
                "desc": f"PE OI at ₹{int(top['strike']):,} growing +{fresh_pct:.0f}% today ({int(top['peOI']):,} OI). Strong support floor — sell PE spreads below this level.",
                "strength": min(95, int(fresh_pct * 2)),
                "direction": "bull", "tag": "OI WALL"
            })

    # Sort by strength descending
    signals.sort(key=lambda s: -s["strength"])
    return signals


# ── STRATEGY GENERATOR ─────────────────────────────────────────────────────

def generate_strategies(oc: dict, gex: dict, dealer: dict, signals: list) -> list:
    """
    Auto-generate option strategies based on all signals.
    Returns list of strategy dicts.
    """
    sym = oc.get("symbol", "?")
    spot = oc.get("spot", 0)
    pcr = oc.get("pcr", 1.0)
    atm_iv = oc.get("atmIV", 15) or 15
    max_pain = oc.get("maxPain", spot)
    strikes = oc.get("strikes", [])
    regime = gex.get("regime", "long_gamma")
    dealer_stance = dealer.get("stance", "neutral")

    step = 100 if sym == "BANKNIFTY" else 50
    lot = 15 if sym == "BANKNIFTY" else 40 if sym == "FINNIFTY" else 50

    def get_s(strike):
        return next((s for s in strikes if s["strike"] == strike), None)

    def round_to_step(v, step):
        return round(v / step) * step

    atm_strike = round_to_step(spot, step)
    strategies = []

    # ── Bull Signals Count ──
    bull_score = sum(1 for s in signals if s["direction"] == "bull")
    bear_score = sum(1 for s in signals if s["direction"] == "bear")
    is_bullish = bull_score >= bear_score and pcr >= 1.0
    is_bearish = bear_score > bull_score and pcr < 1.0
    is_range = regime == "long_gamma" and abs(bull_score - bear_score) <= 1

    # ── 1. Iron Condor (range bound + long gamma) ──
    if regime == "long_gamma" or atm_iv > 16:
        # OTM strikes — sell 1.5% OTM, buy 3% OTM
        sc_strike = round_to_step(spot * 1.015, step)
        sp_strike = round_to_step(spot * 0.985, step)
        lc_strike = sc_strike + step * 2
        lp_strike = sp_strike - step * 2

        sc = get_s(sc_strike) or {}
        sp_ = get_s(sp_strike) or {}
        lc = get_s(lc_strike) or {}
        lp = get_s(lp_strike) or {}

        sc_ltp = sc.get("ceLTP", 0) or 0
        sp_ltp = sp_.get("peLTP", 0) or 0
        lc_ltp = lc.get("ceLTP", 0) or 0
        lp_ltp = lp.get("peLTP", 0) or 0

        net_credit = round(sc_ltp + sp_ltp - lc_ltp - lp_ltp, 1)
        max_loss = round(step * 2 - max(0, net_credit), 1)
        profit_zone = f"₹{sp_strike:,} – ₹{sc_strike:,}"

        rationale = []
        if regime == "long_gamma":
            rationale.append(f"Long gamma regime (GEX: +{gex.get('netGEX',0):.1f}Cr) suppresses volatility")
        if atm_iv > 16:
            rationale.append(f"IV elevated at {atm_iv:.1f}% — premium selling favored")
        rationale.append(f"PCR {pcr:.2f} — range-bound confirmation")
        rationale.append(f"Max pain ₹{int(max_pain):,} within profit zone")

        strategies.append({
            "name": "Iron Condor",
            "type": "neutral",
            "symbol": sym,
            "legs": [
                {"action": "SELL", "strike": sc_strike, "optType": "CE", "ltp": sc_ltp},
                {"action": "BUY",  "strike": lc_strike, "optType": "CE", "ltp": lc_ltp},
                {"action": "SELL", "strike": sp_strike, "optType": "PE", "ltp": sp_ltp},
                {"action": "BUY",  "strike": lp_strike, "optType": "PE", "ltp": lp_ltp},
            ],
            "netCredit": net_credit,
            "maxLoss": max_loss,
            "profitZone": profit_zone,
            "rrRatio": round(max_loss / net_credit, 2) if net_credit > 0 else 999,
            "lot": lot,
            "rationale": rationale,
            "confidence": 78 if regime == "long_gamma" else 62,
            "tag": "Range / Low Vol"
        })

    # ── 2. Bull Put Spread ──
    pe_support = sorted([s for s in strikes if s["strike"] < spot and s["peOI"] > 0], key=lambda x: -x["peOI"])
    if pe_support and (is_bullish or pcr > 1.1):
        sell_strike = pe_support[0]["strike"]
        buy_strike = sell_strike - step * 2
        sell_s = get_s(sell_strike) or {}
        buy_s = get_s(buy_strike) or {}
        sell_ltp = sell_s.get("peLTP", 0) or 0
        buy_ltp = buy_s.get("peLTP", 0) or 0
        credit = round(sell_ltp - buy_ltp, 1)
        max_loss_ = round(step * 2 - max(0, credit), 1)

        rationale = [
            f"Strong put writing at ₹{int(sell_strike):,} ({int(pe_support[0].get('peOI',0)):,} OI)",
            f"PCR {pcr:.2f} — bullish bias",
        ]
        if dealer_stance == "net_short":
            rationale.append("Dealers short delta: buying pressure below = support confirmed")
        if max_pain > spot:
            rationale.append(f"Max pain ₹{int(max_pain):,} above spot — expiry gravity bullish")

        strategies.append({
            "name": "Bull Put Spread",
            "type": "bullish",
            "symbol": sym,
            "legs": [
                {"action": "SELL", "strike": sell_strike, "optType": "PE", "ltp": sell_ltp},
                {"action": "BUY",  "strike": buy_strike,  "optType": "PE", "ltp": buy_ltp},
            ],
            "netCredit": credit,
            "maxLoss": max_loss_,
            "profitZone": f"Above ₹{int(sell_strike):,}",
            "rrRatio": round(max_loss_ / credit, 2) if credit > 0 else 999,
            "lot": lot,
            "rationale": rationale,
            "confidence": min(90, 55 + int(pcr * 10) + (10 if dealer_stance == "net_short" else 0)),
            "tag": "Bullish / Premium Sell"
        })

    # ── 3. Bear Call Spread ──
    ce_resistance = sorted([s for s in strikes if s["strike"] > spot and s["ceOI"] > 0], key=lambda x: -x["ceOI"])
    if ce_resistance and (is_bearish or pcr < 0.9):
        sell_strike = ce_resistance[0]["strike"]
        buy_strike = sell_strike + step * 2
        sell_s = get_s(sell_strike) or {}
        buy_s = get_s(buy_strike) or {}
        sell_ltp = sell_s.get("ceLTP", 0) or 0
        buy_ltp = buy_s.get("ceLTP", 0) or 0
        credit = round(sell_ltp - buy_ltp, 1)
        max_loss_ = round(step * 2 - max(0, credit), 1)

        rationale = [
            f"Heavy call writing at ₹{int(sell_strike):,} ({int(ce_resistance[0].get('ceOI',0)):,} OI)",
            f"PCR {pcr:.2f} — bearish/capped",
        ]
        if dealer_stance == "net_long":
            rationale.append("Dealers long delta: selling into rallies = resistance confirmed")
        if max_pain < spot:
            rationale.append(f"Max pain ₹{int(max_pain):,} below spot — expiry gravity bearish")

        strategies.append({
            "name": "Bear Call Spread",
            "type": "bearish",
            "symbol": sym,
            "legs": [
                {"action": "SELL", "strike": sell_strike, "optType": "CE", "ltp": sell_ltp},
                {"action": "BUY",  "strike": buy_strike,  "optType": "CE", "ltp": buy_ltp},
            ],
            "netCredit": credit,
            "maxLoss": max_loss_,
            "profitZone": f"Below ₹{int(sell_strike):,}",
            "rrRatio": round(max_loss_ / credit, 2) if credit > 0 else 999,
            "lot": lot,
            "rationale": rationale,
            "confidence": min(90, 55 + int((1 - pcr) * 20) + (10 if dealer_stance == "net_long" else 0)),
            "tag": "Bearish / Premium Sell"
        })

    # ── 4. ATM Straddle Buy (low IV, short gamma regime) ──
    if atm_iv < 12 or regime == "short_gamma":
        atm_s = get_s(atm_strike) or {}
        ce_ltp = atm_s.get("ceLTP", 0) or 0
        pe_ltp = atm_s.get("peLTP", 0) or 0
        total_cost = round(ce_ltp + pe_ltp, 1)
        be_up = round(atm_strike + total_cost, 1)
        be_dn = round(atm_strike - total_cost, 1)

        rationale = []
        if atm_iv < 12:
            rationale.append(f"IV at {atm_iv:.1f}% — historically low, options cheap")
        if regime == "short_gamma":
            rationale.append(f"Short gamma regime (GEX: {gex.get('netGEX',0):.1f}Cr) — big moves expected")
        rationale.append(f"Cost: ₹{total_cost} | B/E: ₹{be_dn:,} – ₹{be_up:,}")

        strategies.append({
            "name": "ATM Straddle Buy",
            "type": "volatility",
            "symbol": sym,
            "legs": [
                {"action": "BUY", "strike": atm_strike, "optType": "CE", "ltp": ce_ltp},
                {"action": "BUY", "strike": atm_strike, "optType": "PE", "ltp": pe_ltp},
            ],
            "netCredit": -total_cost,
            "maxLoss": total_cost,
            "profitZone": f"< ₹{be_dn:,} or > ₹{be_up:,}",
            "rrRatio": 0,  # unlimited upside
            "lot": lot,
            "rationale": rationale,
            "confidence": min(90, 50 + (20 if atm_iv < 12 else 0) + (20 if regime == "short_gamma" else 0)),
            "tag": "Volatility Buy"
        })

    # ── 5. Strangle (wider, higher IV) ──
    if atm_iv > 18:
        sell_ce_strike = round_to_step(spot * 1.02, step)
        sell_pe_strike = round_to_step(spot * 0.98, step)
        sc = get_s(sell_ce_strike) or {}
        sp_ = get_s(sell_pe_strike) or {}
        sc_ltp = sc.get("ceLTP", 0) or 0
        sp_ltp = sp_.get("peLTP", 0) or 0
        credit = round(sc_ltp + sp_ltp, 1)

        rationale = [
            f"IV at {atm_iv:.1f}% — very high premium",
            f"Collect ₹{credit} premium per lot",
            f"Max pain ₹{int(max_pain):,} within short strangle range",
            "Hedge: buy further OTM options to cap loss",
        ]

        strategies.append({
            "name": "Short Strangle",
            "type": "neutral",
            "symbol": sym,
            "legs": [
                {"action": "SELL", "strike": sell_ce_strike, "optType": "CE", "ltp": sc_ltp},
                {"action": "SELL", "strike": sell_pe_strike, "optType": "PE", "ltp": sp_ltp},
            ],
            "netCredit": credit,
            "maxLoss": 999999,   # theoretically unlimited — must hedge
            "profitZone": f"₹{sell_pe_strike:,} – ₹{sell_ce_strike:,}",
            "rrRatio": 0,
            "lot": lot,
            "rationale": rationale,
            "confidence": min(75, 40 + int((atm_iv - 18) * 5)),
            "tag": "High IV Premium Sell ⚠️ Hedge Required"
        })

    # Sort by confidence
    strategies.sort(key=lambda s: -s["confidence"])
    return strategies


# ── AGGREGATE SUMMARY ──────────────────────────────────────────────────────

def compute_market_regime_summary(all_gex: dict, all_dealer: dict, all_signals: list) -> dict:
    """
    Cross-symbol market regime summary.
    """
    total_gex = sum(g.get("netGEX", 0) for g in all_gex.values())
    short_gamma_count = sum(1 for g in all_gex.values() if g.get("regime") == "short_gamma")
    bull_signals = sum(1 for s in all_signals if s["direction"] == "bull")
    bear_signals = sum(1 for s in all_signals if s["direction"] == "bear")

    if short_gamma_count >= 2:
        vol_regime = "SHORT_GAMMA"
        vol_desc = "Market in short-gamma regime — expect amplified moves in either direction. Avoid premium selling."
    elif total_gex > 100:
        vol_regime = "LONG_GAMMA"
        vol_desc = "Market in long-gamma regime — dealers suppressing volatility. Premium selling favored."
    else:
        vol_regime = "TRANSITIONING"
        vol_desc = "Mixed gamma exposure — market transitioning. Wait for clarity."

    if bull_signals > bear_signals + 2:
        sentiment = "BULLISH"
    elif bear_signals > bull_signals + 2:
        sentiment = "BEARISH"
    else:
        sentiment = "NEUTRAL"

    dealer_stances = [d.get("stance", "neutral") for d in all_dealer.values()]
    if dealer_stances.count("net_short") >= 2:
        dealer_summary = "Dealers net short delta — buy-dip pressure across markets"
    elif dealer_stances.count("net_long") >= 2:
        dealer_summary = "Dealers net long delta — sell-rally pressure across markets"
    else:
        dealer_summary = "Dealer delta mixed — no dominant directional flow"

    return {
        "volRegime": vol_regime,
        "volDesc": vol_desc,
        "sentiment": sentiment,
        "totalGEX": round(total_gex, 2),
        "dealerSummary": dealer_summary,
        "bullSignals": bull_signals,
        "bearSignals": bear_signals,
    }


# ── MAIN ───────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"SIGNAL ENGINE — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Load option chain data
    oc_data = {}
    for sym, fname in [("NIFTY", "oc_nifty.json"), ("BANKNIFTY", "oc_banknifty.json"), ("FINNIFTY", "oc_finnifty.json")]:
        path = DATA / fname
        if path.exists():
            oc_data[sym] = json.loads(path.read_text())
            print(f"  Loaded {fname}: spot={oc_data[sym].get('spot')}, strikes={len(oc_data[sym].get('strikes',[]))}")
        else:
            print(f"  MISSING: {fname}")

    # Load FII data
    fii_data = {}
    fii_path = DATA / "fii_dii.json"
    if fii_path.exists():
        fii_data = json.loads(fii_path.read_text())
        print(f"  Loaded fii_dii.json: {len(fii_data.get('data',[]))} rows")

    if not oc_data:
        print("  No option chain data found. Exiting.")
        return False

    all_gex = {}
    all_dealer = {}
    all_signals = []
    all_strategies = []

    for sym, oc in oc_data.items():
        print(f"\n  [{sym}]")
        gex = compute_gex(oc)
        dealer = compute_dealer_positioning(oc, gex)
        signals = compute_signals(oc, gex, dealer, fii_data)
        strategies = generate_strategies(oc, gex, dealer, signals)

        all_gex[sym] = gex
        all_dealer[sym] = dealer
        all_signals.extend(signals)
        all_strategies.extend(strategies)

        print(f"    GEX: {gex.get('netGEX')} Cr | regime: {gex.get('regime')} | zero-gamma: {gex.get('zeroGamma')}")
        print(f"    Dealer: {dealer.get('stance')} | flip: {dealer.get('flipLevel')}")
        print(f"    Signals: {len(signals)} | Strategies: {len(strategies)}")

    summary = compute_market_regime_summary(all_gex, all_dealer, all_signals)
    print(f"\n  MARKET SUMMARY: vol={summary['volRegime']} | sentiment={summary['sentiment']} | totalGEX={summary['totalGEX']}Cr")

    # Augment each OC file with GEX and dealer data for the dashboard
    for sym, oc in oc_data.items():
        oc["gex"] = all_gex[sym]
        oc["dealer"] = all_dealer[sym]
        fname = f"oc_{sym.lower()}.json"
        (DATA / fname).write_text(json.dumps(oc, default=str, indent=2))
        print(f"  Updated {fname} with GEX + dealer data")

    # Save signals.json
    output = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "signals": all_signals,
        "strategies": all_strategies,
        "gex": {sym: {k: v for k, v in g.items() if k != "strikes"} for sym, g in all_gex.items()},
        "dealer": {sym: {k: v for k, v in d.items() if k != "strikes"} for sym, d in all_dealer.items()},
    }
    (DATA / "signals.json").write_text(json.dumps(output, default=str, indent=2))
    print(f"\n  SAVED: data/signals.json ({len(all_signals)} signals, {len(all_strategies)} strategies)")
    return True


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
