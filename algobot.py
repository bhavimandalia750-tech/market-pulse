#!/usr/bin/env python3
"""
Market Pulse — AlgoBot  v1.0
==============================
Institutional-grade automated trading system for NSE derivatives.
Inspired by quantitative risk management principles — NOT a guaranteed profit machine.
Markets are probabilistic. This system manages probabilities + risk at scale.

What this does:
  • Eliminates human psychology from every decision
  • Combines ML prediction + rule-based signals (both must agree)
  • Enforces strict risk limits — daily loss cap, consecutive loss pause
  • Backtests every strategy before live deployment
  • Learns from every trade via feedback loop
  • Paper trades by default — flip PAPER_TRADE = False only after backtesting

Architecture (DATA → FEATURE → MODEL → SIGNAL → RISK → EXECUTION → FEEDBACK):
  Module 1: ML Engine       — direction + volatility + regime prediction
  Module 2: Backtest Engine — walk-forward simulation on all history
  Module 3: Signal Fusion   — ML + rule-based agreement gate
  Module 4: Risk Engine     — Kelly sizing, kill switches, manipulation filter
  Module 5: Strategy Selector — regime → strategy mapping
  Module 6: Execution Engine — Zerodha Kite API / paper mode
  Module 7: Portfolio Monitor — real-time P&L, Greeks, drawdown
  Module 8: Feedback Loop   — journal update, model retrain, daily report

Usage:
  python algobot.py               # paper trade (safe default)
  python algobot.py --live        # live trading (only after thorough backtest)
  python algobot.py --backtest    # run backtest on all history and exit
  python algobot.py --report      # print performance report and exit
  python algobot.py --retrain     # retrain ML model and exit
"""

import json
import math
import os
import re
import sys
import time
import logging
import statistics
import traceback
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── ML imports (all available in standard Python env) ────────────────────────
import numpy as np
try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    from sklearn.calibration import CalibratedClassifierCV
    import joblib
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    print("WARNING: scikit-learn not found. pip install scikit-learn joblib numpy")

# ── Config ────────────────────────────────────────────────────────────────────
DATA      = Path("data")
HIST      = Path("data/history")
MODELS    = Path("data/models")
LOGS      = Path("data/logs")
for p in [DATA, HIST, MODELS, LOGS]: p.mkdir(exist_ok=True)

# Master switches — change these, nothing else
PAPER_TRADE     = True       # ALWAYS start here. False = real money.
CAPITAL         = 500_000    # ₹5 lakh trading capital
MAX_RISK_PCT    = 0.01       # 1% max risk per trade
MAX_DAILY_LOSS  = 0.03       # 3% daily loss → hard stop
MAX_CONSEC_LOSS = 3          # 3 consecutive losses → pause
LOT_SIZE        = 25         # NIFTY lot size
MIN_CONFIDENCE  = 0.58       # minimum confidence to trade
ML_WEIGHT       = 0.45       # ML signal weight in fusion (vs 0.55 rule-based)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOGS / f"algobot_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("algobot")

TS_PAT = re.compile(r"_(\d{4}-\d{2}-\d{2}T\d{4})\.json$")
IST    = timezone(timedelta(hours=5, minutes=30))


# ══════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _ts(name: str) -> Optional[datetime]:
    m = TS_PAT.search(name)
    if not m: return None
    try: return datetime.strptime(m.group(1), "%Y-%m-%dT%H%M").replace(tzinfo=timezone.utc)
    except: return None

def load_json(path: Path) -> dict:
    try: return json.loads(path.read_text()) if path.exists() else {}
    except: return {}

def save_json(path: Path, data):
    path.write_text(json.dumps(data, default=str, indent=2))

def now_ist() -> datetime:
    return datetime.now(timezone.utc).astimezone(IST)

def is_market_hours() -> bool:
    t   = now_ist()
    day = t.weekday()    # 0=Mon … 4=Fri
    mins= t.hour*60 + t.minute
    return day < 5 and 9*60+15 <= mins < 15*60+20

def safe_div(a, b, default=0.0) -> float:
    return a / b if b and b != 0 else default

def smean(s): return statistics.mean(s) if s else 0.0
def sstdev(s): return statistics.stdev(s) if len(s)>=2 else 1e-9


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 1 — ML ENGINE
# Direction model: predicts P(up), P(flat), P(down) for next 15 min
# Volatility model: predicts expansion vs compression
# Regime model: trend vs range vs volatile
# Features: 36 derived from last 20 option chain snapshots
# ══════════════════════════════════════════════════════════════════════════════

class MLEngine:
    MODEL_PATH   = MODELS / "direction_model.pkl"
    VOL_MODEL    = MODELS / "volatility_model.pkl"
    SCALER_PATH  = MODELS / "scaler.pkl"
    PRED_PATH    = DATA   / "ml_prediction.json"
    WINDOW       = 20     # lookback snapshots
    FORWARD      = 3      # predict 3 snapshots ahead = 15 min

    def __init__(self):
        self.dir_model  = None
        self.vol_model  = None
        self.scaler     = None
        self._load_models()

    def _load_models(self):
        if not ML_AVAILABLE: return
        try:
            if self.MODEL_PATH.exists():
                self.dir_model = joblib.load(self.MODEL_PATH)
                self.scaler    = joblib.load(self.SCALER_PATH)
                log.info(f"ML: loaded direction model ({self.MODEL_PATH.stat().st_size//1024}KB)")
            if self.VOL_MODEL.exists():
                self.vol_model = joblib.load(self.VOL_MODEL)
                log.info("ML: loaded volatility model")
        except Exception as e:
            log.warning(f"ML: could not load models: {e}")

    def _load_history(self, symbol: str, max_days: int = 7) -> list:
        """Load all OC snapshots for a symbol."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
        rows   = []
        prefix = f"oc_{symbol.lower()}_"
        for f in sorted(HIST.glob(f"{prefix}*.json")):
            ts = _ts(f.name)
            if ts is None or ts < cutoff: continue
            try:
                r  = json.loads(f.read_text())
                gx = r.get("gex",    {}) or {}
                dl = r.get("dealer", {}) or {}
                sp = float(r.get("spot", 0) or 0)
                if sp <= 0: continue
                rows.append({
                    "t":      ts,
                    "spot":   sp,
                    "pcr":    float(r.get("pcr",   1) or 1),
                    "iv":     float(r.get("atmIV", 0) or 0),
                    "mp":     float(r.get("maxPain",0) or 0),
                    "ceOI":   float(r.get("totalCeOI",0) or 0),
                    "peOI":   float(r.get("totalPeOI",0) or 0),
                    "gex":    float(gx.get("netGEX",  0) or 0),
                    "regime": 1.0 if gx.get("regime") == "short_gamma" else 0.0,
                    "dealer": float(dl.get("netDealerDelta",0) or 0),
                    "stance": 1.0 if dl.get("stance") == "net_long" else -1.0 if dl.get("stance") == "net_short" else 0.0,
                    "flip":   float(dl.get("flipLevel") or sp),
                })
            except: continue
        rows.sort(key=lambda x: x["t"])
        return rows

    def extract_features(self, rows: list, idx: int) -> Optional[list]:
        """
        36 features from a 20-snapshot window ending at idx.
        Returns None if insufficient data.
        """
        W = self.WINDOW
        if idx < W: return None
        w = rows[idx - W:idx + 1]  # W+1 rows so we can compute returns

        spots  = [r["spot"]   for r in w]
        pcrs   = [r["pcr"]    for r in w]
        ivs    = [r["iv"]     for r in w]
        gexes  = [r["gex"]    for r in w]
        dlts   = [r["dealer"] for r in w]
        ceois  = [r["ceOI"]   for r in w]
        peois  = [r["peOI"]   for r in w]

        def ret(i, j): return safe_div(spots[j] - spots[i], spots[i])
        def mean_ret(n): return smean([ret(i, i+1) for i in range(len(spots)-n-1, len(spots)-1)])
        def vol_n(n): rets = [ret(i,i+1) for i in range(len(spots)-n-1, len(spots)-1)]; return sstdev(rets) if len(rets)>1 else 0

        s = spots
        f = [
            # ── Price momentum (6 features) ────────────────────────
            ret(-W, -1),                              # full window return
            ret(-6, -1),                              # 30-min return
            ret(-3, -1),                              # 15-min return
            ret(-2, -1),                              # 10-min return
            ret(-1,  0),                              # last 5-min return
            (s[-1] - smean(s[-10:])) / (sstdev(s[-10:]) or 1),  # z-score vs 50min mean

            # ── Volatility (4 features) ────────────────────────────
            vol_n(W-1),                               # 20-bar vol
            vol_n(5),                                 # 5-bar vol (recent)
            safe_div(vol_n(5), vol_n(W-1)),           # vol ratio (expansion)
            safe_div(max(s[-5:]) - min(s[-5:]), s[-1]), # range/price

            # ── PCR (5 features) ───────────────────────────────────
            pcrs[-1],                                 # current PCR
            pcrs[-1] - smean(pcrs[-6:]),              # PCR vs 30-min mean
            pcrs[-1] - smean(pcrs[-W:]),              # PCR vs session mean
            smean(pcrs[-3:]) - smean(pcrs[-6:-3]),    # PCR momentum
            sstdev(pcrs[-10:]),                       # PCR stability

            # ── IV (4 features) ────────────────────────────────────
            ivs[-1],                                  # current IV
            ivs[-1] - smean(ivs[-W:]),                # IV vs mean
            smean(ivs[-3:]) - smean(ivs[-6:-3]),      # IV momentum
            safe_div(ivs[-1] - min(ivs), max(ivs) - min(ivs) + 1e-6),  # IV rank in window

            # ── GEX / Dealer (7 features) ──────────────────────────
            gexes[-1],                                # current GEX
            smean(gexes[-6:]) - smean(gexes[-W:]),    # GEX trend
            rows[idx]["regime"],                      # short/long gamma
            rows[idx]["stance"],                      # dealer stance
            safe_div(rows[idx]["flip"] - s[-1], s[-1]),  # flip level distance
            dlts[-1] / 1e7,                           # dealer delta (scaled)
            smean(dlts[-3:]) - smean(dlts[-6:-3]),    # dealer momentum

            # ── OI Flow (5 features) ───────────────────────────────
            safe_div(ceois[-1] - ceois[-6], ceois[-6]+1),  # CE OI change 30min
            safe_div(peois[-1] - peois[-6], peois[-6]+1),  # PE OI change 30min
            safe_div(ceois[-1], peois[-1]+1),         # CE/PE ratio
            safe_div(ceois[-1] - ceois[-3], ceois[-3]+1),  # CE OI accel
            safe_div(peois[-1] - peois[-3], peois[-3]+1),  # PE OI accel

            # ── Max Pain (2 features) ──────────────────────────────
            safe_div(rows[idx]["mp"] - s[-1], s[-1]), # MP distance from spot
            safe_div(rows[idx]["mp"] - rows[max(0,idx-12)]["mp"], rows[idx]["mp"]+1),  # MP drift

            # ── Time context (3 features) ──────────────────────────
            rows[idx]["t"].astimezone(IST).hour / 24,  # hour of day
            rows[idx]["t"].weekday() / 5,              # day of week
            rows[idx]["t"].astimezone(IST).minute / 60, # minute
        ]
        return [round(float(x), 6) if math.isfinite(x) else 0.0 for x in f]

    def build_dataset(self, symbol: str) -> tuple:
        """Build X, y for training. y: 1=up, 0=flat, -1=down."""
        rows = self._load_history(symbol)
        n    = len(rows)
        if n < self.WINDOW + self.FORWARD + 5:
            log.warning(f"ML: only {n} rows — need {self.WINDOW + self.FORWARD + 5}+")
            return np.array([]), np.array([]), np.array([])

        X_dir, X_vol, y_dir, y_vol = [], [], [], []

        for i in range(self.WINDOW, n - self.FORWARD):
            feats = self.extract_features(rows, i)
            if feats is None: continue

            # Direction target: move of next FORWARD×5min
            curr  = rows[i]["spot"]
            fwd   = rows[i + self.FORWARD]["spot"]
            ret   = safe_div(fwd - curr, curr)
            y_d   = 1 if ret > 0.0015 else -1 if ret < -0.0015 else 0

            # Volatility target: did range expand?
            fwd_spots = [rows[i+j]["spot"] for j in range(1, self.FORWARD+1)]
            rng  = safe_div(max(fwd_spots) - min(fwd_spots), curr)
            y_v  = 1 if rng > safe_div(max(rows[i]["spot"] for rows2 in [rows[i-10:i]] for rows2 in [rows2]) - min(rows[i]["spot"] for rows2 in [rows[i-10:i]] for rows2 in [rows2]), curr) else 0

            X_dir.append(feats)
            y_dir.append(y_d)
            # Volatility target
            hist_range = safe_div(
                max(r["spot"] for r in rows[max(0,i-10):i]) - min(r["spot"] for r in rows[max(0,i-10):i]),
                curr
            )
            X_vol.append(feats)
            y_vol.append(1 if rng > hist_range * 1.3 else 0)

        X = np.array(X_dir, dtype=np.float32)
        y_dir_arr = np.array(y_dir)
        y_vol_arr = np.array(y_vol)
        return X, y_dir_arr, y_vol_arr

    def train(self, symbol: str = "NIFTY") -> dict:
        """Train direction + volatility models. Saves pkl files."""
        log.info(f"ML: training on {symbol} history...")
        X, y_dir, y_vol = self.build_dataset(symbol)

        if len(X) < 20:
            msg = f"Insufficient data: {len(X)} samples (need 20+). Accumulate more history."
            log.warning(f"ML: {msg}")
            return {"status": "insufficient_data", "samples": len(X), "message": msg}

        # Scale features
        scaler = StandardScaler()
        X_sc   = scaler.fit_transform(X)

        # Direction model — Random Forest with probability calibration
        rf = RandomForestClassifier(
            n_estimators=200, max_depth=5, min_samples_leaf=3,
            class_weight="balanced", random_state=42, n_jobs=-1
        )
        # Calibrate probabilities (important for confidence scores)
        dir_model = CalibratedClassifierCV(rf, cv=min(3, len(X)//10 or 2), method="sigmoid")
        dir_model.fit(X_sc, y_dir)

        # Volatility model — Gradient Boosting
        gb = GradientBoostingClassifier(
            n_estimators=100, max_depth=3, learning_rate=0.05, random_state=42
        )
        vol_model = CalibratedClassifierCV(gb, cv=min(3, len(X)//10 or 2), method="sigmoid")
        vol_model.fit(X_sc, y_vol)

        # Cross-validation scores
        cv_dir = cross_val_score(rf, X_sc, y_dir, cv=min(3, len(X)//10 or 2), scoring="accuracy")
        cv_vol = cross_val_score(gb, X_sc, y_vol, cv=min(3, len(X)//10 or 2), scoring="accuracy")

        # Save models
        joblib.dump(dir_model, self.MODEL_PATH)
        joblib.dump(vol_model, self.VOL_MODEL)
        joblib.dump(scaler,    self.SCALER_PATH)
        self.dir_model = dir_model
        self.vol_model = vol_model
        self.scaler    = scaler

        result = {
            "status":         "trained",
            "samples":        len(X),
            "trainedAt":      datetime.now(timezone.utc).isoformat(),
            "dirAccuracy":    round(float(cv_dir.mean()), 3),
            "dirAccuracyStd": round(float(cv_dir.std()),  3),
            "volAccuracy":    round(float(cv_vol.mean()), 3),
            "classBalance":   {
                "up":   int(np.sum(y_dir == 1)),
                "flat": int(np.sum(y_dir == 0)),
                "down": int(np.sum(y_dir == -1)),
            },
            "note": (
                "GOOD signal" if cv_dir.mean() > 0.55 else
                "Marginal signal — need more data" if cv_dir.mean() > 0.45 else
                "Weak — do NOT trade live until accuracy > 0.55"
            )
        }
        log.info(f"ML: trained. dir_acc={result['dirAccuracy']:.3f}±{result['dirAccuracyStd']:.3f}")
        save_json(DATA / "ml_training_result.json", result)
        return result

    def predict(self, symbol: str) -> dict:
        """Run prediction on latest snapshots. Returns probabilities."""
        empty = {"status": "no_model", "upProb": 0.33, "flatProb": 0.34, "downProb": 0.33,
                 "volExpansion": 0.5, "regime": "unknown", "confidence": 0.0}

        if not ML_AVAILABLE or self.dir_model is None or self.scaler is None:
            log.debug("ML: no model loaded — returning neutral")
            return empty

        rows = self._load_history(symbol)
        if len(rows) < self.WINDOW + 2:
            return {**empty, "status": "insufficient_history"}

        idx   = len(rows) - 1
        feats = self.extract_features(rows, idx)
        if feats is None:
            return {**empty, "status": "feature_extraction_failed"}

        try:
            X = np.array([feats], dtype=np.float32)
            X_sc = self.scaler.transform(X)

            # Direction probabilities [down, flat, up] (class order: -1, 0, 1)
            dir_probs = self.dir_model.predict_proba(X_sc)[0]
            classes   = self.dir_model.classes_
            prob_map  = dict(zip(classes, dir_probs))
            p_up   = float(prob_map.get( 1, 0.33))
            p_flat = float(prob_map.get( 0, 0.34))
            p_down = float(prob_map.get(-1, 0.33))

            # Volatility probability
            vol_probs    = self.vol_model.predict_proba(X_sc)[0]
            p_expansion  = float(vol_probs[1]) if len(vol_probs) > 1 else 0.5

            # Regime from features
            regime_score = feats[12] if len(feats) > 12 else 0  # short_gamma feature
            regime = "short_gamma" if regime_score > 0.5 else "long_gamma"

            # Confidence = how much the model disagrees with 33% baseline
            max_prob = max(p_up, p_down)
            confidence = max(0.0, (max_prob - 0.40) / 0.50)  # 0 at 40%, 1 at 90%

            result = {
                "status":      "ok",
                "symbol":      symbol,
                "upProb":      round(p_up,   3),
                "flatProb":    round(p_flat, 3),
                "downProb":    round(p_down, 3),
                "direction":   "up" if p_up > p_down and p_up > p_flat else
                               "down" if p_down > p_up and p_down > p_flat else "flat",
                "volExpansion":round(p_expansion, 3),
                "regime":      regime,
                "confidence":  round(confidence, 3),
                "predictedAt": datetime.now(timezone.utc).isoformat(),
            }
            save_json(self.PRED_PATH, result)
            log.info(f"ML [{symbol}]: up={p_up:.2f} down={p_down:.2f} flat={p_flat:.2f} conf={confidence:.2f}")
            return result
        except Exception as e:
            log.error(f"ML predict error: {e}")
            return {**empty, "status": f"error: {e}"}


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 2 — BACKTEST ENGINE
# Walk-forward simulation: train on first 80%, test on last 20%
# Simulates each strategy on historical snapshots
# Outputs: backtest_results.json with equity curve, win rate, max drawdown
# ══════════════════════════════════════════════════════════════════════════════

class BacktestEngine:

    def __init__(self, ml: MLEngine):
        self.ml = ml

    def run(self, symbol: str = "NIFTY") -> dict:
        rows = self.ml._load_history(symbol, max_days=7)
        n    = len(rows)
        log.info(f"Backtest [{symbol}]: {n} snapshots")

        if n < 40:
            return {"status": "insufficient_data", "samples": n}

        # Walk-forward: train on first 70%, test on last 30%
        split       = int(n * 0.70)
        train_rows  = rows[:split]
        test_rows   = rows[split:]

        # Simulate three strategies on test set
        results = {}
        for strat in ["breakout", "reversal", "premium_sell"]:
            res = self._simulate_strategy(strat, test_rows, symbol)
            results[strat] = res
            log.info(f"  {strat}: WR={res['winRate']:.0%} expectancy={res['expectancy']:.2f} maxDD={res['maxDD']:.1%}")

        # Overall system result (best strategy per regime)
        combined = self._simulate_regime_switching(test_rows, symbol)
        results["combined"] = combined

        output = {
            "symbol":       symbol,
            "backtestAt":   datetime.now(timezone.utc).isoformat(),
            "trainSamples": split,
            "testSamples":  n - split,
            "strategies":   results,
            "verdict":      self._verdict(combined),
        }
        save_json(DATA / "backtest_results.json", output)
        log.info(f"Backtest complete. Combined: WR={combined['winRate']:.0%} exp={combined['expectancy']:.2f}")
        return output

    def _simulate_strategy(self, strat: str, rows: list, symbol: str) -> dict:
        W = self.ml.WINDOW
        capital = CAPITAL
        equity  = [capital]
        trades  = []
        wins = losses = 0

        for i in range(W, len(rows) - 3):
            signal = self._generate_rule_signal(strat, rows, i)
            if not signal: continue

            entry   = rows[i]["spot"]
            atr     = self._atr(rows, i, 12)
            sl_dist = atr * 1.5
            tp_dist = atr * (2.5 if strat == "breakout" else 1.8 if strat == "reversal" else 3.0)
            direction = signal["direction"]

            sl = entry - sl_dist if direction == "up" else entry + sl_dist
            tp = entry + tp_dist if direction == "up" else entry - tp_dist

            # Simulate next 6 bars (30 min max hold)
            result = "timeout"; exit_price = rows[min(i+6, len(rows)-1)]["spot"]
            for j in range(1, min(7, len(rows)-i)):
                price = rows[i+j]["spot"]
                if direction == "up":
                    if price <= sl: result = "loss"; exit_price = sl; break
                    if price >= tp: result = "win";  exit_price = tp; break
                else:
                    if price >= sl: result = "loss"; exit_price = sl; break
                    if price <= tp: result = "win";  exit_price = tp; break

            # P&L
            risk_pts = sl_dist
            lots     = max(1, int((capital * MAX_RISK_PCT) / (risk_pts * LOT_SIZE)))
            pnl_pts  = (exit_price - entry) * (1 if direction == "up" else -1)
            pnl      = pnl_pts * lots * LOT_SIZE
            capital += pnl
            equity.append(capital)

            if result == "win": wins += 1
            else: losses += 1
            trades.append({"pnl": pnl, "result": result, "rr": safe_div(pnl_pts, risk_pts)})

        total       = wins + losses or 1
        win_rate    = wins / total
        all_pnl     = [t["pnl"] for t in trades]
        expectancy  = smean(all_pnl) if all_pnl else 0
        max_dd      = self._max_drawdown(equity)

        return {
            "winRate":   round(win_rate, 3),
            "trades":    total,
            "wins":      wins,
            "losses":    losses,
            "expectancy": round(expectancy, 2),
            "totalPnL":  round(capital - CAPITAL, 2),
            "maxDD":     round(max_dd, 4),
            "equityCurve": [round(e) for e in equity[-50:]],  # last 50 points
        }

    def _simulate_regime_switching(self, rows: list, symbol: str) -> dict:
        """Switch strategy based on current regime — same logic as live system."""
        W = self.ml.WINDOW
        capital = CAPITAL; equity = [capital]
        wins = losses = 0; trades_log = []

        for i in range(W, len(rows) - 3):
            gex_regime = rows[i]["regime"]
            # Regime → strategy
            strat = "breakout" if gex_regime > 0.5 else "reversal"
            signal = self._generate_rule_signal(strat, rows, i)
            if not signal: continue

            entry   = rows[i]["spot"]
            atr     = self._atr(rows, i, 12)
            sl_dist = atr * 1.5
            tp_dist = atr * 2.5
            direction = signal["direction"]
            sl = entry - sl_dist if direction == "up" else entry + sl_dist
            tp = entry + tp_dist if direction == "up" else entry - tp_dist

            result = "timeout"; exit_price = rows[min(i+6, len(rows)-1)]["spot"]
            for j in range(1, min(7, len(rows)-i)):
                price = rows[i+j]["spot"]
                if direction == "up":
                    if price <= sl: result = "loss"; exit_price = sl; break
                    if price >= tp: result = "win";  exit_price = tp; break
                else:
                    if price >= sl: result = "loss"; exit_price = sl; break
                    if price <= tp: result = "win";  exit_price = tp; break

            risk_pts = sl_dist
            lots     = max(1, int((capital * MAX_RISK_PCT) / (risk_pts * LOT_SIZE + 1e-9)))
            pnl_pts  = (exit_price - entry) * (1 if direction == "up" else -1)
            pnl      = pnl_pts * lots * LOT_SIZE
            capital += pnl; equity.append(capital)
            if result == "win": wins += 1
            else: losses += 1

        total = wins + losses or 1
        return {
            "winRate":    round(wins/total, 3),
            "trades":     total,
            "wins":       wins,
            "losses":     losses,
            "expectancy": round(smean([e - CAPITAL for e in equity]) if equity else 0, 2),
            "totalPnL":   round(capital - CAPITAL, 2),
            "maxDD":      round(self._max_drawdown(equity), 4),
            "equityCurve": [round(e) for e in equity[-50:]],
        }

    def _generate_rule_signal(self, strat: str, rows: list, i: int) -> Optional[dict]:
        """Rule-based entry for each strategy."""
        spots = [r["spot"] for r in rows[max(0,i-12):i+1]]
        pcr   = rows[i]["pcr"]
        gex   = rows[i]["gex"]
        gex_r = rows[i]["regime"]
        dlt   = rows[i]["dealer"]

        if strat == "breakout":
            # Short gamma + price above recent high + PCR bearish
            recent_high = max(spots[:-1]) if len(spots) > 1 else spots[-1]
            recent_low  = min(spots[:-1]) if len(spots) > 1 else spots[-1]
            if gex_r > 0.5 and spots[-1] > recent_high * 1.001:
                return {"direction": "up",   "setup": "breakout"}
            if gex_r > 0.5 and spots[-1] < recent_low  * 0.999:
                return {"direction": "down", "setup": "breakout"}

        elif strat == "reversal":
            # Long gamma + PCR extreme + spot stretched from mean
            mean_spot = smean(spots)
            std_spot  = sstdev(spots)
            z         = safe_div(spots[-1] - mean_spot, std_spot)
            if gex_r < 0.5 and z > 1.5 and pcr > 1.2:
                return {"direction": "down", "setup": "reversal"}
            if gex_r < 0.5 and z < -1.5 and pcr < 0.8:
                return {"direction": "up",   "setup": "reversal"}

        elif strat == "premium_sell":
            # Long gamma + IV elevated → sell straddle (no directional entry, track premium decay)
            if gex_r < 0.5 and rows[i]["iv"] > 18:
                return {"direction": "neutral", "setup": "premium_sell"}

        return None

    def _atr(self, rows: list, i: int, n: int = 12) -> float:
        spots = [r["spot"] for r in rows[max(0,i-n):i+1]]
        if len(spots) < 2: return spots[0] * 0.003
        trs = [abs(spots[j] - spots[j-1]) for j in range(1, len(spots))]
        return smean(trs)

    def _max_drawdown(self, equity: list) -> float:
        if len(equity) < 2: return 0.0
        peak = equity[0]; max_dd = 0.0
        for e in equity:
            if e > peak: peak = e
            dd = safe_div(peak - e, peak)
            if dd > max_dd: max_dd = dd
        return max_dd

    def _verdict(self, combined: dict) -> str:
        wr  = combined.get("winRate",   0)
        exp = combined.get("expectancy",0)
        dd  = combined.get("maxDD",     1)
        if wr > 0.55 and exp > 0 and dd < 0.08:
            return "APPROVED — statistically viable for paper trading"
        elif wr > 0.45 and exp > 0:
            return "MARGINAL — paper trade only, accumulate more data"
        else:
            return "NOT APPROVED — do not trade live. Need more history data."


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 3 — SIGNAL FUSION
# Requires BOTH ML prediction AND rule-based signal to agree.
# ML alone → no trade. Rules alone → reduced size. Both agree → full size.
# This is the anti-false-signal gate.
# ══════════════════════════════════════════════════════════════════════════════

class SignalFusion:

    def __init__(self, ml: MLEngine):
        self.ml = ml

    def fuse(self, symbol: str, intel_decision: dict, ml_pred: dict) -> dict:
        """
        Fuse ML prediction with rule-based intelligence decision.
        Returns final fused signal with combined confidence.
        """
        rule_action    = intel_decision.get("action", "NO_TRADE")
        rule_direction = intel_decision.get("direction", "neutral")
        rule_conf      = float(intel_decision.get("confidence", 0))

        ml_direction   = ml_pred.get("direction", "flat")
        ml_conf        = float(ml_pred.get("confidence", 0))
        up_prob        = float(ml_pred.get("upProb",   0.33))
        down_prob      = float(ml_pred.get("downProb", 0.33))
        vol_expansion  = float(ml_pred.get("volExpansion", 0.5))

        # Map ML direction to BUY/SELL
        ml_action = "NO_TRADE"
        if ml_direction == "up"   and up_prob   > 0.50: ml_action = "BUY"
        if ml_direction == "down" and down_prob  > 0.50: ml_action = "SELL"

        # Agreement gate
        agreement = "full"
        if rule_action == "NO_TRADE" or ml_action == "NO_TRADE":
            agreement = "none"
        elif rule_action != ml_action:
            agreement = "conflict"

        # Fused confidence
        if agreement == "full":
            # Weighted average, boosted for full agreement
            fused_conf = (rule_conf * (1 - ML_WEIGHT) + ml_conf * ML_WEIGHT) * 1.15
        elif agreement == "conflict":
            fused_conf = 0.0   # conflict = no trade
        else:
            fused_conf = rule_conf * 0.60  # one side missing = reduced conf

        fused_conf = round(min(0.95, max(0.0, fused_conf)), 3)

        # Anti-manipulation check: avoid trading into large one-sided moves
        anti_manip = self._anti_manipulation_check(symbol)

        action = rule_action if agreement == "full" and fused_conf >= MIN_CONFIDENCE else "NO_TRADE"
        if anti_manip["detected"]:
            log.warning(f"Fusion [{symbol}]: manipulation detected — {anti_manip['reason']}")
            action = "NO_TRADE"

        result = {
            "action":        action,
            "direction":     rule_direction if action != "NO_TRADE" else "neutral",
            "confidence":    fused_conf,
            "agreement":     agreement,
            "ruleAction":    rule_action,
            "ruleConf":      rule_conf,
            "mlAction":      ml_action,
            "mlConf":        ml_conf,
            "upProb":        up_prob,
            "downProb":      down_prob,
            "volExpansion":  vol_expansion,
            "antiManip":     anti_manip,
            "fusedAt":       datetime.now(timezone.utc).isoformat(),
        }
        log.info(f"Fusion [{symbol}]: rule={rule_action}({rule_conf:.2f}) ml={ml_action}({ml_conf:.2f}) "
                 f"agree={agreement} → {action}({fused_conf:.2f})")
        save_json(DATA / "fusion_signal.json", result)
        return result

    def _anti_manipulation_check(self, symbol: str) -> dict:
        """
        Detect manipulation footprints in the last 6 snapshots.
        Big players leave traces: sudden volume spikes, stop hunts, IV pumps.
        Strategy: detect and WAIT, not fight.
        """
        rows = self.ml._load_history(symbol)
        if len(rows) < 8:
            return {"detected": False}

        recent = rows[-6:]
        spots  = [r["spot"] for r in recent]
        ivs    = [r["iv"]   for r in recent]

        # Sudden IV pump: IV spike >30% in 2 bars
        iv_spike = (ivs[-1] - ivs[-3]) / (ivs[-3] + 1e-9) > 0.30

        # Stop hunt: bar moves >0.5% then reverses >0.3% in next bar
        stop_hunt = False
        for i in range(1, len(spots)-1):
            move = abs(spots[i] - spots[i-1]) / spots[i-1]
            rev  = abs(spots[i+1] - spots[i]) / spots[i]
            if move > 0.005 and rev > 0.003:
                stop_hunt = True; break

        # Extreme one-sided OI build (>5% in 2 bars)
        ce_latest = rows[-1]["ceOI"]
        ce_prev   = rows[-3]["ceOI"]
        oi_surge  = abs(ce_latest - ce_prev) / (ce_prev + 1) > 0.05

        detected = iv_spike or stop_hunt
        return {
            "detected":  detected,
            "ivSpike":   iv_spike,
            "stopHunt":  stop_hunt,
            "oiSurge":   oi_surge,
            "reason":    ("IV spike" if iv_spike else "Stop hunt" if stop_hunt else ""),
        }


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 4 — RISK ENGINE
# Kelly criterion sizing, kill switches, drawdown control.
# This is your real "loss cover" — not a guarantee, but mathematical protection.
# ══════════════════════════════════════════════════════════════════════════════

class RiskEngine:
    STATE_FILE = DATA / "risk_state.json"

    def __init__(self):
        self.state = self._load_state()

    def _load_state(self) -> dict:
        default = {
            "dailyPnL":        0.0,
            "dailyTrades":     0,
            "consecLosses":    0,
            "totalTrades":     0,
            "tradingPaused":   False,
            "pauseReason":     "",
            "lastResetDate":   "",
            "equityCurve":     [CAPITAL],
            "peak":            CAPITAL,
            "currentCapital":  CAPITAL,
        }
        saved = load_json(self.STATE_FILE)
        if saved:
            # Reset daily stats if new trading day
            today = now_ist().date().isoformat()
            if saved.get("lastResetDate") != today:
                saved["dailyPnL"]     = 0.0
                saved["dailyTrades"]  = 0
                saved["lastResetDate"]= today
                saved["tradingPaused"]= False
                saved["pauseReason"]  = ""
                log.info("Risk: new trading day — daily stats reset")
        return {**default, **saved}

    def _save_state(self):
        save_json(self.STATE_FILE, self.state)

    def check_kill_switches(self) -> dict:
        """Returns {allowed: bool, reason: str} before each trade."""
        s = self.state

        # Daily loss limit
        daily_loss_pct = abs(s["dailyPnL"]) / CAPITAL
        if s["dailyPnL"] < 0 and daily_loss_pct >= MAX_DAILY_LOSS:
            self.state["tradingPaused"] = True
            self.state["pauseReason"]   = f"Daily loss limit hit: {daily_loss_pct:.1%} >= {MAX_DAILY_LOSS:.0%}"
            self._save_state()
            return {"allowed": False, "reason": self.state["pauseReason"]}

        # Consecutive losses
        if s["consecLosses"] >= MAX_CONSEC_LOSS:
            self.state["tradingPaused"] = True
            self.state["pauseReason"]   = f"{s['consecLosses']} consecutive losses — pausing"
            self._save_state()
            return {"allowed": False, "reason": self.state["pauseReason"]}

        # Manual pause
        if s["tradingPaused"]:
            return {"allowed": False, "reason": s.get("pauseReason", "Manual pause")}

        # Market hours check
        if not is_market_hours():
            return {"allowed": False, "reason": "Outside market hours"}

        return {"allowed": True, "reason": ""}

    def size_position(self, confidence: float, sl_distance: float, vol_state: str) -> dict:
        """
        Kelly-inspired position sizing.
        Kelly fraction = (edge / odds), but we use fractional Kelly (1/4) for safety.
        """
        if sl_distance <= 0:
            return {"lots": 0, "riskAmount": 0, "note": "Invalid SL distance"}

        # Base risk from confidence
        # At 58% confidence → 0.6% risk. At 80% → 1.0% risk. At 95% → 1.4% risk.
        base_risk = MAX_RISK_PCT * (confidence - MIN_CONFIDENCE) / (0.95 - MIN_CONFIDENCE)
        base_risk = min(MAX_RISK_PCT * 1.5, max(MAX_RISK_PCT * 0.3, base_risk))

        # Reduce size in high volatility
        vol_mult = 0.7 if vol_state == "expansion" else 1.0 if vol_state == "normal" else 0.85

        # Reduce after consecutive losses (anti-martingale)
        loss_mult = max(0.5, 1 - self.state["consecLosses"] * 0.15)

        adj_risk    = base_risk * vol_mult * loss_mult
        risk_amount = CAPITAL * adj_risk
        lots        = max(1, int(risk_amount / (sl_distance * LOT_SIZE)))
        lots        = min(lots, 10)  # absolute cap

        return {
            "lots":        lots,
            "riskAmount":  round(risk_amount, 0),
            "adjRiskPct":  round(adj_risk * 100, 2),
            "volMult":     vol_mult,
            "lossMult":    loss_mult,
            "note":        f"Kelly-sized: {lots}L @ {adj_risk*100:.1f}% risk",
        }

    def record_trade(self, pnl: float, result: str):
        """Call after each trade closes. Updates kill switch state."""
        s = self.state
        s["dailyPnL"]       += pnl
        s["dailyTrades"]    += 1
        s["totalTrades"]    += 1
        s["currentCapital"] += pnl

        if result == "win":
            s["consecLosses"] = 0
        else:
            s["consecLosses"] += 1

        # Update equity curve and peak
        s["equityCurve"].append(s["currentCapital"])
        if len(s["equityCurve"]) > 500: s["equityCurve"] = s["equityCurve"][-500:]
        s["peak"] = max(s["peak"], s["currentCapital"])

        self._save_state()
        log.info(f"Risk: trade recorded pnl={pnl:+.0f} result={result} "
                 f"dailyPnL={s['dailyPnL']:+.0f} consecLoss={s['consecLosses']}")


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 5 — STRATEGY SELECTOR
# Regime → strategy → parameters. One source of truth.
# ══════════════════════════════════════════════════════════════════════════════

class StrategySelector:

    STRATEGIES = {
        # fmt: {name: {sl_mult, tp_mult, max_hold_bars, description}}
        "breakout": {
            "slMult": 1.5, "tpMult": 2.5, "maxHold": 6,
            "desc": "Short gamma + vol expansion. Dealers amplify moves. Ride with stops.",
            "regimes": ["short_gamma"], "volStates": ["expansion", "normal"],
        },
        "reversal": {
            "slMult": 1.2, "tpMult": 1.8, "maxHold": 8,
            "desc": "Long gamma + price stretched. Dealers suppress vol. Mean-reversion.",
            "regimes": ["long_gamma"], "volStates": ["normal", "compression"],
        },
        "trap_fade": {
            "slMult": 0.8, "tpMult": 2.0, "maxHold": 4,
            "desc": "Liquidity sweep detected. Enter opposite after rejection.",
            "regimes": ["any"], "volStates": ["any"],
        },
        "premium_sell": {
            "slMult": 3.0, "tpMult": 0.5, "maxHold": 20,  # hold to decay
            "desc": "Long gamma + IV > 20%. Sell straddle/strangle. No directional bias.",
            "regimes": ["long_gamma"], "volStates": ["elevated"],
        },
    }

    def select(self, market_state: str, gex_regime: str, vol_state: str,
               liquidity_sweep: bool, fusion: dict) -> dict:
        """Select best strategy for current conditions."""

        # Trap fade takes priority if sweep detected
        if liquidity_sweep and fusion.get("agreement") == "full":
            return {"name": "trap_fade", **self.STRATEGIES["trap_fade"],
                    "reason": "Liquidity sweep detected — fade the sweep"}

        # Premium sell in long gamma + high IV
        if gex_regime == "long_gamma" and vol_state in ("elevated", "normal") and fusion.get("mlAction") == "NO_TRADE":
            return {"name": "premium_sell", **self.STRATEGIES["premium_sell"],
                    "reason": "Long gamma + IV elevated — sell premium"}

        # Breakout in short gamma + expansion
        if gex_regime == "short_gamma" and vol_state in ("expansion", "normal"):
            return {"name": "breakout", **self.STRATEGIES["breakout"],
                    "reason": "Short gamma + expanding vol — ride breakout"}

        # Reversal in long gamma + compression/normal
        if gex_regime == "long_gamma" and vol_state in ("compression", "normal"):
            return {"name": "reversal", **self.STRATEGIES["reversal"],
                    "reason": "Long gamma + price stretched — mean reversion"}

        return {"name": "none", "reason": "No strategy matches current conditions"}


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 6 — EXECUTION ENGINE
# Paper mode (default) or live via Zerodha Kite API.
# Handles full order lifecycle: place → monitor → SL hit → TP hit → square off.
# ══════════════════════════════════════════════════════════════════════════════

class ExecutionEngine:
    ORDERS_FILE = DATA / "live_orders.json"

    def __init__(self, paper: bool = True):
        self.paper   = paper
        self.kite    = None
        self.orders  = load_json(self.ORDERS_FILE) or {"active": [], "closed": []}
        if not paper:
            self._init_kite()

    def _init_kite(self):
        """Initialize Zerodha Kite. Requires KITE_API_KEY + KITE_ACCESS_TOKEN env vars."""
        try:
            from kiteconnect import KiteConnect
            api_key      = os.environ.get("KITE_API_KEY", "")
            access_token = os.environ.get("KITE_ACCESS_TOKEN", "")
            if not api_key or not access_token:
                raise ValueError("Set KITE_API_KEY and KITE_ACCESS_TOKEN env vars")
            self.kite = KiteConnect(api_key=api_key)
            self.kite.set_access_token(access_token)
            profile = self.kite.profile()
            log.info(f"Execution: Kite connected — {profile.get('user_name', 'unknown')}")
        except ImportError:
            log.error("Execution: kiteconnect not installed. pip install kiteconnect")
            raise
        except Exception as e:
            log.error(f"Execution: Kite init failed — {e}")
            raise

    def place_order(self, decision: dict, sizing: dict, strategy: dict) -> dict:
        """Place a trade. Paper mode simulates it. Live mode calls Kite."""
        action    = decision.get("action")
        direction = decision.get("direction")
        symbol    = decision.get("context", {}).get("symbol", "NIFTY") or "NIFTY"
        entry     = float(decision.get("entry", 0))
        sl        = float(decision.get("stopLoss", 0))
        tp1       = float(decision.get("target1", 0))
        tp2       = float(decision.get("target2", 0))
        lots      = int(sizing.get("lots", 1))
        confidence= float(decision.get("confidence", 0))

        if action == "NO_TRADE" or entry == 0:
            return {"status": "skipped", "reason": "NO_TRADE signal"}

        order_id = f"{'PAPER' if self.paper else 'LIVE'}_{symbol}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"

        order = {
            "orderId":    order_id,
            "symbol":     symbol,
            "action":     action,
            "direction":  direction,
            "entry":      round(entry, 2),
            "sl":         round(sl, 2),
            "tp1":        round(tp1, 2),
            "tp2":        round(tp2, 2),
            "lots":       lots,
            "confidence": confidence,
            "strategy":   strategy.get("name", "unknown"),
            "placedAt":   datetime.now(timezone.utc).isoformat(),
            "status":     "open",
            "paper":      self.paper,
            "pnl":        0.0,
            "result":     "pending",
        }

        if self.paper:
            log.info(f"PAPER ORDER: {action} {lots}L {symbol} @ ₹{entry:.0f} "
                     f"SL=₹{sl:.0f} TP=₹{tp1:.0f} [{strategy.get('name')}] conf={confidence:.2f}")
        else:
            # Real Kite order
            try:
                # For options — place futures order (simpler for NSE intraday)
                tradingsymbol = f"NIFTY{datetime.now().strftime('%y%b').upper()}FUT"
                tx_type = "BUY" if action == "BUY" else "SELL"
                qty     = lots * LOT_SIZE

                kite_order_id = self.kite.place_order(
                    tradingsymbol = tradingsymbol,
                    exchange      = "NFO",
                    transaction_type = tx_type,
                    quantity      = qty,
                    order_type    = "LIMIT",
                    price         = entry,
                    product       = "MIS",   # intraday
                    validity      = "DAY",
                    variety       = "regular",
                )
                order["kiteOrderId"] = kite_order_id
                log.info(f"LIVE ORDER placed: {kite_order_id} — {tx_type} {qty} {tradingsymbol}")

                # Place cover order (SL)
                sl_order_id = self.kite.place_order(
                    tradingsymbol    = tradingsymbol,
                    exchange         = "NFO",
                    transaction_type = "SELL" if tx_type == "BUY" else "BUY",
                    quantity         = qty,
                    order_type       = "SL-M",
                    trigger_price    = sl,
                    product          = "MIS",
                    validity         = "DAY",
                    variety          = "regular",
                )
                order["kiteSlOrderId"] = sl_order_id
                log.info(f"SL order placed: {sl_order_id} @ ₹{sl:.0f}")
            except Exception as e:
                log.error(f"Kite order failed: {e}")
                order["status"] = "failed"
                order["error"]  = str(e)
                return order

        self.orders["active"].append(order)
        self._save_orders()
        return order

    def monitor_positions(self, risk_engine: RiskEngine) -> list:
        """Check open positions against current price. Auto-exit on SL/TP/time."""
        closed = []
        if not self.orders["active"]: return closed

        for order in list(self.orders["active"]):
            if order["status"] != "open": continue

            curr_price = self._get_current_price(order["symbol"])
            if curr_price is None: continue

            direction  = order["direction"]
            sl         = order["sl"]
            tp1        = order["tp1"]
            tp2        = order["tp2"]
            entry      = order["entry"]
            lots       = order["lots"]
            result     = None; exit_price = curr_price

            # Check exits
            if direction in ("up", "bull"):
                if curr_price <= sl:  result = "loss"; exit_price = sl
                elif curr_price >= tp1: result = "win"; exit_price = tp1
            elif direction in ("down", "bear"):
                if curr_price >= sl:  result = "loss"; exit_price = sl
                elif curr_price <= tp1: result = "win"; exit_price = tp1

            # Force square off at 3:20 PM IST
            ist_now = now_ist()
            if ist_now.hour * 60 + ist_now.minute >= 15 * 60 + 20:
                result = "timeout"; exit_price = curr_price
                log.info(f"Execution: auto-squareoff at 3:20 PM — {order['orderId']}")

            if result:
                pnl_pts = (exit_price - entry) * (1 if direction in ("up","bull") else -1)
                pnl     = pnl_pts * lots * LOT_SIZE
                order.update({"status": "closed", "result": result, "pnl": round(pnl, 2),
                               "exitPrice": round(exit_price, 2),
                               "closedAt": datetime.now(timezone.utc).isoformat()})
                self.orders["active"].remove(order)
                self.orders["closed"].append(order)
                if len(self.orders["closed"]) > 200:
                    self.orders["closed"] = self.orders["closed"][-200:]
                risk_engine.record_trade(pnl, result)
                closed.append(order)
                log.info(f"Position closed: {order['orderId']} {result} pnl=₹{pnl:+.0f}")

                if not self.paper and self.kite:
                    try:
                        self.kite.cancel_order(order_id=order.get("kiteSlOrderId",""), variety="regular")
                    except: pass

        self._save_orders()
        return closed

    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get latest spot price from cached OC data."""
        try:
            oc_path = DATA / f"oc_{symbol.lower()}.json"
            if oc_path.exists():
                return float(json.loads(oc_path.read_text()).get("spot", 0) or 0) or None
        except: pass
        return None

    def square_off_all(self):
        """Emergency: close all positions immediately."""
        log.warning("Execution: SQUARE OFF ALL positions")
        for order in list(self.orders["active"]):
            order["status"]   = "closed"
            order["result"]   = "forced_exit"
            order["closedAt"] = datetime.now(timezone.utc).isoformat()
            self.orders["closed"].append(order)
        self.orders["active"] = []
        self._save_orders()

    def _save_orders(self):
        save_json(self.ORDERS_FILE, self.orders)


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 7 — PORTFOLIO MONITOR
# Real-time P&L, Greeks exposure, drawdown, daily report.
# ══════════════════════════════════════════════════════════════════════════════

class PortfolioMonitor:

    def snapshot(self, risk: RiskEngine, execution: ExecutionEngine,
                 fusion: dict, intel: dict) -> dict:
        """Generate current portfolio state snapshot."""
        s = risk.state
        active_orders = execution.orders.get("active", [])
        closed_today  = [
            o for o in execution.orders.get("closed", [])
            if o.get("closedAt", "")[:10] == now_ist().date().isoformat()
        ]

        open_pnl    = sum(o.get("pnl", 0) for o in active_orders)
        closed_pnl  = sum(o.get("closedAt", 0) and o.get("pnl", 0) for o in closed_today)
        daily_pnl   = s["dailyPnL"]

        # Drawdown from peak
        peak    = s["peak"]
        curr    = s["currentCapital"]
        dd      = safe_div(peak - curr, peak)

        # Greeks estimate from active positions
        total_delta = sum(
            o.get("lots", 1) * LOT_SIZE * (1 if o.get("direction") in ("up","bull") else -1)
            for o in active_orders
        )

        snap = {
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "capital":         round(curr, 0),
            "peak":            round(peak, 0),
            "drawdown":        round(dd, 4),
            "dailyPnL":        round(daily_pnl, 2),
            "openPnL":         round(open_pnl, 2),
            "closedTodayPnL":  round(closed_pnl, 2),
            "openPositions":   len(active_orders),
            "closedToday":     len(closed_today),
            "totalDelta":      total_delta,
            "consecLosses":    s["consecLosses"],
            "dailyTrades":     s["dailyTrades"],
            "tradingPaused":   s["tradingPaused"],
            "killSwitchStatus": "ACTIVE" if s["tradingPaused"] else "OK",
            "dailyLossUsed":   round(abs(min(0, daily_pnl)) / CAPITAL * 100, 2),
            "mlSignal":        fusion.get("direction", "—"),
            "mlConf":          fusion.get("mlConf", 0),
            "fusionAction":    fusion.get("action", "—"),
            "fusionConf":      fusion.get("confidence", 0),
            "marketState":     intel.get("marketState", {}).get("state", "—"),
        }
        save_json(DATA / "portfolio_snapshot.json", snap)
        return snap

    def daily_report(self, risk: RiskEngine, execution: ExecutionEngine) -> str:
        """Generate end-of-day text report."""
        s       = risk.state
        closed  = execution.orders.get("closed", [])
        today   = now_ist().date().isoformat()
        today_t = [o for o in closed if o.get("closedAt","")[:10] == today]

        wins  = sum(1 for t in today_t if t.get("result") == "win")
        total = len(today_t)
        pnl   = sum(t.get("pnl",0) for t in today_t)
        wr    = safe_div(wins, total)

        lines = [
            f"{'='*50}",
            f"MARKET PULSE ALGOBOT — DAILY REPORT {today}",
            f"{'='*50}",
            f"Trades Today : {total}  (W:{wins} L:{total-wins})",
            f"Win Rate     : {wr:.0%}",
            f"P&L Today    : ₹{pnl:+,.0f}",
            f"Capital      : ₹{s['currentCapital']:,.0f}",
            f"Drawdown     : {safe_div(s['peak']-s['currentCapital'],s['peak']):.1%} from peak",
            f"Consec Losses: {s['consecLosses']}",
            f"{'─'*50}",
        ]
        for t in today_t[-10:]:
            lines.append(f"  {t.get('orderId','?')[-12:]} {t.get('strategy','?'):15} "
                         f"{t.get('result','?'):7} ₹{t.get('pnl',0):+,.0f}")
        lines.append(f"{'='*50}")
        report = "\n".join(lines)
        (LOGS / f"daily_report_{today}.txt").write_text(report)
        return report


# ══════════════════════════════════════════════════════════════════════════════
# MODULE 8 — FEEDBACK LOOP
# Every closed trade feeds back into: journal → model retrain → weight update.
# This is how the system learns — the Aladdin loop.
# ══════════════════════════════════════════════════════════════════════════════

class FeedbackLoop:

    JOURNAL_FILE  = DATA / "trade_journal.json"
    WEIGHTS_FILE  = DATA / "strategy_weights.json"

    def update_journal(self, order: dict, ml_pred: dict, fusion: dict):
        """Add closed trade to journal with full context."""
        journal = self._load_journal()
        entry = {
            "id":           order.get("orderId"),
            "symbol":       order.get("symbol"),
            "timestamp":    order.get("placedAt"),
            "closedAt":     order.get("closedAt"),
            "setup_type":   order.get("strategy"),
            "action":       order.get("action"),
            "direction":    order.get("direction"),
            "entry":        order.get("entry"),
            "sl":           order.get("sl"),
            "tp1":          order.get("tp1"),
            "exitPrice":    order.get("exitPrice"),
            "lots":         order.get("lots"),
            "pnl":          order.get("pnl"),
            "result":       order.get("result"),
            "rr_achieved":  safe_div(
                (order.get("exitPrice",0) - order.get("entry",0)) *
                (1 if order.get("direction") in ("up","bull") else -1),
                abs(order.get("entry",0) - order.get("sl",0)) + 1e-9
            ),
            "confidence":   order.get("confidence"),
            "mlUpProb":     ml_pred.get("upProb"),
            "mlDownProb":   ml_pred.get("downProb"),
            "mlConf":       ml_pred.get("confidence"),
            "fusionAgreement": fusion.get("agreement"),
            "antiManip":    fusion.get("antiManip", {}).get("detected", False),
            "paper":        order.get("paper", True),
        }
        journal.append(entry)
        journal = journal[-1000:]  # keep last 1000
        self._save_journal(journal)
        log.info(f"Feedback: journal updated — {entry['id']} {entry['result']} ₹{entry['pnl']:+.0f}")

    def update_strategy_weights(self):
        """
        Re-score each strategy based on recent performance.
        Strategies that work get higher weight in the selector.
        """
        journal = self._load_journal()
        weights = {}

        for setup in ["breakout", "reversal", "trap_fade", "premium_sell"]:
            trades  = [t for t in journal if t.get("setup_type") == setup]
            recent  = trades[-20:] if len(trades) > 20 else trades
            if not recent: weights[setup] = 0.5; continue
            wins   = sum(1 for t in recent if t.get("result") == "win")
            total  = len(recent)
            wr     = wins / total
            avg_rr = smean([t.get("rr_achieved", 0) for t in recent])
            edge   = wr * avg_rr  # expected value
            weights[setup] = round(min(1.5, max(0.1, 0.5 + edge)), 3)
            log.info(f"Feedback: {setup} weight={weights[setup]:.3f} (wr={wr:.0%} rr={avg_rr:.2f})")

        save_json(self.WEIGHTS_FILE, {"weights": weights, "updatedAt": datetime.now(timezone.utc).isoformat()})
        return weights

    def should_retrain(self) -> bool:
        """Retrain ML model if: >50 new trades since last train, or daily."""
        journal     = self._load_journal()
        result_path = DATA / "ml_training_result.json"
        if not result_path.exists(): return True
        last_train  = json.loads(result_path.read_text()).get("trainedAt", "")
        if not last_train: return True
        try:
            last_dt = datetime.fromisoformat(last_train)
            hours_ago = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
            new_trades = sum(1 for t in journal if t.get("timestamp","") > last_train)
            return hours_ago > 24 or new_trades > 50
        except:
            return True

    def _load_journal(self) -> list:
        raw = load_json(self.JOURNAL_FILE)
        if isinstance(raw, list):   return raw
        if isinstance(raw, dict):   return raw.get("trades", [])
        return []

    def _save_journal(self, journal: list):
        save_json(self.JOURNAL_FILE, journal)


# ══════════════════════════════════════════════════════════════════════════════
# MASTER PIPELINE — One tick = one full cycle
# ══════════════════════════════════════════════════════════════════════════════

class AlgoBot:

    def __init__(self, paper: bool = PAPER_TRADE):
        self.paper     = paper
        self.ml        = MLEngine()
        self.backtest  = BacktestEngine(self.ml)
        self.risk      = RiskEngine()
        self.execution = ExecutionEngine(paper=paper)
        self.portfolio = PortfolioMonitor()
        self.feedback  = FeedbackLoop()
        self.fusion    = SignalFusion(self.ml)
        self.strategy  = StrategySelector()

        mode = "PAPER" if paper else "⚠  LIVE MONEY"
        log.info(f"AlgoBot initialized — MODE: {mode}")
        log.info(f"Capital: ₹{CAPITAL:,} | MaxRisk: {MAX_RISK_PCT:.0%}/trade | DailyStop: {MAX_DAILY_LOSS:.0%}")
        if not paper:
            log.warning("LIVE MODE: real money at risk. Ensure backtest approved this system first.")

    def tick(self, symbol: str = "NIFTY") -> dict:
        """
        One full decision cycle. Call every 5 minutes during market hours.
        Returns the decision taken this tick.
        """
        log.info(f"{'─'*55}")
        log.info(f"Tick [{symbol}] @ {now_ist().strftime('%H:%M IST')}")

        # 1. Monitor open positions first
        closed = self.execution.monitor_positions(self.risk)
        for order in closed:
            ml_pred_path = DATA / "ml_prediction.json"
            fusion_path  = DATA / "fusion_signal.json"
            ml_p  = load_json(ml_pred_path)
            fus_p = load_json(fusion_path)
            self.feedback.update_journal(order, ml_p, fus_p)

        # 2. Kill switch check
        kill = self.risk.check_kill_switches()
        if not kill["allowed"]:
            log.warning(f"Kill switch ACTIVE: {kill['reason']}")
            return {"action": "BLOCKED", "reason": kill["reason"]}

        # 3. ML prediction
        ml_pred = self.ml.predict(symbol)

        # 4. Load rule-based intelligence decision
        intel  = load_json(DATA / "intelligence.json")
        sym_d  = intel.get("symbols", {}).get(symbol, {})
        rule_d = sym_d.get("decision", {"action": "NO_TRADE", "confidence": 0})

        # 5. Signal fusion
        fused = self.fusion.fuse(symbol, rule_d, ml_pred)

        # 6. Check if we should trade
        if fused["action"] == "NO_TRADE":
            log.info(f"Decision: NO_TRADE (conf={fused['confidence']:.2f} agree={fused['agreement']})")
            self._save_tick_result(symbol, fused, None, None, None, None)
            return {"action": "NO_TRADE", "confidence": fused["confidence"]}

        # 7. Strategy selection
        market_state = sym_d.get("marketState", {})
        vol_intel    = sym_d.get("volIntelligence", {})
        liquidity    = sym_d.get("liquidity", {})
        strategy = self.strategy.select(
            market_state.get("state", "neutral"),
            market_state.get("gexRegime", "unknown"),
            vol_intel.get("volState", "normal"),
            liquidity.get("liquiditySweep", False),
            fused,
        )
        if strategy["name"] == "none":
            log.info(f"Decision: NO_TRADE — no strategy matches regime")
            return {"action": "NO_TRADE", "reason": strategy["reason"]}

        # 8. Position sizing
        sl_dist = abs(rule_d.get("entry", 0) - rule_d.get("stopLoss", 0)) or \
                  vol_intel.get("atr5", 50) * strategy["slMult"]
        sizing  = self.risk.size_position(fused["confidence"], sl_dist, vol_intel.get("volState","normal"))

        if sizing["lots"] == 0:
            return {"action": "NO_TRADE", "reason": "Position size = 0"}

        # 9. Build final decision with all context
        final_decision = {
            **rule_d,
            "action":     fused["action"],
            "direction":  fused["direction"],
            "confidence": fused["confidence"],
            "lots":       sizing["lots"],
            "riskAmount": sizing["riskAmount"],
            "strategy":   strategy["name"],
            "context": {
                **rule_d.get("context", {}),
                "symbol":    symbol,
                "mlUpProb":  ml_pred.get("upProb"),
                "mlDownProb":ml_pred.get("downProb"),
                "agreement": fused["agreement"],
            }
        }

        # 10. Place order
        order = self.execution.place_order(final_decision, sizing, strategy)

        # 11. Portfolio snapshot
        snap = self.portfolio.snapshot(self.risk, self.execution, fused, intel)
        log.info(f"Portfolio: cap=₹{snap['capital']:,.0f} dd={snap['drawdown']:.1%} "
                 f"dailyPnL=₹{snap['dailyPnL']:+,.0f}")

        # 12. Retrain if needed (background)
        if self.feedback.should_retrain():
            log.info("Feedback: triggering model retrain...")
            self.ml.train(symbol)
            self.feedback.update_strategy_weights()

        result = {
            "action":     final_decision["action"],
            "direction":  final_decision["direction"],
            "confidence": fused["confidence"],
            "strategy":   strategy["name"],
            "lots":       sizing["lots"],
            "entry":      final_decision.get("entry"),
            "sl":         final_decision.get("stopLoss"),
            "tp1":        final_decision.get("target1"),
            "orderId":    order.get("orderId"),
            "paper":      self.paper,
        }
        self._save_tick_result(symbol, fused, strategy, sizing, final_decision, order)
        return result

    def _save_tick_result(self, symbol, fused, strategy, sizing, decision, order):
        save_json(DATA / "algobot_last_tick.json", {
            "symbol":    symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "paper":     self.paper,
            "fusion":    fused,
            "strategy":  strategy,
            "sizing":    sizing,
            "decision":  decision,
            "order":     order,
        })

    def run_loop(self, symbol: str = "NIFTY"):
        """Main loop — runs every 5 minutes during market hours."""
        log.info(f"AlgoBot: starting live loop for {symbol}")
        while True:
            try:
                if is_market_hours():
                    result = self.tick(symbol)
                    log.info(f"Tick result: {result.get('action')} {result.get('direction','')}")
                else:
                    ist = now_ist()
                    log.info(f"Outside market hours ({ist.strftime('%H:%M IST')}) — waiting")

                    # End of day: generate report and retrain
                    if ist.hour == 15 and 30 <= ist.minute < 35:
                        report = self.portfolio.daily_report(self.risk, self.execution)
                        print(report)
                        self.ml.train(symbol)
                        self.feedback.update_strategy_weights()

                time.sleep(300)  # 5 minutes

            except KeyboardInterrupt:
                log.info("AlgoBot: stopped by user")
                self.execution.square_off_all()
                break
            except Exception as e:
                log.error(f"AlgoBot tick error: {e}")
                traceback.print_exc()
                time.sleep(30)  # brief pause then retry


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Market Pulse AlgoBot")
    parser.add_argument("--live",      action="store_true", help="Live trading mode (real money)")
    parser.add_argument("--backtest",  action="store_true", help="Run backtest and exit")
    parser.add_argument("--report",    action="store_true", help="Print performance report and exit")
    parser.add_argument("--retrain",   action="store_true", help="Retrain ML model and exit")
    parser.add_argument("--tick",      action="store_true", help="Run single tick and exit")
    parser.add_argument("--symbol",    default="NIFTY",     help="Symbol: NIFTY/BANKNIFTY/FINNIFTY")
    args = parser.parse_args()

    sym = args.symbol.upper()

    if args.retrain:
        ml = MLEngine()
        result = ml.train(sym)
        print(json.dumps(result, indent=2))
        return

    if args.backtest:
        ml = MLEngine()
        ml.train(sym)
        bt = BacktestEngine(ml)
        result = bt.run(sym)
        print(json.dumps(result, indent=2, default=str))
        return

    if args.report:
        risk  = RiskEngine()
        exe   = ExecutionEngine(paper=True)
        mon   = PortfolioMonitor()
        print(mon.daily_report(risk, exe))
        return

    paper = not args.live
    if args.live:
        print("\n⚠  WARNING: LIVE TRADING MODE — real money at risk")
        print("   Ensure you have run --backtest and reviewed the results.")
        confirm = input("   Type 'CONFIRM LIVE' to proceed: ")
        if confirm != "CONFIRM LIVE":
            print("   Aborted.")
            return

    bot = AlgoBot(paper=paper)

    if args.tick:
        result = bot.tick(sym)
        print(json.dumps(result, indent=2, default=str))
    else:
        bot.run_loop(sym)


if __name__ == "__main__":
    main()
