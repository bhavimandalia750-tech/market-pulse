"""
cleanup_candles.py
------------------
Deletes candle JSON files older than 7 days from data/candles/
and rebuilds the candles index.json.

Run manually:   python cleanup_candles.py
Run via CI/CD:  triggered by cleanup-candles.yml every Sunday at 00:00 UTC
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────
CANDLE_DIR = Path("data/candles")
MAX_CANDLE_DAYS = 7          # keep files for this many days
# ─────────────────────────────────────────────────────────────────────────


def cleanup_candles() -> int:
    """Delete candle files older than MAX_CANDLE_DAYS. Returns count removed."""
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=MAX_CANDLE_DAYS)
    ).strftime("%Y-%m-%d")

    print(f"Cleanup: removing candle files with date < {cutoff}")

    removed = 0
    for f in CANDLE_DIR.glob("*_*.json"):
        if f.name == "index.json":
            continue
        try:
            # expected filename pattern: SYMBOL_YYYY-MM-DD.json
            date_part = f.stem.split("_", 1)[1]
            if date_part < cutoff:
                f.unlink()
                print(f"  Deleted: {f.name}")
                removed += 1
        except Exception as exc:
            print(f"  Skipped {f.name}: {exc}")

    print(f"Cleanup complete — {removed} file(s) removed.")
    return removed


def rebuild_candle_index() -> None:
    """Rewrite data/candles/index.json from the remaining files."""
    index: dict[str, list[str]] = {}

    for f in sorted(CANDLE_DIR.glob("*_*.json")):
        if f.name == "index.json":
            continue
        try:
            sym, date_part = f.stem.split("_", 1)
            index.setdefault(sym, []).append(date_part)
        except Exception:
            pass

    # keep only the most recent MAX_CANDLE_DAYS dates per symbol
    for sym in index:
        index[sym] = sorted(set(index[sym]))[-MAX_CANDLE_DAYS:]

    payload = {
        "symbols": index,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }
    (CANDLE_DIR / "index.json").write_text(json.dumps(payload, indent=2))
    print(f"Index rebuilt: {index}")


if __name__ == "__main__":
    if not CANDLE_DIR.exists():
        print(f"ERROR: {CANDLE_DIR} does not exist. Run from the repo root.")
        raise SystemExit(1)

    cleanup_candles()
    rebuild_candle_index()
