#!/usr/bin/env python3
"""
Market Pulse — History Cleanup
================================
Deletes snapshots in data/history/ that are older than 7 days.
Runs daily via GitHub Actions (cleanup.yml).

File naming convention expected:
    data/history/<stem>_<YYYY-MM-DDTHHMM>.json
    e.g. oc_nifty_2026-03-13T0915.json

Logic:
  - Parse the timestamp from the filename
  - If (now_utc - file_timestamp) > 7 days → delete permanently
  - Files with unparseable names are left untouched (never delete unknown files)
  - Prints a full report: kept / deleted / skipped / errors
"""

import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

HIST          = Path("data/history")
RETENTION_DAYS = 7
DRY_RUN       = "--dry-run" in sys.argv   # pass --dry-run to preview without deleting

# Matches: any_name_2026-03-13T0915.json
TS_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2}T\d{4})\.json$")


def parse_ts(filename: str) -> datetime | None:
    """Extract UTC datetime from filename. Returns None if pattern doesn't match."""
    m = TS_PATTERN.search(filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%dT%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def run() -> dict:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=RETENTION_DAYS)

    print(f"\n{'='*55}")
    print(f"HISTORY CLEANUP — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Retention: {RETENTION_DAYS} days  |  Cutoff: {cutoff.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Mode: {'DRY RUN (no files deleted)' if DRY_RUN else 'LIVE (permanent deletion)'}")
    print(f"{'='*55}\n")

    if not HIST.exists():
        print("data/history/ does not exist — nothing to clean.")
        return {"deleted": 0, "kept": 0, "skipped": 0, "errors": 0}

    files = sorted(HIST.glob("*.json"))
    print(f"Total files found: {len(files)}\n")

    stats = {"deleted": 0, "kept": 0, "skipped": 0, "errors": 0}
    deleted_files = []
    kept_files    = []

    for f in files:
        ts = parse_ts(f.name)

        if ts is None:
            print(f"  SKIP  (unparseable name): {f.name}")
            stats["skipped"] += 1
            continue

        age_days = (now - ts).total_seconds() / 86400

        if ts < cutoff:
            # Older than retention — delete
            if DRY_RUN:
                print(f"  [DRY]  would delete ({age_days:.1f}d old): {f.name}")
            else:
                try:
                    f.unlink()
                    print(f"  DEL   ({age_days:.1f}d old): {f.name}")
                    deleted_files.append(f.name)
                except Exception as e:
                    print(f"  ERROR deleting {f.name}: {e}")
                    stats["errors"] += 1
                    continue
            stats["deleted"] += 1
        else:
            kept_files.append(f.name)
            stats["kept"] += 1

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n{'─'*55}")
    print(f"SUMMARY")
    print(f"  Deleted : {stats['deleted']}")
    print(f"  Kept    : {stats['kept']}")
    print(f"  Skipped : {stats['skipped']}  (unparseable filenames)")
    print(f"  Errors  : {stats['errors']}")
    print(f"{'─'*55}")

    if kept_files:
        oldest_kept = kept_files[0]
        newest_kept = kept_files[-1]
        print(f"  Oldest kept : {oldest_kept}")
        print(f"  Newest kept : {newest_kept}")

    # Estimate storage used by remaining files
    remaining = list(HIST.glob("*.json"))
    total_bytes = sum(f.stat().st_size for f in remaining if f.is_file())
    print(f"  Storage used by history/ : {total_bytes / 1024 / 1024:.2f} MB ({len(remaining)} files)")
    print(f"{'='*55}\n")

    # Write a cleanup log for the dashboard to display
    log = {
        "lastRun"      : now.isoformat(),
        "retentionDays": RETENTION_DAYS,
        "cutoff"       : cutoff.isoformat(),
        "deleted"      : stats["deleted"],
        "kept"         : stats["kept"],
        "skipped"      : stats["skipped"],
        "errors"       : stats["errors"],
        "storageMB"    : round(total_bytes / 1024 / 1024, 3),
        "fileCount"    : len(remaining),
        "oldestKept"   : kept_files[0]  if kept_files else None,
        "newestKept"   : kept_files[-1] if kept_files else None,
        "dryRun"       : DRY_RUN,
    }
    Path("data/cleanup_log.json").write_text(json.dumps(log, indent=2))
    print("Wrote data/cleanup_log.json")

    return stats


if __name__ == "__main__":
    result = run()
    if result["errors"] > 0:
        print(f"\nWARNING: {result['errors']} deletion error(s). Check log above.")
        sys.exit(1)
