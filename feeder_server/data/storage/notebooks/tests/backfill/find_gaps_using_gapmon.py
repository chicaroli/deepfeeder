# feeder_server/data/storage/notebooks/find_gaps_using_gapmon.py
"""Use BinanceGapMonitor helper functions to scan committed journal envelopes for gaps.

This script performs a one-shot dry-run of the gap-monitor logic without
starting background threads or enqueuing backfill tasks. It:
  - reads the journal batch cursor watermark (_WM_BATCH_SCOPE)
  - scans committed envelopes via monitor._fetch_envs()
  - reuses monitor._get_last_seq() to seed per-symbol last_seq (read-only)
  - detects gaps exactly as the monitor would and reports them

Run inside the Deephaven app container Python console or as a script:
    python /data/storage/notebooks/find_gaps_using_gapmon.py

This is read-only: it does not call planner.add_gap() nor journal.set_watermark().
"""
from __future__ import annotations
import time
from typing import List, Tuple

import deepfeeder as df
from runtime.gap_monitors.binance_gap_monitor import BinanceGapMonitor, _WM_BATCH_SCOPE
from runtime.backfill.planner import BackfillPlanner, Gap


class RecordingPlanner:
    def __init__(self):
        self.gaps: List[Gap] = []

    def add_gap(self, gap: Gap):
        # record but do not attempt to schedule tasks
        self.gaps.append(gap)
        return []


def scan_once(journal, limit: int = 256) -> List[Tuple[str, int, int, int]]:
    """Scan committed batches once and return list of (symbol, start, end, batch_id)."""
    planner = RecordingPlanner()
    monitor = BinanceGapMonitor(journal=journal, planner=planner, event_store=None, poll_delay_s=0.01)

    try:
        last = int(journal.get_watermark(_WM_BATCH_SCOPE))
    except Exception:
        last = 0
    next_batch = last + 1

    found = []
    while True:
        envs = monitor._fetch_envs(next_batch, limit)
        if not envs:
            break
        last_bid = None
        for env in envs:
            last_bid = env.batch_id
            for t in env.rows:
                if t.provider != "binance" or t.stream != "trades":
                    continue
                if t.seq is None:
                    continue
                sym = t.symbol
                seq = int(t.seq)
                last_seq = monitor._get_last_seq(sym)

                if last_seq is None:
                    # First sighting: seed local last_seq (do NOT persist)
                    monitor._last_seq[sym] = seq
                    continue

                if seq > last_seq + 1:
                    gap_start = last_seq + 1
                    gap_end = seq - 1
                    found.append((sym, int(gap_start), int(gap_end), int(env.batch_id)))
                    # record in monitoring planner for parity (but planner doesn't enqueue)
                    try:
                        planner.add_gap(Gap(provider="binance", symbol=sym, start_id=int(gap_start), end_id=int(gap_end), discovered_at_ns=t.ts_ns))
                    except Exception:
                        pass

                if seq > last_seq:
                    monitor._last_seq[sym] = seq

        if last_bid is None:
            break
        next_batch = last_bid + 1
    return found


def main():
    services = df.get_services_container()
    journal = services.get("journal_store")
    print("Scanning committed journal envelopes for Binance trade gaps (dry-run)...")
    gaps = scan_once(journal)
    if not gaps:
        print("No gaps found by gap-monitor scan.")
    else:
        print(f"Found {len(gaps)} gap(s):")
        for sym, a, b, bid in gaps:
            print(f"  {sym}: [{a},{b}] discovered_in_batch={bid}")
    print("Done.")


if __name__ == "__main__":
    main()

