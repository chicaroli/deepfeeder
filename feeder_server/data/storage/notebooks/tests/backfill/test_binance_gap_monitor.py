# feeder_server/data/storage/notebooks/test_binance_gap_monitor.py
"""Quick Deephaven console/script test for BinanceGapMonitor.

Run inside the Deephaven app container Python console or as a script:
    python test_binance_gap_monitor.py

This script creates a mock Journal and a recording BackfillPlanner and
invokes the BinanceGapMonitor._run loop in a background thread with a
short-lived set of envelopes. It runs two scenarios:
  1) contiguous seqs -> no gaps should be emitted
  2) a jump in seq -> one gap should be emitted

If the monitor emits gaps when it shouldn't, the script will print
results and assert accordingly to help debugging.
"""
import threading
import time
from runtime.gap_monitors.binance_gap_monitor import BinanceGapMonitor
from core.contracts import Tick, Envelope


class MockJournal:
    """Minimal JournalStore shim for testing BinanceGapMonitor."""
    def __init__(self, envelopes):
        # envelopes: list[Envelope]
        self.envelopes = {env.batch_id: env for env in envelopes}
        self._watermarks = {}

    def get_watermark(self, scope: str = "global"):
        return self._watermarks.get(scope, 0)

    def set_watermark(self, value: int, scope: str = "global") -> None:
        self._watermarks[scope] = int(value)

    def next_committed_from(self, start_batch_inclusive: int, limit: int):
        # return envelopes with batch_id >= start, in ascending order
        keys = sorted(k for k in self.envelopes.keys() if k >= start_batch_inclusive)
        out = [self.envelopes[k] for k in keys[:limit]]
        # emulate consumption by removing returned batches so subsequent calls don't repeat
        for k in keys[:limit]:
            del self.envelopes[k]
        return out


class RecordingPlanner:
    """Planner that records gaps enqueued by the monitor."""
    def __init__(self):
        self.gaps = []

    def add_gap(self, gap):
        # record and return empty task list ( planner usually slices into tasks )
        self.gaps.append(gap)
        return []


def make_tick(symbol: str, seq: int, ts_ns: int = None):
    if ts_ns is None:
        ts_ns = time.time_ns()
    return Tick(provider="binance", stream="trades", symbol=symbol, ts_ns=ts_ns, seq=seq, payload={}, is_final=True)


def make_env(batch_id: int, ticks):
    return Envelope(batch_id=batch_id, produced_at_ns=time.time_ns(), first_offset=None, last_offset=None, rows=ticks)


def run_scenario(envelopes, expect_gaps, timeout_s=1.0):
    journal = MockJournal(envelopes)
    planner = RecordingPlanner()
    monitor = BinanceGapMonitor(journal=journal, planner=planner, event_store=None, poll_delay_s=0.01)
    # The monitor loop checks self._running; when invoking _run directly in tests
    # we must mark it running so the loop body executes.
    monitor._running = True

    stop_event = threading.Event()
    thr = threading.Thread(target=lambda: monitor._run(stop_event), name="bg-gapmon")
    thr.daemon = True
    thr.start()

    # Wait briefly for processing
    time.sleep(timeout_s)
    # stop the loop
    stop_event.set()
    thr.join(timeout=1.0)

    found = len(planner.gaps)
    print(f"Scenario result: found {found} gap(s). Expected: {expect_gaps}.")
    for g in planner.gaps:
        print(f"  GAP: provider={g.provider} symbol={g.symbol} start={g.start_id} end={g.end_id}")

    # basic assertion to help detect regressions
    assert found == expect_gaps, f"Expected {expect_gaps} gaps, found {found}"


def main():
    symbol = "BTCUSDT"

    # Scenario 1: contiguous sequences -> NO gaps
    envs1 = [
        make_env(1, [make_tick(symbol, 1), make_tick(symbol, 2)]),
        make_env(2, [make_tick(symbol, 3), make_tick(symbol, 4)]),
    ]
    run_scenario(envs1, expect_gaps=0)

    # Scenario 2: a jump (missing 2-3) -> ONE gap (2..3)
    envs2 = [
        make_env(10, [make_tick(symbol, 1)]),
        make_env(11, [make_tick(symbol, 4)]),
    ]
    run_scenario(envs2, expect_gaps=1)

    print("All scenarios completed.")


if __name__ == "__main__":
    main()
