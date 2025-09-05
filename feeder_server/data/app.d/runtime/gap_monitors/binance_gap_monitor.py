# runtime/gap_monitors/binance_gap_monitor.py
from __future__ import annotations
import time
from typing import Dict, Optional
from runtime.dh_thread import spawn, DHThread
from runtime.eventlog import emit_event
from core.contracts import EventStore, JournalStore, Tick
from runtime.backfill.planner import BackfillPlanner, Gap

_WM_BATCH_SCOPE = "gapmon:binance:batch"
_WM_LASTSEQ_SCOPE_PREFIX = "gapmon:binance:last_seq:"  # + symbol

class BinanceGapMonitor:
    """
    Scans persisted envelopes from EventStore to detect gaps in (provider='binance', stream='trades') Tick.seq.
    Emits gaps into BackfillPlanner (which mirrors them to UI and tasks).
    Uses JournalStore.watermarks to persist cursors and per-symbol last_seq across restarts.
    """

    def __init__(self, *, event_store: EventStore, journal: JournalStore, planner: BackfillPlanner, poll_delay_s: float = 0.25):
        self.event_store = event_store
        self.journal = journal
        self.planner = planner
        self.poll_delay_s = float(poll_delay_s)
        self._thread: Optional[DHThread] = None
        self._running = False
        self._last_seq: Dict[str, int] = {}   # symbol -> last seen seq

    # ----- lifecycle --------------------------------------------------------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = spawn("feeder", "binance_gapmon", "monitor", self._run)

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.stop()
            try:
                self._thread.join(timeout=2.0)
            except Exception:
                pass

    # ----- helpers ----------------------------------------------------------
    def _get_last_seq(self, symbol: str) -> Optional[int]:
        if symbol in self._last_seq:
            return self._last_seq[symbol]
        try:
            v = self.journal.get_watermark(f"{_WM_LASTSEQ_SCOPE_PREFIX}{symbol}")
            self._last_seq[symbol] = int(v) if v > 0 else None  # type: ignore[return-value]
            return self._last_seq[symbol]
        except Exception:
            return None

    def _set_last_seq(self, symbol: str, seq: int) -> None:
        self._last_seq[symbol] = int(seq)
        try:
            self.journal.set_watermark(int(seq), f"{_WM_LASTSEQ_SCOPE_PREFIX}{symbol}")
        except Exception:
            pass

    # ----- main loop --------------------------------------------------------
    def _run(self, stop_event):
        # starting point: resume from last processed batch id
        try:
            next_batch = int(self.journal.get_watermark(_WM_BATCH_SCOPE)) + 1
        except Exception:
            next_batch = 1

        emit_event("feeder", "binance_gapmon", "monitor", "INFO", "START", f"resume at batch {next_batch}")

        while not stop_event.is_set() and self._running:
            envs = []
            try:
                envs = self.event_store.next_from(next_batch, 256)
            except Exception as e:
                emit_event("feeder", "binance_gapmon", "monitor", "ERROR", "STORE_READ_ERR", repr(e))
                time.sleep(0.5)

            if not envs:
                time.sleep(self.poll_delay_s)
                continue

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
                    last = self._get_last_seq(sym)
                    if last is None:
                        # first observation for this symbol; initialize and continue
                        self._set_last_seq(sym, seq)
                        continue
                    if seq > last + 1:
                        # gap detected: (last+1 ... seq-1)
                        try:
                            self.planner.add_gap(Gap(
                                provider="binance",
                                symbol=sym,
                                start_id=last + 1,
                                end_id=seq - 1,
                                discovered_at_ns=t.ts_ns,
                                priority=0,
                            ))
                        except Exception as e:
                            emit_event("feeder", f"binance:{sym}", "monitor", "ERROR", "GAP_ENQUEUE_ERR", repr(e))
                    # advance last
                    if seq > last:
                        self._set_last_seq(sym, seq)

            if last_bid is not None:
                next_batch = last_bid + 1
                # persist cursor
                try:
                    self.journal.set_watermark(last_bid, _WM_BATCH_SCOPE)
                except Exception:
                    pass
