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

# Set >0 to always backfill a small recent slice on the first sighting of a symbol
_BOOTSTRAP_LOOKBACK_IDS = 0  # e.g., 10_000 if you want a startup lookback

class BinanceGapMonitor:
    """
    Scans committed envelopes to detect gaps in (provider='binance', stream='trades') Tick.seq.
    Emits gaps into BackfillPlanner (which mirrors them to UI and tasks).
    Persists:
      - batch cursor in journal watermarks (_WM_BATCH_SCOPE)
      - per-symbol last_seq in journal watermarks (_WM_LASTSEQ_SCOPE_PREFIX + symbol)

    NOTE: Prefers Journal (committed history). Falls back to EventStore if journal
    does not expose next_committed_from(start, limit).
    """

    def __init__(
        self,
        *,
        journal: JournalStore,
        planner: BackfillPlanner,
        event_store: Optional[EventStore] = None,
        poll_delay_s: float = 0.25,
    ):
        self.journal = journal
        self.planner = planner
        self.event_store = event_store  # optional, only for fallback
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
            self._last_seq[symbol] = int(v) if v and int(v) > 0 else None  # type: ignore[return-value]
            return self._last_seq[symbol]
        except Exception:
            return None

    def _set_last_seq(self, symbol: str, seq: int) -> None:
        self._last_seq[symbol] = int(seq)
        try:
            self.journal.set_watermark(int(seq), f"{_WM_LASTSEQ_SCOPE_PREFIX}{symbol}")
        except Exception:
            pass

    def _fetch_envs(self, start_batch: int, limit: int):
        # Prefer Journal scanning by batch (committed). Fall back to EventStore if needed.
        try:
            return self.journal.next_committed_from(start_batch, limit)  # type: ignore[attr-defined]
        except AttributeError:
            if self.event_store is None:
                return []
            return self.event_store.next_from(start_batch, limit)

    # ----- main loop --------------------------------------------------------
    def _run(self, stop_event):
        # starting point: resume from last processed batch id (committed)
        try:
            last = int(self.journal.get_watermark(_WM_BATCH_SCOPE))
        except Exception:
            last = 0
        next_batch = last + 1

        emit_event("feeder", "binance_gapmon", "monitor", "INFO", "START",
                   f"resume at batch {last}")

        while not stop_event.is_set() and self._running:
            try:
                envs = self._fetch_envs(next_batch, 256)
            except Exception as e:
                emit_event("feeder", "binance_gapmon", "monitor", "ERROR", "STORE_READ_ERR", repr(e))
                time.sleep(0.5)
                continue

            if not envs:
                time.sleep(self.poll_delay_s)
                continue

            last_bid = None
            # Process each committed envelope in order
            for env in envs:
                last_bid = env.batch_id
                # Scan only Binance trades rows
                for t in env.rows:
                    if t.provider != "binance" or t.stream != "trades":
                        continue
                    if t.seq is None:
                        continue

                    sym = t.symbol
                    seq = int(t.seq)
                    last_seq = self._get_last_seq(sym)

                    if last_seq is None:
                        # First sighting for the symbol
                        if _BOOTSTRAP_LOOKBACK_IDS > 0 and seq > _BOOTSTRAP_LOOKBACK_IDS:
                            # Optional: seed a lookback slice [seq-L, seq-1]
                            try:
                                self.planner.add_gap(Gap(
                                    provider="binance",
                                    symbol=sym,
                                    start_id=seq - _BOOTSTRAP_LOOKBACK_IDS,
                                    end_id=seq - 1,                # off-by-one safe
                                    discovered_at_ns=t.ts_ns,
                                    priority=0,
                                ))
                            except Exception as e:
                                emit_event("feeder", f"binance:{sym}", "monitor", "ERROR", "GAP_ENQUEUE_ERR", repr(e))
                        self._set_last_seq(sym, seq)
                        continue

                    # Gap detection: strictly greater than last+1
                    if seq > last_seq + 1:
                        gap_start = last_seq + 1
                        gap_end   = seq - 1   # <-- off-by-one fix (do NOT include current seq)
                        if gap_end >= gap_start:
                            try:
                                self.planner.add_gap(Gap(
                                    provider="binance",
                                    symbol=sym,
                                    start_id=int(gap_start),
                                    end_id=int(gap_end),
                                    discovered_at_ns=t.ts_ns,
                                    priority=0,
                                ))
                            except Exception as e:
                                emit_event("feeder", f"binance:{sym}", "monitor", "ERROR", "GAP_ENQUEUE_ERR", repr(e))

                    # advance last_seq
                    if seq > last_seq:
                        self._set_last_seq(sym, seq)

            if last_bid is not None:
                next_batch = last_bid + 1
                # persist batch cursor after successful processing
                try:
                    self.journal.set_watermark(last_bid, _WM_BATCH_SCOPE)
                except Exception:
                    pass
