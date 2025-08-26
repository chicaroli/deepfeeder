"""
GapDetector: Scans Deephaven (dh) tables for missing intervals and emits gap tasks for backfill.

Source of truth is the live dh tables, not the journal.
"""

from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Set
from threading import Lock
import time
from runtime.eventlog import emit_event
from runtime.dh_thread import spawn



class GapFiller:
    """
    Detects gaps in Deephaven tables and backfills missing data for a given symbol, exchange (optional), and key column (e.g., TradeID).
    Tracks a last_watermark to avoid refilling already-handled ranges.
    """
    def __init__(self, provider: str, symbol: str, key_column: str, exchange: str = None, api_key: str | None = None, max_emit: int = 50):
        self.provider = provider
        self.symbol = symbol.upper()
        self.key_column = key_column
        self.exchange = exchange
        self.api_key = api_key
        self._worker = None
        self._worker_lock = Lock()
        self._log_key = f"{self.provider}{':' + self.exchange if self.exchange else ''}:{self.symbol}"

        # The Deephaven mirrored table reference will be attached at start()
        self.dh_table = None

        # Backfill/coalescing configuration (can be tuned per-provider by overriding)
        self.max_requests_per_scan = 10         # Maximum number of merged requests to perform in a single scan iteration
        self.min_sleep_between_requests = 0.4   # Minimum seconds to wait between provider REST requests to avoid bursts
        self.max_ids_per_request = 50_000       # Maximum ID span (inclusive) to request from provider in a single call
        self.gap_merge_distance = 50            # Merge adjacent/nearby gaps when the distance between them is <= this value

    # Worker lifecycle helpers
    def is_running(self) -> bool:
        """Return True if a worker is set and appears to be alive.

        Note: the worker object is expected to expose an `is_alive()` method (DHThread).
        """
        w = self._worker
        return w is not None and getattr(w, 'is_alive', lambda: True)()

    def _set_worker(self, worker) -> None:
        """Atomically set the worker reference."""
        with self._worker_lock:
            self._worker = worker

    def _clear_worker(self) -> None:
        """Atomically clear the worker reference."""
        with self._worker_lock:
            self._worker = None

    # Start the gap filler worker
    def start(self, scan_interval: Optional[int] = None, api_key: str | None = None, dh_table: Optional[object] = None):
        """
        Start the gap filler in a managed DHThread using spawn(). Returns the DHThread.
        """
        if self.is_running():
            return self._worker

        # Attach the provided dh_table to the worker instance so run() can access it.
        self.dh_table = dh_table

        # Store API key on the instance if provided (do not pass it to the thread target)
        if api_key is not None:
            self.api_key = api_key

        # Spawn the managed DHThread (run will read self.dh_table)
        try:
            worker = spawn('feeder', self._log_key, 'gapfiller', self.run, scan_interval if scan_interval else 60)
            self._set_worker(worker)
            emit_event("feeder", self._log_key, "backfill", "INFO", "START", f"Starting gap filler for {self.symbol}",
                       {"scan_interval": scan_interval},)
            return worker
        except Exception as ex:
            emit_event("feeder", self._log_key, "backfill", "ERROR", "START_ERR", f"Failed to start gap filler: {ex}", {"error": str(ex)})
            return None

    def stop(self, timeout: int = 3):
        """
        Stop the running worker, if any, and join with timeout.
        """
        if not self.is_running():
            return
        try:
            # Stop and join the underlying worker, then clear the reference.
            self._worker.stop()
            self._worker.join(timeout=timeout)
        finally:
            # Clear worker and detach the table reference
            self._clear_worker()
            self.dh_table = None

    def run(self, stop_event, scan_interval: int = 60):
        """
        Executes the main loop for gap detection and backfilling in a managed thread.
        This method continuously scans for gaps in the attached Deephaven table at specified intervals.
        If gaps are detected, it emits detection events and invokes the provider-specific backfill logic.
        The loop can be stopped externally using the provided threading.Event.
        Args:
            stop_event (threading.Event): Event used to signal stopping the loop.
            scan_interval (int, optional): Number of seconds to wait between scans. Defaults to 60.
        Emits:
            - Warning event if no Deephaven table is attached.
            - Info event summarizing detected gaps.
            - Error event if an exception occurs during processing.
        Calls:
            - provider_gap_detection: Provider-specific method to detect gaps.
            - backfill_gaps: Provider-specific method to backfill detected gaps.
        """
        while not stop_event.is_set():
            try:
                # Table safety check
                if self.dh_table is None:
                    emit_event("feeder", self._log_key, "gap_detection", "WARN", "NO_TABLE", 
                            "No Deephaven table attached for server-side gap detection", {})
                    continue

                # Gap Detection: Call provider specific gap detection methods
                gaps = self.provider_gap_detection(self.dh_table)
                if not gaps:
                    continue

                # Coalesce gaps to optimize backfill requests
                merged_gaps = self._coalesce_gaps(gaps, max_ids=self.max_ids_per_request, merge_distance=self.gap_merge_distance)

                # Emit detection events and invoke provider backfill
                emit_event("feeder", self._log_key, "gap_detection", "INFO", "GAP_DETECTION",
                    f"Detected {len(merged_gaps)} backfill request(s) (from {len(gaps)} detected gaps)",
                    {
                        "provider": self.provider,
                        "symbol": self.symbol,
                        "total_gaps": len(gaps),
                        "merged_gaps": len(merged_gaps),
                        "sample_gaps": gaps[: min(5, len(gaps))] if len(gaps) else [],
                        "exchange": self.exchange,
                        }
                    )

                # Call Backfill Manager
                self.backfill_gaps(merged_gaps)

            except Exception as ex:
                # Generic worker error event for the provider
                emit_event("feeder", self._log_key, "backfill", "ERROR", "WORKER_ERR", f"{self.provider} run error: {ex}",
                           {"error": str(ex)},)
            stop_event.wait(scan_interval)


    def provider_gap_detection(self, dh_table, start: Optional[datetime] = None, end: Optional[datetime] = None) -> Optional[List[Tuple[int, int]]]:
        raise NotImplementedError("Provider-specific _detect_gaps_table() must be implemented in subclass.")

    def backfill_gaps(self, merged_gaps: List[Tuple[int, int]]):
        """
        Backfill missing data for each gap using provider-specific REST API logic.
        This is a stub to be implemented per-provider.
        """
        # Limit the number of requests per scan to avoid aggressive bursts.
        to_process = merged_gaps[: self.max_requests_per_scan]

        for idx, (start_id, end_id) in enumerate(to_process):
            try:
                # Provider receives a single merged (start,end) tuple wrapped in a list
                self.provider_backfill_gaps([(start_id, end_id)])
            except Exception as ex:
                emit_event("feeder", self._log_key, "backfill", "ERROR", "BACKFILL_REQ_ERR",
                           f"Error while backfilling {self.symbol} {start_id}..{end_id}: {ex}", {"error": str(ex)})
            # Throttle between requests
            if idx < len(to_process) - 1:
                time.sleep(self.min_sleep_between_requests)

        # If we had more merged requests than we processed, emit info so operator knows there's remaining work
        if len(merged_gaps) > len(to_process):
            emit_event("feeder", self._log_key, "backfill", "INFO", "BACKFILL_DEFERRED",
                       f"Deferred {len(merged_gaps) - len(to_process)} merged backfill request(s) to later scans",
                       {"deferred": len(merged_gaps) - len(to_process)})

    def provider_backfill_gaps(self, gaps: List[Tuple[int, int]]):
        raise NotImplementedError("Provider-specific backfill_gaps() must be implemented in subclass.")

    def _coalesce_gaps(self, gaps: List[Tuple[int, int]], max_ids: int, merge_distance: int) -> List[Tuple[int, int]]:
        """
        Merge and coalesce a list of integer ID gaps into a smaller set of request ranges.

        - gaps: list of (start_id, end_id)
        - max_ids: maximum span length for a single request
        - merge_distance: if two gaps are within `merge_distance` IDs, merge them

        Returns a list of non-overlapping, sorted (start, end) ranges.
        """
        if not gaps:
            return []

        # Normalize and sort
        normalized = [(int(s), int(e)) for s, e in gaps]
        normalized.sort()

        merged: List[Tuple[int, int]] = []
        cur_s, cur_e = normalized[0]

        for s, e in normalized[1:]:
            # If next gap is close enough (within merge_distance) OR merging keeps size under max_ids, merge
            potential_end = max(cur_e, e)
            if s <= cur_e + merge_distance or (potential_end - cur_s) <= max_ids:
                # Expand current range
                cur_e = potential_end
                # If merged range exceeds max_ids, split into bounded chunks
                if (cur_e - cur_s) > max_ids:
                    # split into multiple chunks of size max_ids
                    while (cur_e - cur_s) > max_ids:
                        chunk_end = cur_s + max_ids
                        merged.append((cur_s, chunk_end))
                        cur_s = chunk_end + 1
                    # cur_s..cur_e now <= max_ids
            else:
                merged.append((cur_s, cur_e))
                cur_s, cur_e = s, e

        merged.append((cur_s, cur_e))
        return merged

    # watermark feature removed; gap detection uses table state
