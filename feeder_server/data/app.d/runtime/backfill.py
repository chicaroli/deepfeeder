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
    def __init__(self, provider: str, symbols: str | list[str], key_column: str, exchange: None | str | list[str] = None, api_key: str | None = None, max_emit: int = 50, feeder_name: str = None):
        self.provider = provider
        # Normalize symbols to a list of uppercase strings
        if isinstance(symbols, str):
            self.symbols = [symbols.upper()]
        else:
            self.symbols = [s.upper() for s in symbols]
        self.key_column = key_column
        # Handle exchange as None, str, or list[str]
        if exchange is None:
            self.exchanges = [None] * len(self.symbols)
        elif isinstance(exchange, str):
            self.exchanges = [exchange] * len(self.symbols)
        elif isinstance(exchange, list):
            if len(exchange) != len(self.symbols):
                raise ValueError("Length of exchange list must match symbols list")
            self.exchanges = exchange
        else:
            raise TypeError("exchange must be None, str, or list[str]")
        # For backward compatibility, set self.exchange to first exchange
        self.exchange = self.exchanges[0] if self.exchanges else None
        self.api_key = api_key
        self.feeder_name = feeder_name if feeder_name is not None else ','.join(self.symbols)
        self._worker = None
        self._worker_lock = Lock()
        # For logging, use feeder_name
        self._log_key = f"{self.provider}{':' + str(self.exchange) if self.exchange else ''}:{self.feeder_name}"

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
            emit_event("feeder", self._log_key, "backfill", "INFO", "START", f"Starting gap filler for {self.feeder_name}",
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
                gaps_dict = self.provider_gap_detection(self.dh_table)
                if not gaps_dict:
                    continue

                # Coalesce gaps per symbol
                merged_gaps_dict = self._coalesce_gaps(gaps_dict, max_ids=self.max_ids_per_request, merge_distance=self.gap_merge_distance)

                # Emit detection events and invoke provider backfill per symbol
                for symbol in self.symbols:
                    gaps = gaps_dict.get(symbol, [])
                    merged_gaps = merged_gaps_dict.get(symbol, [])
                    meta = {
                        "provider": self.provider,
                        "feeder_name": self.feeder_name,
                        "symbol": symbol,
                        "total_gaps": len(gaps),
                        "merged_gaps": len(merged_gaps),
                        "exchange": self.exchange,
                    }
                    # If sample_gaps is present, serialize datetimes
                    if gaps:
                        meta["sample_gaps"] = [
                            (g[0].isoformat() if hasattr(g[0], "isoformat") else str(g[0]),
                                g[1].isoformat() if hasattr(g[1], "isoformat") else str(g[1]))
                            for g in gaps
                        ]
                        emit_event("feeder", self._log_key, "gap_detection", "INFO", "GAP_DETECTION",
                                   f"Detected {len(merged_gaps)} backfill request(s) (from {len(gaps)} detected gaps) for {symbol}",
                                   meta)
                self.backfill_gaps(merged_gaps_dict)

            except Exception as ex:
                # Generic worker error event for the provider
                emit_event("feeder", self._log_key, "backfill", "ERROR", "WORKER_ERR", f"{self.provider} run error: {ex}",
                           {"error": str(ex)},)
            stop_event.wait(scan_interval)


    def provider_gap_detection(self, dh_table, start: Optional[datetime] = None, end: Optional[datetime] = None) -> Optional[List[Tuple[int, int]]]:
        raise NotImplementedError("Provider-specific _detect_gaps_table() must be implemented in subclass.")

    def backfill_gaps(self, merged_gaps_dict):
        """
        Backfill missing data for each gap using provider-specific REST API logic.
        Handles gaps as a dict mapping symbol to list of gaps.
        """
        for symbol in self.symbols:
            merged_gaps = merged_gaps_dict.get(symbol, [])
            to_process = merged_gaps[: self.max_requests_per_scan]
            for idx, (start_id, end_id) in enumerate(to_process):
                try:
                    self.provider_backfill_gaps({symbol: [(start_id, end_id)]})
                except Exception as ex:
                    emit_event("feeder", self._log_key, "backfill", "ERROR", "BACKFILL_REQ_ERR",
                               f"Error while backfilling {self.feeder_name} {symbol} {start_id}..{end_id}: {ex}", {"error": str(ex)})
                if idx < len(to_process) - 1:
                    time.sleep(self.min_sleep_between_requests)
            if len(merged_gaps) > len(to_process):
                emit_event("feeder", self._log_key, "backfill", "INFO", "BACKFILL_DEFERRED",
                           f"Deferred {len(merged_gaps) - len(to_process)} merged backfill request(s) for {symbol} to later scans",
                           {"deferred": len(merged_gaps) - len(to_process), "symbol": symbol})

    def provider_backfill_gaps(self, gaps: List[Tuple[int, int]]):
        raise NotImplementedError("Provider-specific backfill_gaps() must be implemented in subclass.")

    def _coalesce_gaps(self, gaps_dict, max_ids: int, merge_distance: int):
        """
        Merge and coalesce gaps per symbol into a smaller set of request ranges.
        Accepts a dict mapping symbol to list of gaps.
        Returns a dict mapping symbol to list of merged gaps.
        """
        merged_dict = {}
        for symbol, gaps in gaps_dict.items():
            if not gaps:
                merged_dict[symbol] = []
                continue
            normalized = [(int(s), int(e)) for s, e in gaps]
            normalized.sort()
            merged: List[Tuple[int, int]] = []
            cur_s, cur_e = normalized[0]
            for s, e in normalized[1:]:
                potential_end = max(cur_e, e)
                if s <= cur_e + merge_distance or (potential_end - cur_s) <= max_ids:
                    cur_e = potential_end
                    if (cur_e - cur_s) > max_ids:
                        while (cur_e - cur_s) > max_ids:
                            chunk_end = cur_s + max_ids
                            merged.append((cur_s, chunk_end))
                            cur_s = chunk_end + 1
                else:
                    merged.append((cur_s, cur_e))
                    cur_s, cur_e = s, e
            merged.append((cur_s, cur_e))
            merged_dict[symbol] = merged
        return merged_dict

    # watermark feature removed; gap detection uses table state
