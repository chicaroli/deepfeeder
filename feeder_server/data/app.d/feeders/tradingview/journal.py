"""TradingViewJournal: managed, batched JSONL journaling for TV bars.

This module exposes a single class, ``TradingViewJournal``, which batches
bar records and writes JSONL files partitioned by exchange/symbol/date.
No module-level global state is created; callers must instantiate and
manage a journal instance themselves.
"""
from __future__ import annotations

import queue
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
from persistence.paths import TV_HOT_BARS_DIR
from .transform import records_to_dataframe, to_arrow_table



class TradingViewJournal:
    """A managed, non-blocking journaling helper for TradingView bars.

    Usage:
      j = TradingViewJournal(service, name)
      j.start()
      j.journal_bar_nonblocking(...)
      j.stop()

    All tunables and paths are instance-scoped to avoid module globals.
    """

    def __init__(
        self,
        service: str,
        name: str,
        role: str = "journal",
        queue_max: int = 10000,
        batch_size: int = 100,
        flush_interval: float = 2.0,
        base_dir: Optional[Path] = None,
    ):
        self._q: "queue.Queue[dict]" = queue.Queue(maxsize=queue_max)
        self._stop = False
        self._thread = None
        self._service = service
        self._name = name
        self._role = role
        self._batch_size = batch_size
        self._flush_interval = flush_interval

        if base_dir is None:
            # Use project-configured hot directory for TV bars persistence
            base_dir = Path(TV_HOT_BARS_DIR)
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        # Partition schema used by pyarrow.dataset to create hive-style folders
        try:
            self._partition_schema = pa.schema([
                ("dt", pa.string()),
                ("exchange", pa.string()),
                ("symbol", pa.string()),
            ])
        except Exception:
            # fallback minimal schema
            self._partition_schema = pa.schema([("dt", pa.string()), ("symbol", pa.string())])
        # optional tuning for dataset writer
        self.max_rows_per_file = None
        self.max_rows_per_group = None

    def start(self):
        if self._thread is not None:
            return
        self._thread = spawn(self._service, self._name, self._role, self._run)
        emit_event(self._service, self._name, self._role, "INFO", "JOURNAL_START", f"journal started: base={self._base_dir}")

    def stop(self, timeout: float = 2.0):
        try:
            if self._thread is not None:
                self._stop = True
                t = self._thread
                try:
                    t.stop()
                except Exception:
                    pass
                self._thread = None
        except Exception:
            pass
        emit_event(self._service, self._name, self._role, "INFO", "JOURNAL_STOP", "journal stopped")

    def journal_bar_nonblocking(self, exchange: Any, symbol: Any, ts: Any, open_p: Any, high: Any, low: Any, close: Any, 
                                volume: Any) -> None:
        rec = self._row_to_dict(exchange, symbol, ts, open_p, high, low, close, volume)
        try:
            self._q.put_nowait(rec)
        except queue.Full:
            emit_event(self._service, self._name, self._role, "WARNING", "JOURNAL_FULL", "journal queue full, dropping record")

    def _row_to_dict(self, exchange: Any, symbol: Any, ts: Any, open_p: Any, high: Any, low: Any, close: Any, volume: Any) -> dict:
        try:
            ts_str = str(ts)
        except Exception:
            try:
                ts_str = ts.isoformat()
            except Exception:
                ts_str = repr(ts)
        # Always uppercase symbol before writing
        symbol_upper = (symbol or "").upper() if symbol is not None else None
        return {
            "exchange": (exchange or "").upper() if exchange is not None else None,
            "symbol": symbol_upper,
            "timestamp": ts_str,
            "open": open_p,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }

    def _flush_batch(self, batch: list):
        # Convert raw records into a typed DataFrame using transformation helpers
        try:
            df = records_to_dataframe(batch)
        except Exception:
            # If normalization fails, emit an event and skip this batch to avoid blocking
            emit_event(self._service, self._name, self._role, "ERROR", "JOURNAL_NORMALIZE", "failed to normalize batch")
            return

        if df.empty:
            return

        # Deduplicate by exchange, symbol, timestamp (keep last row per key)
        dedup_cols = ["exchange", "symbol", "timestamp"]
        try:
            df = df.sort_values(dedup_cols + ["timestamp"]).drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)
        except Exception:
            # If dedup fails, emit event and continue with original df
            emit_event(self._service, self._name, self._role, "WARNING", "JOURNAL_DEDUP_FAIL", "deduplication failed; writing all rows")

        # Use pyarrow.dataset to write a hive-partitioned dataset in a single call
        try:
            tbl = to_arrow_table(df)
            epoch_ms = int(time.time() * 1000)
            self._file_seq = getattr(self, '_file_seq', 0) + 1
            basename = f"part-tv-{epoch_ms}-{self._file_seq}-{{i}}.parquet"
            ds.write_dataset(
                data=tbl,
                base_dir=str(self._base_dir),
                format="parquet",
                partitioning=ds.partitioning(self._partition_schema, flavor="hive"),
                basename_template=basename,
                existing_data_behavior="overwrite_or_ignore",
                create_dir=True,
                max_rows_per_file=self.max_rows_per_file or None,
                max_rows_per_group=self.max_rows_per_group or None,
            )
            emit_event(self._service, self._name, self._role, "INFO", "JOURNAL_FLUSH", {"rows": int(df.shape[0]), "files_seq": self._file_seq})
        except Exception:
            emit_event(self._service, self._name, self._role, "ERROR", "JOURNAL_FLUSH_ERR", "flush error")

    def _run(self, stop_event):
        batch = []
        last_flush = time.time()
        while not stop_event.is_set() and not self._stop:
            timeout = max(0.0, self._flush_interval - (time.time() - last_flush))
            try:
                item = self._q.get(timeout=timeout)
                if item is None:
                    break
                batch.append(item)
                while len(batch) < self._batch_size:
                    try:
                        item = self._q.get_nowait()
                        if item is None:
                            break
                        batch.append(item)
                    except queue.Empty:
                        break
            except queue.Empty:
                pass

            now = time.time()
            if batch and (len(batch) >= self._batch_size or (now - last_flush) >= self._flush_interval):
                try:
                    self._flush_batch(batch)
                except Exception:
                    pass
                batch = []
                last_flush = now

        if batch:
            try:
                self._flush_batch(batch)
            except Exception:
                pass
