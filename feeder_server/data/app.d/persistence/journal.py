"""Hot journal to Parquet and replay utilities.

Design:
- Taps are registered into provider writers to capture each row into an
  in-memory queue with minimal overhead.
- Background threads flush rows to Parquet in partitioned datasets.
- Replay reads Parquet back and writes to the live writers using
  write_row_direct to avoid re-tapping.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone, date
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import deque
import os

import pyarrow as pa
import pyarrow.dataset as ds

from persistence.paths import ensure_dirs
from persistence.adapters import discover_adapters

from runtime.dh_thread import spawn
from runtime.eventlog import emit_event

# Provider adapters are discovered dynamically


def _to_utc(dt_like: Any) -> datetime:
    """Best-effort conversion of Deephaven Instant or datetime to UTC datetime."""
    try:
        # deephaven Instant supports isoformat; many ops pass python datetime
        if isinstance(dt_like, datetime):
            if dt_like.tzinfo is None:
                return dt_like.replace(tzinfo=timezone.utc)
            return dt_like.astimezone(timezone.utc)
        # Fallback: leave as-is
        s = str(dt_like)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.now(timezone.utc)


def _ymd(d: datetime) -> str:
    return d.date().isoformat()


class _Sink:
    def __init__(self, name: str, base_dir: str, to_table):
        self.name = name
        self.base_dir = base_dir
        self._to_table = to_table
        self.q: Deque[Dict[str, Any]] = deque(maxlen=20000)
        self._stop = threading.Event()
        self._worker = None
        self.flush_rows = int(os.getenv("DEEPFEEDER_JOURNAL_FLUSH_ROWS", "5000"))
        self.flush_secs = float(os.getenv("DEEPFEEDER_JOURNAL_FLUSH_SECS", "2.0"))

    def start(self):
        if self._worker is not None:
            return
        self._worker = spawn("journal", self.name, "sink", self._run)

    def stop(self):
        self._stop.set()
        if self._worker is not None:
            try:
                self._worker.join(timeout=3)
            except Exception:
                pass
            self._worker = None

    def enqueue(self, rec: Dict[str, Any]) -> None:
        try:
            self.q.append(rec)
        except Exception:
            pass

    def _flush(self, batch: List[Dict[str, Any]]):
        if not batch:
            return
        tbl = self._to_table(batch)
        # Partition by dt/symbol
        try:
            ds.write_dataset(
                tbl,
                base_dir=self.base_dir,
                format="parquet",
                partitioning=["dt", "symbol"],
                existing_data_behavior="overwrite_or_ignore",  # write new files
            )
        except Exception as exc:
            try:
                emit_event("journal", self.name, "sink", "ERROR", "PARQUET_WRITE", f"flush error: {exc!r}")
            except Exception:
                pass

    def _run(self, stop_event: Any):
        buf: List[Dict[str, Any]] = []
        last = datetime.now(timezone.utc)
        import time
        while not self._stop.is_set() and not getattr(stop_event, 'is_set', lambda: False)():
            rec = None
            try:
                rec = self.q.popleft()
            except Exception:
                pass
            if rec:
                buf.append(rec)
            now = datetime.now(timezone.utc)
            if len(buf) >= self.flush_rows or (buf and (now - last).total_seconds() >= self.flush_secs):
                self._flush(buf)
                buf.clear()
                last = now
            time.sleep(0.05)
        if buf:
            self._flush(buf)


class JournalService:
    """Manage taps and sinks using provider adapters and provide replay API."""

    def __init__(self) -> None:
        ensure_dirs()
        self._adapters = discover_adapters()
        # Build a sink per adapter
        self._sinks = {a.name: _Sink(a.name, a.base_dir, a.to_arrow_table) for a in self._adapters}
        self._started = False

    def start(self) -> str:
        if self._started:
            return "[journal] already started"
        # Register adapter taps and start sinks
        for a in self._adapters:
            sink = self._sinks[a.name]
            def _tap_factory(adapter, sink_):
                def _tap(*row_args):
                    try:
                        rec = adapter.to_record(row_args)
                        if rec:
                            sink_.enqueue(rec)
                    except Exception:
                        pass
                return _tap
            try:
                a.register_tap(_tap_factory(a, sink))
            except Exception:
                pass
            sink.start()
        self._started = True
        return "[journal] started"

    def stop(self) -> str:
        if not self._started:
            return "[journal] not running"
        for s in self._sinks.values():
            s.stop()
        self._started = False
        return "[journal] stopped"

    def purge_hot_partitions(self, keep_days: int = 14) -> int:
        """Delete dt=YYYY-MM-DD partitions older than keep_days for both streams.

        Returns number of dt partitions removed.
        """
        import os
        from datetime import date, timedelta
        cutoff = date.today() - timedelta(days=int(keep_days))
        removed = 0
        for base in [s.base_dir for s in self._sinks.values()]:
            try:
                if not os.path.isdir(base):
                    continue
                for name in os.listdir(base):
                    if not name.startswith("dt="):
                        continue
                    try:
                        day = date.fromisoformat(name[3:])
                    except Exception:
                        continue
                    if day < cutoff:
                        path = os.path.join(base, name)
                        try:
                            # remove whole dt partition directory tree
                            import shutil
                            shutil.rmtree(path, ignore_errors=True)
                            removed += 1
                        except Exception:
                            pass
            except Exception:
                pass
        return removed

    # --- replay helpers ---
    def _iter_parquet(self, base_dir: str, symbol: str, t0: datetime, t1: datetime):
        import os
        # iterate date partitions
        cur = t0.date()
        while cur <= t1.date():
            part = os.path.join(base_dir, f"dt={cur.isoformat()}", f"symbol={symbol.lower()}")
            if os.path.isdir(part):
                try:
                    dataset = ds.dataset(part, format="parquet")
                    # pushdown filter on ts
                    filt = (ds.field("ts") >= pa.scalar(t0, type=pa.timestamp("us", tz="UTC"))) & (ds.field("ts") < pa.scalar(t1, type=pa.timestamp("us", tz="UTC")))
                    for frag in dataset.get_fragments(filt):
                        scanner = ds.Scanner.from_fragment(frag)
                        tbl = scanner.to_table()
                        for r in tbl.to_pylist():
                            yield r
                except Exception:
                    pass
            cur = date.fromordinal(cur.toordinal() + 1)

    def replay_binance(self, symbol: str, t0_iso: str, t1_iso: str) -> str:
        from datetime import datetime, timezone
        t0 = datetime.fromisoformat(t0_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        t1 = datetime.fromisoformat(t1_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        # find adapter by name
        adapter = next((a for a in self._adapters if a.name == 'binance'), None)
        if adapter is None:
            return "[replay] binance adapter not available"
        writer = adapter.get_writer()
        # Build seen set from live table
        seen: set[Any] = set()
        try:
            seen = adapter.build_seen(symbol, t0, t1)
        except Exception:
            pass
        n = 0
        for r in self._iter_parquet(adapter.base_dir, symbol, t0, t1):
            # Skip legacy JSON-row files (typed rows have these keys)
            if not isinstance(r, dict) or "Symbol" not in r or "TradeID" not in r:
                continue
            try:
                key = adapter.make_key(r)
            except Exception:
                key = None
            if key is not None and key in seen:
                continue
            try:
                # write without triggering taps
                args = adapter.to_writer_args(r)
                getattr(writer, "write_row_direct", writer.write_row)(*args)
                if key is not None:
                    seen.add(key)
                n += 1
            except Exception:
                pass
        return f"[replay] binance {symbol} +{n}"

    def replay_tv(self, symbol: str, t0_iso: str, t1_iso: str) -> str:
        from datetime import datetime, timezone
        t0 = datetime.fromisoformat(t0_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        t1 = datetime.fromisoformat(t1_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        adapter = next((a for a in self._adapters if a.name == 'tv'), None)
        if adapter is None:
            return "[replay] tv adapter not available"
        writer = adapter.get_writer()
        seen: set[Any] = set()
        try:
            seen = adapter.build_seen(symbol, t0, t1)
        except Exception:
            pass
        n = 0
        for r in self._iter_parquet(adapter.base_dir, symbol, t0, t1):
            # Skip legacy JSON-row files (typed rows have these keys)
            if not isinstance(r, dict) or "Symbol" not in r or "LpTime" not in r:
                continue
            try:
                key = adapter.make_key(r)
            except Exception:
                key = None
            if key is not None and key in seen:
                continue
            try:
                args = adapter.to_writer_args(r)
                getattr(writer, "write_row_direct", writer.write_row)(*args)
                if key is not None:
                    seen.add(key)
                n += 1
            except Exception:
                pass
        return f"[replay] tv {symbol} +{n}"
