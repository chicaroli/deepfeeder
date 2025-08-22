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
import time

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from persistence.paths import ensure_dirs
from persistence.config import load_config, JournalConfig
from persistence.adapters import discover_adapters

from runtime.dh_thread import spawn
from runtime.eventlog import emit_event

# Provider adapters are discovered dynamically


class _Sink:
    def __init__(self, name: str, base_dir: str, to_table):
        self.name = name
        self.base_dir = base_dir
        self._to_table = to_table
        self.q: Deque[Dict[str, Any]] = deque(maxlen=20000)
        self._stop = threading.Event()
        self._worker = None
        # Load config once per sink
        self._cfg = load_config()
        self.flush_rows = self._cfg.flush_rows
        self.flush_secs = self._cfg.flush_secs
        self.max_rows_per_file = self._cfg.max_rows_per_file
        self.max_rows_per_group = self._cfg.max_rows_per_group
        # Unique sequence per sink to avoid filename collisions across flushes
        self._file_seq = 0
        # Compaction settings
        self._compact_enabled = self._cfg.compact_enabled
        self._compact_interval = self._cfg.compact_interval_secs
        self._compact_stable_secs = self._cfg.compact_stable_secs
        self._compact_min_files = self._cfg.compact_min_files
        self._master_prefix = self._cfg.master_prefix
        self._last_compact_ts = 0.0

    def start(self):
        if self._worker is not None:
            return
        self._worker = spawn("journal", self.name, "sink", self._run)
        try:
            emit_event("journal", self.name, "sink", "INFO", "SINK_START", f"sink started: base={self.base_dir}")
        except Exception:
            pass

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
        try:
            epoch_ms = int(time.time() * 1000)
            self._file_seq += 1
            basename = f"part-{self.name}-{epoch_ms}-{self._file_seq}-{{i}}.parquet"
            ds.write_dataset(
                data=tbl,
                base_dir=self.base_dir,
                format="parquet",
                partitioning=ds.partitioning(
                    pa.schema([("dt", pa.string()), ("symbol", pa.string())]),
                    flavor="hive"
                    ),
                basename_template=basename,
                existing_data_behavior="overwrite_or_ignore",
                create_dir=True,
                max_rows_per_file=self.max_rows_per_file or None,
                max_rows_per_group=self.max_rows_per_group or None,
            )
            if getattr(self._cfg, "log_flush_enabled", False):
                try:
                    emit_event("journal", self.name, "sink", "INFO", "FLUSH", f"wrote batches=1")
                except Exception:
                    pass
        except Exception as exc:
            try:
                emit_event("journal", self.name, "sink", "ERROR", "PARQUET_WRITE", f"flush error: {exc!r}")
            except Exception:
                pass

    def _run(self, stop_event: Any):
        buf: List[Dict[str, Any]] = []
        last = datetime.now(timezone.utc)
        while not self._stop.is_set() and not getattr(stop_event, 'is_set', lambda: False)():
            drain_count = self.flush_rows - len(buf)
            while drain_count > 0 and self.q:
                try:
                    rec = self.q.popleft()
                    if rec:
                        buf.append(rec)
                except Exception:
                    break
                drain_count -= 1
            now = datetime.now(timezone.utc)
            if len(buf) >= self.flush_rows or (buf and (now - last).total_seconds() >= self.flush_secs):
                self._flush(buf)
                buf.clear()
                last = now
            time.sleep(0.05)
            if self._compact_enabled:
                try:
                    now_s = time.time()
                    if (now_s - self._last_compact_ts) >= self._compact_interval:
                        self._maybe_compact_today()
                        self._last_compact_ts = now_s
                except Exception:
                    pass
        if buf:
            self._flush(buf)

    def _today_dir(self) -> Optional[str]:
        try:
            today = date.today().isoformat()
            p1 = os.path.join(self.base_dir, f"dt={today}")
            if os.path.isdir(p1):
                return p1
        except Exception:
            pass
        return None

    def _maybe_compact_today(self) -> None:
        ddir = self._today_dir()
        if not ddir:
            return
        try:
            for entry in os.listdir(ddir):
                # only consider modern symbol partitions: symbol=<name>
                if not entry.startswith("symbol="):
                    continue
                spath = os.path.join(ddir, entry)
                if not os.path.isdir(spath):
                    continue
                sym = entry.split("=", 1)[1].lower()
                self._compact_symbol_dir(ddir, sym, spath)
        except Exception:
            pass

    def _compact_symbol_dir(self, ddir: str, symbol: str, spath: str) -> None:
        try:
            files = [f for f in os.listdir(spath) if f.endswith('.parquet')]
            if not files:
                return
            day_token = os.path.basename(ddir).split('=')[-1]
            master_name = f"{self._master_prefix}{day_token}-{symbol}.parquet"
            master_path = os.path.join(spath, master_name)
            now_s = time.time()
            parts = []
            for f in files:
                if f == os.path.basename(master_path):
                    continue
                fp = os.path.join(spath, f)
                try:
                    st = os.stat(fp)
                    if (now_s - st.st_mtime) >= self._compact_stable_secs:
                        parts.append(fp)
                except Exception:
                    continue
            if len(parts) < 1:
                return
            if len(files) < self._compact_min_files and not os.path.exists(master_path):
                return
            inputs = []
            if os.path.exists(master_path):
                inputs.append(master_path)
            inputs.extend(parts)
            if not inputs:
                return
            tables = []
            for fp in inputs:
                try:
                    tables.append(pq.read_table(fp))
                except Exception:
                    pass
            if not tables:
                return
            try:
                merged = pa.concat_tables(tables, promote_options="default")  # type: ignore[arg-type]
            except TypeError:
                merged = pa.concat_tables(tables, promote=True)
            tmp_path = os.path.join(spath, f".__tmp__{os.getpid()}_{int(now_s)}.parquet")
            pq.write_table(merged, tmp_path)
            try:
                if os.path.exists(master_path):
                    os.remove(master_path)
            except Exception:
                pass
            os.replace(tmp_path, master_path)
            for fp in parts:
                try:
                    os.remove(fp)
                except Exception:
                    pass
            try:
                emit_event("journal", self.name, "compact", "INFO", "COMPACT_DONE", f"{symbol} -> {master_name}", {"files": len(inputs), "rows": int(merged.num_rows)})
            except Exception:
                pass
        except Exception as exc:
            try:
                emit_event("journal", self.name, "compact", "ERROR", "COMPACT_ERR", str(exc))
            except Exception:
                pass


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
        # Counter for records enqueued per provider
        self._enq_counters = {a.name: 0 for a in self._adapters}
        for a in self._adapters:
            sink = self._sinks[a.name]
            def _tap_factory(adapter, sink_, provider_name):
                def _tap(*row_args):
                    try:
                        rec = adapter.to_record(row_args)
                        if rec:
                            before_len = len(sink_.q)
                            sink_.enqueue(rec)
                            after_len = len(sink_.q)
                            self._enq_counters[provider_name] += 1
                            # Print every 100 records
                            if self._enq_counters[provider_name] % 100 == 0:
                                print(f"[journal] tap: provider={provider_name} enqueued={self._enq_counters[provider_name]} qlen={after_len}")
                            # Always print warning if dropped
                            if after_len == sink_.q.maxlen and before_len == after_len:
                                print(f"[journal][WARNING] queue full for provider={provider_name}, record dropped!")
                    except Exception as exc:
                        print(f"[journal][ERROR] tap exception: {exc}")
                return _tap
            try:
                a.register_tap(_tap_factory(a, sink, a.name))
            except Exception:
                pass
            sink.start()
        self._started = True
        print(f"[journal] started. Enqueue counters: {self._enq_counters}")
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
    def _row_ts(self, r: Dict[str, Any]) -> Optional[datetime]:
        v = r.get("ts") or r.get("Timestamp") or r.get("LpTime")
        if isinstance(v, datetime):
            return v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
        try:
            s = str(v)
            if not s:
                return None
            # Truncate fractional seconds to 6 digits for Python compatibility
            if '.' in s:
                pre, post = s.split('.', 1)
                if '+' in post or 'Z' in post:
                    if '+' in post:
                        frac, rest = post.split('+', 1)
                        frac = frac[:6]
                        s = f"{pre}.{frac}+{rest}"
                    else:
                        frac, rest = post.split('Z', 1)
                        frac = frac[:6]
                        s = f"{pre}.{frac}Z"
                else:
                    s = f"{pre}.{post[:6]}"
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return None
        
    def _iter_parquet(self, base_dir: str, symbol: str, t0: datetime, t1: datetime):
        import os
        # iterate modern partitioned layout only
        cur = t0.date()
        while cur <= t1.date():
            part = os.path.join(base_dir, f"dt={cur.isoformat()}", f"symbol={symbol.lower()}")
            if os.path.isdir(part):
                try:
                    dataset = ds.dataset(part, format="parquet")
                    schema = dataset.schema
                    have_ts = any(f.name == "ts" for f in schema)
                    fragments = dataset.get_fragments() if not have_ts else dataset.get_fragments(
                        (ds.field("ts") >= pa.scalar(t0, type=pa.timestamp("us", tz="UTC"))) & (ds.field("ts") < pa.scalar(t1, type=pa.timestamp("us", tz="UTC")))
                    )
                    for frag in fragments:
                        scanner = ds.Scanner.from_fragment(frag)
                        tbl = scanner.to_table()
                        for r in tbl.to_pylist():
                            ts = self._row_ts(r)
                            if ts is None or ts < t0 or ts >= t1:
                                continue
                            yield r
                except Exception:
                    pass
            cur = date.fromordinal(cur.toordinal() + 1)


    def replay(self, provider: str, symbol: str, t0_iso: str, t1_iso: str) -> str:
        """Delegate replay to provider's journal_adapter."""
        adapter = next((a for a in self._adapters if a.name == provider), None)
        if adapter is None:
            return f"[replay] {provider} adapter not available"
        if not hasattr(adapter, "replay"):
            return f"[replay] {provider} adapter missing replay()"
        return adapter.replay(symbol, t0_iso, t1_iso, self._iter_parquet)
