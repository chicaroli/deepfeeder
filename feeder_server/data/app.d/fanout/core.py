# fanout/core.py
"""
core.py
MarketFeeder orchestration for subscriptions, snapshots, and replay.
"""
from typing import Dict, Set, Callable, Optional, Iterable, Tuple, Iterator
import time
import os
import pyarrow as pa
import pandas as pd

from datetime import datetime, timezone
from deephaven import filters as dff
from deephaven.arrow import to_arrow

from runtime.heartbeat import Heartbeater
from runtime.eventlog import emit_event
from .listener import _SymListener
from .schemas import SCHEMAS
from .utils import _symbol_filter_expr, _filter_fields


class MarketFeeder:
    """Orchestrates listeners, subscriptions, snapshots, and replay for market data."""
    def __init__(self):
        self._lsn: Dict[str, _SymListener] = {}
        self._refs: Dict[str, int] = {}
        self._subs: Dict[str, Set[Callable[[dict], None]]] = {}
        # handle -> (key, callback, fields, only_completed)
        self._handles: Dict[str, Tuple[str, Callable[[dict], None], Optional[Iterable[str]], bool]] = {}
        # Heartbeat for fanout core supervisor
        self._hb = Heartbeater("fanout", "market_feeder", "core")
        self._hb.beat("starting", meta={"listeners": 0, "subs": 0})
        self._last_core_meta_ts = 0.0
        self._core_meta_interval = float(os.getenv("DEEPFEEDER_FANOUT_CORE_HEARTBEAT_MIN_INTERVAL", "5"))

    @staticmethod
    def _key(provider: str, schema: str, symbol: str) -> str:
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        return f"{provider}|{schema}|{symbol}"

    def _ensure_listener(self, provider: str, data_schema: str, symbol: str):
        key = self._key(provider, data_schema, symbol)
        if key in self._lsn:
            return
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")
        base = spec.table_fn()
        sym_expr = _symbol_filter_expr(spec, symbol)
        # IMPORTANT: For bar (binned) schemas we must retain full history so that new bars arrive as ADDS;
        # using last_by() here collapsed history to a single rolling row, causing only MODIFIED events and
        # preventing completion detection.
        if spec.bin_period_minutes:
            view = base.where(sym_expr)
        else:
            # Non-bar (e.g., trades, quotes) can optionally keep all rows; leaving logic as-is (no last_by)
            # If a future optimization is needed, last_by could be reintroduced behind a flag.
            view = base.where(sym_expr)
        view = view.view([*spec.cols])
        def emit_completed(msg: dict):
            for h, (k, cb, fields, only_completed) in list(self._handles.items()):
                if k != key:
                    continue
                try:
                    if only_completed:
                        # Emit only completed data; keep structure with empty added/updated
                        completed = msg.get("completed")
                        if completed is None or (hasattr(completed, "num_rows") and completed.num_rows == 0):
                            continue  # nothing to deliver this tick
                        slim = {"added": None, "updated": None, "completed": completed, "meta": msg.get("meta", {})}
                        cb(_filter_fields(slim, fields))
                    else:
                        # Emit all tables (added, updated, completed)
                        cb(_filter_fields(msg, fields))
                except Exception:
                    pass
        # Allow buffer length override per-process
        try:
            buf_len = int(os.getenv("DEEPFEEDER_FANOUT_LISTENER_BUFFER_LEN", "256"))
        except Exception:
            buf_len = 256
        # Event: creating listener
        try:
            emit_event("fanout", f"{provider}:{data_schema}:{symbol}", "core", "INFO", "LISTENER_CREATE", "Creating listener", {"provider": provider, "schema": data_schema, "symbol": symbol, "buf_len": buf_len})
        except Exception:
            pass
        lsn = _SymListener(provider, data_schema, symbol, spec, view, emit_completed, buf_maxlen=buf_len)
        lsn.start()
        # Event: listener started
        try:
            emit_event("fanout", f"{provider}:{data_schema}:{symbol}", "core", "INFO", "LISTENER_START", "Listener started")
        except Exception:
            pass
        # require explicit start, reintroduce with a defensive try/except similar to _SymListener.start().
        self._lsn[key] = lsn
        self._subs.setdefault(key, set())
        self._maybe_core_beat()

    def _destroy_listener_if_unused(self, key: str):
        if self._refs.get(key, 0) > 0:
            return
        l = self._lsn.pop(key, None)
        if l:
            l.stop()
            try:
                emit_event("fanout", f"{l.provider}:{l.data_schema}:{l.symbol}", "core", "INFO", "LISTENER_STOP", "Listener stopped")
            except Exception:
                pass
        self._subs.pop(key, None)

    def subscribe(self, provider: str, data_schema: str, symbol: str,
                  callback: Callable[[dict], None],
                  fields: Optional[Iterable[str]] = None,
                  only_completed: bool = False) -> str:
        """Subscribe to a symbol stream.

        Params:
          - fields: Optional iterable of column names to project from Arrow tables.
          - only_completed: If True, deliver only the 'completed' table batches (no 'added' or 'updated').
                            If False, deliver all of added/updated/completed as available.
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol)
        self._refs[key] = self._refs.get(key, 0) + 1
        self._ensure_listener(provider, data_schema, symbol)
        handle = f"{key}:{id(callback)}"
        self._handles[handle] = (key, callback, set(fields) if fields else None, only_completed)
        self._subs.setdefault(key, set()).add(callback)
        # Event: subscription added
        try:
            emit_event("fanout", f"{provider}:{data_schema}:{symbol}", "core", "INFO", "SUBSCRIBE", "Subscriber added", {
                "handle": handle,
                "only_completed": bool(only_completed),
                "fields": list(fields) if fields else [],
            })
        except Exception:
            pass
        return handle

    def unsubscribe(self, handle: str) -> None:
        info = self._handles.pop(handle, None)
        if not info:
            return
        key, callback, *_ = info
        subs = self._subs.get(key)
        if subs and callback in subs:
            subs.remove(callback)
        self._refs[key] = max(0, self._refs.get(key, 0) - 1)
        self._destroy_listener_if_unused(key)
        self._maybe_core_beat()
        # Event: subscription removed
        try:
            emit_event("fanout", key, "core", "INFO", "UNSUBSCRIBE", "Subscriber removed", {"handle": handle})
        except Exception:
            pass

    def unsubscribe_all(self) -> int:
        """Unsubscribe all active handles and stop all listeners.

        Returns the number of handles that were removed.
        Safe to call multiple times.
        """
        # Remove all handles via the normal path to keep ref-counts consistent
        handles = list(self._handles.keys())
        for h in handles:
            try:
                self.unsubscribe(h)
            except Exception:
                # Continue best-effort
                pass
        # As a safety net, stop any remaining listeners and clear state
        for key, l in list(self._lsn.items()):
            try:
                l.stop()
            except Exception:
                pass
        self._lsn.clear()
        self._refs.clear()
        self._subs.clear()
        self._maybe_core_beat()
        # Event: all subscriptions removed
        try:
            emit_event("fanout", "market_feeder", "core", "INFO", "UNSUBSCRIBE_ALL", "All subscribers removed", {"removed": len(handles)})
        except Exception:
            pass
        return len(handles)

    def snapshot_range(
        self,
        provider: str,
        data_schema: str,
        symbol: str,
        *,
        exchange: Optional[str] = None,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
        fields: Optional[Iterable[str]] = None,
        include_open_bar: bool = True,  # reserved for future use
    ) -> pa.Table:
        """
        Return a non-ticking snapshot as a single Arrow table.
        - Bars: all rows in [start_ns, end_ns] (optionally include current open bar).
        - Trades/quotes: all rows in [start_ns, end_ns].
        Uses Deephaven filter objects (no query-language helpers required).

        Parameters:
          :param provider: Data provider name (e.g. 'binance', 'tradingview').
          :param data_schema: Schema key (e.g. 'trades', 'ohlcv_1m').
          :param symbol: Instrument symbol (case-sensitive per provider rules).
          :param exchange: Optional exchange code. Applied only if the underlying schema exposes an 'Exchange' column.
          :param start_ns: Inclusive nanosecond epoch bounds. If omitted, unbounded on that side.
          :param end_ns: Inclusive nanosecond epoch bounds. If omitted, unbounded on that side.
          :param fields: Optional iterable of column names (case-insensitive) to project.
          :param include_open_bar: Reserved (no-op currently).
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per snapshot is allowed. 'symbol' must be a string.")
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")

        base = spec.table_fn()

        # Build filter objects
        flts = [dff.Filter.from_(f"{spec.symbol_col} == `{symbol}`")]
        if exchange and "Exchange" in spec.cols:
            flts.append(dff.Filter.from_(f"Exchange == `{exchange.upper()}`"))
        if start_ns is not None:
            start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
            flts.append(dff.Filter.from_(f"{spec.time_col} >= {start_dt.isoformat()}"))
        if end_ns is not None:
            end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=timezone.utc)
            flts.append(dff.Filter.from_(f"{spec.time_col} <= {end_dt.isoformat()}"))

        # Query DH object
        t = base.where(*flts).view(list(spec.cols)).sort([spec.time_col])
        arr = to_arrow(t)

        # Column filtering (case-insensitive)
        if fields:
            keep = [n for n in arr.schema.names if n in fields or n in ["Symbol", "Timestamp"]]
            arr = arr.select(keep) if keep else pa.table({})
        return arr

    def snapshot_rows(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            *,
            start_ns: Optional[int] = None,
            end_ns: Optional[int] = None,
            fields: Optional[Iterable[str]] = None,
    ) -> Iterator[dict]:
        """Convenience: iterate dict rows from snapshot_range()."""
        arr = self.snapshot_range(provider, data_schema, symbol, start_ns=start_ns, end_ns=end_ns, fields=fields)
        yield from arr.to_pylist()

    def replay_since_ns(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            since_ns: Optional[int],
            emit: Callable[[str, dict], None],
            fields: Optional[Iterable[str]] = None,
    ) -> int:
        """
        Row-wise replay from the listener ring buffer for batches newer than since_ns (epoch ns).
        Calls: emit(part, row_dict) for each row, where part ∈ {'added','updated','completed'}.
        Returns total rows emitted.
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per replay is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol)
        lsn = self._lsn.get(key)
        if not lsn:
            return 0
        cols_req = {f.lower() for f in fields} if fields else None

        def _iter_rows(tbl: Optional[pa.Table]) -> Iterator[dict]:
            if tbl is None:
                return iter(())
            sel = tbl
            if cols_req:
                try:
                    names = list(tbl.schema.names)
                    keep = [n for n in names if n.lower() in cols_req]
                    sel = tbl.select(keep) if keep else None
                except Exception:
                    sel = tbl
            return iter(()) if sel is None else iter(sel.to_pylist())

        total = 0
        # Work on a static copy of the ring to avoid concurrent mutation surprises
        for msg in list(lsn.buf):
            meta = msg.get("meta") or {}
            ts_ns = meta.get("ts_ns")
            if ts_ns is None:
                ts_iso = meta.get("timestamp")
                if not ts_iso:
                    continue
                ts_ns = int(pd.Timestamp(ts_iso, tz="UTC").value)
            if since_ns is None or ts_ns > since_ns:
                for part in ("added", "updated", "completed"):
                    for row in _iter_rows(msg.get(part)):
                        emit(part, row)
                        total += 1
        return total

    def attach_gapless(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            *,
            emit_snapshot_row: Callable[[dict], None],
            emit_replay_row: Callable[[str, dict], None],
            fields: Optional[Iterable[str]] = None,
            only_completed: bool = True,
            start_ns: Optional[int] = None,
    ) -> Tuple[str, Optional[int]]:
        """
        Gapless attach helper:
          1) Ensure/subscribe to live (returns handle).
          2) Emit a 'snapshot' (rows) using snapshot_range() from start_ns (defaults to UTC day start).
          3) Compute watermark_ns as last row time from snapshot (bars: last *completed* bar).
          4) Emit a short replay from ring buffer newer than watermark.
        Returns (handle, watermark_ns).
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per gapless attach is allowed. 'symbol' must be a string.")
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")

        # 1) live subscription (proven path)
        handle = self.subscribe(provider, data_schema, symbol, callback=lambda _: None,
                                fields=fields, only_completed=only_completed)

        # 2) snapshot (default to UTC day start)
        if start_ns is None:
            day_start = pd.Timestamp.utcnow().normalize().tz_localize("UTC")
            start_ns = int(day_start.value)
        snap_tbl = self.snapshot_range(provider, data_schema, symbol, start_ns=start_ns, fields=fields)
        snap_py = snap_tbl.to_pylist()
        for row in snap_py:
            emit_snapshot_row(row)

        # 3) watermark_ns
        wm_ns: Optional[int] = None
        if snap_py:
            # Bars: last completed bar is previous row; trades: last row
            if spec.bin_period_minutes and len(snap_py) >= 2:
                last_completed = snap_py[-2]
                ts_val = last_completed.get(spec.time_col) or last_completed.get("Timestamp") or last_completed.get(
                    "Ts")
            else:
                ts_val = snap_py[-1].get(spec.time_col) or snap_py[-1].get("Timestamp") or snap_py[-1].get("Ts")
            try:
                wm_ns = int(pd.Timestamp(ts_val, tz="UTC").value)
            except Exception:
                wm_ns = None

        # 4) short replay from ring buffer (row-wise)
        try:
            self.replay_since_ns(provider, data_schema, symbol, wm_ns, emit=emit_replay_row, fields=fields)
        except Exception:
            pass

        return handle, wm_ns

    def stats(self) -> dict:
        """Return a snapshot of internal state for diagnostics / monitoring.

        Contents:
          total_listeners: number of active symbol listeners
          total_handles: number of active subscription handles
          total_subscriptions: total callbacks registered (same as total_handles)
          total_symbols: distinct symbol keys (same as total_listeners)
          buffer_total_messages: sum of buffered batches across listeners.
          listeners: list of per-listener dicts (provider/schema/symbol, ref_count, subscriber_count, buffer_len, last_* row counts)
        """
        listeners_info = []
        buffer_total = 0
        for key, lsn in self._lsn.items():
            try:
                snap = lsn.snapshot()
            except Exception:
                snap = {'provider': lsn.provider, 'schema': lsn.data_schema, 'symbol': lsn.symbol, 'error': 'snapshot-failed'}
            ref_count = self._refs.get(key, 0)
            subs = self._subs.get(key, set())
            subscriber_count = len(subs) if subs else 0
            buffer_len = snap.get('buffer_len', 0)
            buffer_total += buffer_len
            snap.update({
                'key': key,
                'ref_count': ref_count,
                'subscriber_count': subscriber_count,
            })
            listeners_info.append(snap)
        return {
            'total_listeners': len(self._lsn),
            'total_handles': len(self._handles),
            'total_subscriptions': len(self._handles),
            'total_symbols': len(self._lsn),
            'buffer_total_messages': buffer_total,
            'listeners': listeners_info,
        }

    # --- internal -----------------------------------------------------
    def _maybe_core_beat(self):
        now = time.time()
        if now - self._last_core_meta_ts >= self._core_meta_interval:
            try:
                self._hb.beat("running", meta={"listeners": len(self._lsn), "subs": len(self._handles)})
            except Exception:
                pass
            self._last_core_meta_ts = now



# Expose singleton for App Mode
market_feeder = MarketFeeder()
SYM_LISTENER = _SymListener
