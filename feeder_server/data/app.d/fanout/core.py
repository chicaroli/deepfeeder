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
from .protocol import envelope_header, ROW_TYPE_TRADE, ROW_TYPE_BAR
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
        # Sequence counters for unified event streams per (provider|schema|symbol)
        self._seqs: Dict[str, int] = {}

    @staticmethod
    def _key(provider: str, schema: str, symbol: str, exchange: Optional[str] = None) -> str:
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        return f"{provider}|{schema}|{symbol}" + (f"@{exchange.upper()}" if exchange else "")

    def _next_seq(self, key: str) -> int:
        """Return next monotonic integer for this stream and increment it."""
        v = self._seqs.get(key, 0)
        self._seqs[key] = v + 1
        return v

    def _ensure_listener(self, provider: str, data_schema: str, symbol: str, exchange: Optional[str] = None):
        key = self._key(provider, data_schema, symbol, exchange)
        if key in self._lsn:
            return
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")
        base = spec.table_fn()
        sym_expr = _symbol_filter_expr(spec, symbol)
        view = base.where(sym_expr)
        if exchange and "Exchange" in spec.cols:
            view = view.where([f'Exchange == `{exchange.upper()}`'])
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
            emit_event(
                "fanout", f"{provider}:{data_schema}:{symbol}", "core", "INFO", "LISTENER_CREATE", "Creating listener",
                {"provider": provider, "schema": data_schema, "symbol": symbol, "buf_len": buf_len}
            )
        except Exception:
            pass
        lsn = _SymListener(provider, data_schema, symbol, spec, view, emit_completed, buf_maxlen=buf_len)
        lsn.start()
        # Event: listener started
        try:
            emit_event(
                "fanout", f"{provider}:{data_schema}:{symbol}", "core", "INFO", "LISTENER_START", "Listener started"
            )
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
                  only_completed: bool = False,
                  exchange: Optional[str] = None) -> str:
        """Subscribe to a symbol stream.

        Params:
          - fields: Optional iterable of column names to project from Arrow tables.
          - only_completed: If True, deliver only the 'completed' table batches (no 'added' or 'updated').
                            If False, deliver all of added/updated/completed as available.
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol, exchange)
        self._refs[key] = self._refs.get(key, 0) + 1
        self._ensure_listener(provider, data_schema, symbol, exchange)
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

    @staticmethod
    def snapshot_range(
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
        # Exclude placeholder rows if the filled schema exposes IsEmpty
        if "IsEmpty" in spec.cols:
            flts.append(dff.Filter.from_("IsEmpty == false"))
        if start_ns is not None:
            start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
            # Use Instant.parse for Deephaven compatibility
            flts.append(
                dff.Filter.from_(f'{spec.time_col} >= Instant.parse("{start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}")'))
        if end_ns is not None:
            end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=timezone.utc)
            flts.append(
                dff.Filter.from_(f'{spec.time_col} <= Instant.parse("{end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")}")'))

        # Query DH object
        t = base.where(flts).view(list(spec.cols)).sort([spec.time_col])
        arr = to_arrow(t)

        # Column filtering (case-insensitive)
        if fields:
            must_keep = {spec.time_col, spec.symbol_col}
            keep = [n for n in arr.schema.names if n in fields or n in must_keep]
            arr = arr.select(keep) if keep else pa.table({})
        return arr

    def snapshot_rows(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            *,
            exchange: Optional[str] = None,
            start_ns: Optional[int] = None,
            end_ns: Optional[int] = None,
            fields: Optional[Iterable[str]] = None,
    ) -> Iterator[dict]:
        """Convenience: iterate dict rows from snapshot_range()."""
        arr = self.snapshot_range(provider, data_schema, symbol, start_ns=start_ns, end_ns=end_ns, fields=fields,
                                  exchange=exchange)
        yield from arr.to_pylist()

    def catchup_since_ns(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            since_ns: Optional[int],
            *,
            exchange: Optional[str] = None,
            emit: Callable[[dict], None],
            fields: Optional[Iterable[str]] = None,
            only_completed: Optional[bool] = None,
    ) -> int:
        """Emit buffered listener batches newer than since_ns as unified JSON events.
        Internal catch-up used during attachment; not the future backtest/"replay" API.
        Returns the number of events emitted.
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per replay is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol, exchange)
        lsn = self._lsn.get(key)
        if not lsn:
            return 0
        emitted = 0
        parts = ("completed",) if only_completed else None
        for msg in list(lsn.buf):
            meta = msg.get("meta") or {}
            ts_ns = meta.get("ts_ns")
            if ts_ns is None:
                ts_iso = meta.get("timestamp")
                if not ts_iso:
                    continue
                ts_ns = int(pd.Timestamp(ts_iso, tz="UTC").value)
            if since_ns is None or ts_ns > since_ns:
                try:
                    emitted += self._emit_tables_as_events(
                        phase="snapshot",
                        provider=provider,
                        data_schema=data_schema,
                        symbol=symbol,
                        key=key,
                        msg=msg,
                        emit=emit,
                        watermark_ns=None,
                        fields=fields,
                        parts=parts,
                    )
                except Exception:
                    pass
        return emitted

    def _emit_tables_as_events(
            self,
            *,
            phase: str,
            provider: str,
            data_schema: str,
            symbol: str,
            key: str,
            msg: dict,
            emit: Callable[[dict], None],
            watermark_ns: Optional[int] = None,
            fields: Optional[Iterable[str]] = None,
            parts: Optional[Iterable[str]] = None,
    ) -> int:
        """Convert an in-memory batch msg with Arrow tables into JSON row events and emit one per non-empty part.
        Returns the number of events emitted.
        """
        parts_iter = tuple(parts) if parts is not None else ("added", "updated", "completed")
        meta = dict(msg.get("meta") or {})
        # Add stream identity to meta for convenience
        meta.setdefault("provider", provider)
        meta.setdefault("data_schema", data_schema)
        meta.setdefault("symbol", symbol)
        meta.setdefault("only_completed", False)
        spec = SCHEMAS.get((provider, data_schema))
        if fields:
            try:
                meta.setdefault("fields", list(fields))
            except Exception:
                pass
        events = 0
        for part in parts_iter:
            tbl = msg.get(part)
            try:
                n = getattr(tbl, "num_rows", 0)
            except Exception:
                n = 0
            if not tbl or n == 0:
                continue
            try:
                rows = tbl.to_pylist()
            except Exception:
                # Fallback: deliver empty if conversion fails
                rows = []
            env = {
                **envelope_header(),
                "phase": phase,
                "part": part,
                "provider": provider,
                "data_schema": data_schema,
                "symbol": symbol,
                "row_mode": True,
                "row_type": ROW_TYPE_BAR if spec.bin_period_minutes else ROW_TYPE_TRADE,
                "rows": rows,
                "watermark_ns": None if phase != "snapshot" else watermark_ns,
                "meta": {**meta, "seq": self._next_seq(key)},
            }
            try:
                emit(env)
                events += 1
            except Exception:
                # continue best-effort for other parts
                pass
        return events

    def attach_gapless(
            self,
            provider: str,
            data_schema: str,
            symbol: str,
            *,
            exchange: Optional[str] = None,
            emit: Callable[[dict], None],
            fields: Optional[Iterable[str]] = None,
            only_completed: bool = True,
            start_ns: Optional[int] = None,
            snapshot_batch: bool = True,
    ) -> Tuple[str, Optional[int]]:
        """Attach to a stream and emit JSON-only unified events across phases: snapshot, snapshot_boundary, replay, live.

        - emit receives a dict with keys: version, phase, part, provider, data_schema, symbol,
          row_mode=True, rows=[...], watermark_ns (for snapshot & boundary), meta={...}.
        - For live, one event per non-empty part is emitted.
        - For snapshot, by default a single batched event with all rows is emitted (snapshot_batch=True).
        Returns (subscription_handle, watermark_ns).
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per gapless attach is allowed. 'symbol' must be a string.")
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")
        key = self._key(provider, data_schema, symbol, exchange)
        # Reset per-stream sequence at attach start
        self._seqs[key] = 0

        # Live subscription: wrap DH tables into JSON events
        def _live_cb(msg: dict):
            m = dict(msg or {})
            mm = dict(m.get("meta") or {})
            mm["only_completed"] = bool(only_completed)
            m["meta"] = mm
            try:
                self._emit_tables_as_events(
                    phase="live", provider=provider, data_schema=data_schema, symbol=symbol, key=key,
                    msg=m, emit=emit, watermark_ns=None, fields=fields)
            except Exception:
                pass

        handle = self.subscribe(provider, data_schema, symbol, callback=_live_cb,
                                fields=fields, only_completed=only_completed, exchange=exchange)

        # Compute default start if not provided
        if start_ns is None:
            day_start = pd.Timestamp.utcnow().normalize()
            if day_start.tzinfo is None:
                day_start = day_start.tz_localize("UTC")
            else:
                day_start = day_start.tz_convert("UTC")
            start_ns = int(day_start.value)

        # Snapshot as a single batched event (JSON rows)
        snap_tbl = self.snapshot_range(provider, data_schema, symbol, start_ns=start_ns, fields=fields,
                                       exchange=exchange)
        snap_rows = snap_tbl.to_pylist() if snap_tbl is not None else []

        # Compute watermark (robust logic) BEFORE emitting snapshot so clients can resume
        wm_ns: Optional[int] = None
        if snap_rows:
            candidate_idx = -1
            if spec.bin_period_minutes:
                last_row = snap_rows[-1]
                is_final = any(last_row.get(k) is True for k in ("IsFinal", "is_final", "Final", "final"))
                if not is_final and len(snap_rows) >= 2:
                    candidate_idx = -2
            ts_val = (snap_rows[candidate_idx].get(spec.time_col) or
                      snap_rows[candidate_idx].get("Timestamp") or
                      snap_rows[candidate_idx].get("Ts"))
            try:
                ts_obj = pd.Timestamp(ts_val)
                if ts_obj.tzinfo is None:
                    ts_obj = ts_obj.tz_localize("UTC")
                else:
                    ts_obj = ts_obj.tz_convert("UTC")
                wm_ns = int(ts_obj.value)
            except Exception as e:
                print(f"[attach_gapless] watermark parse failed: {ts_val!r} ({type(ts_val)}) -> {e}")
                wm_ns = None

        # Emit snapshot event(s) including watermark_ns so clients can resume
        if snapshot_batch:
            env = {
                **envelope_header(),
                "phase": "snapshot",
                "part": "completed",  # snapshot contains completed history
                "provider": provider,
                "data_schema": data_schema,
                "symbol": symbol,
                "row_mode": True,
                "row_type": ROW_TYPE_BAR if spec.bin_period_minutes else ROW_TYPE_TRADE,
                "rows": snap_rows,
                "watermark_ns": wm_ns,
                "meta": {
                    "seq": self._next_seq(key),
                    "only_completed": bool(only_completed),
                    "fields": list(fields) if fields else [],
                    "start_ns": int(start_ns) if start_ns is not None else None,
                    "end_ns": None,
                },
            }
            try:
                emit(env)
            except Exception:
                pass
        else:
            for row in snap_rows:
                env = {
                    **envelope_header(),
                    "phase": "snapshot",
                    "part": "completed",
                    "provider": provider,
                    "data_schema": data_schema,
                    "symbol": symbol,
                    "row_mode": True,
                    "row_type": ROW_TYPE_BAR if spec.bin_period_minutes else ROW_TYPE_TRADE,
                    "rows": [row],
                    "watermark_ns": wm_ns,
                    "meta": {
                        "seq": self._next_seq(key),
                        "only_completed": bool(only_completed),
                        "fields": list(fields) if fields else [],
                        "start_ns": int(start_ns) if start_ns is not None else None,
                        "end_ns": None,
                    },
                }
                try:
                    emit(env)
                except Exception:
                    pass

        # Replay buffered then boundary
        try:
            self.catchup_since_ns(provider, data_schema, symbol, wm_ns, emit=emit, fields=fields, only_completed=only_completed)
        except Exception:
            pass

        boundary_env = {
            **envelope_header(),
            "phase": "snapshot_boundary",
            "part": None,
            "provider": provider,
            "data_schema": data_schema,
            "symbol": symbol,
            "row_mode": True,
            "row_type": ROW_TYPE_BAR if spec.bin_period_minutes else ROW_TYPE_TRADE,
            "rows": [],
            "watermark_ns": wm_ns,
            "meta": {"seq": self._next_seq(key), "only_completed": bool(only_completed)},
        }
        try:
            emit(boundary_env)
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
