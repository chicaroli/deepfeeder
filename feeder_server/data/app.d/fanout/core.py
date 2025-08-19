"""
core.py
MarketFeeder orchestration for subscriptions, snapshots, and replay.
"""
import pandas as pd
from typing import Dict, Set, Callable, Optional, Iterable, Tuple
from .listener import _SymListener
from .schemas import SCHEMAS, SchemaSpec
from .utils import _symbol_filter_expr, _filter_fields, _rename_snapshot_cols, _today_expr

class MarketFeeder:
    """Orchestrates listeners, subscriptions, snapshots, and replay for market data."""
    def __init__(self):
        self._lsn: Dict[str, _SymListener] = {}
        self._refs: Dict[str, int] = {}
        self._subs: Dict[str, Set[Callable[[dict], None]]] = {}
        self._handles: Dict[str, Tuple[str, Callable[[dict], None], Optional[Iterable[str]]]] = {}

    def _key(self, provider: str, schema: str, symbol: str) -> str:
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
        view = (base.where(sym_expr)
                    .last_by([spec.symbol_col]) if spec.bin_period_minutes else base.where(sym_expr))
        view = view.view(*spec.cols)
        def emit_completed(msg: dict):
            for h, (k, cb, fields, only_completed) in list(self._handles.items()):
                if k != key:
                    continue
                # Only emit completed bars if requested
                if only_completed:
                    completed = msg.get("completed")
                    if completed is None or (hasattr(completed, "num_rows") and completed.num_rows == 0):
                        continue
                try:
                    cb(_filter_fields(msg, fields))
                except Exception:
                    pass
        lsn = _SymListener(provider, data_schema, symbol, spec, view, emit_completed)
        lsn.start()
        self._lsn[key] = lsn
        self._subs.setdefault(key, set())

    def _destroy_listener_if_unused(self, key: str):
        if self._refs.get(key, 0) > 0:
            return
        l = self._lsn.pop(key, None)
        if l:
            l.stop()
        self._subs.pop(key, None)

    def subscribe(self, provider: str, data_schema: str, symbol: str,
                  callback: Callable[[dict], None],
                  fields: Optional[Iterable[str]] = None,
                  only_completed: bool = False) -> str:
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol)
        self._refs[key] = self._refs.get(key, 0) + 1
        self._ensure_listener(provider, data_schema, symbol)
        handle = f"{key}:{id(callback)}"
        self._handles[handle] = (key, callback, set(fields) if fields else None, only_completed)
        self._subs.setdefault(key, set()).add(callback)
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

    def get_today_snapshot(self, provider: str, data_schema: str, symbol: str,
                           fields: Optional[Iterable[str]] = None) -> pd.DataFrame:
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per snapshot is allowed. 'symbol' must be a string.")
        spec = SCHEMAS.get((provider, data_schema))
        if spec is None:
            raise ValueError(f"Unknown provider/schema: {provider}/{data_schema}")
        base = spec.table_fn()
        sym_expr = _symbol_filter_expr(spec, symbol)
        cols = tuple(spec.cols) if not fields else tuple(
            c for c in spec.cols if (c.lower() in {f.lower() for f in fields} or c in (spec.time_col, spec.symbol_col))
        )
        t = (base.where(_today_expr(spec, sym_expr))
                 .view(*cols)
                 .sort([spec.time_col]))
        df = t.to_pandas()
        try:
            t.release()
        except Exception:
            pass
        rename = {
            spec.time_col: "timestamp",
            spec.symbol_col: "symbol",
            "Open": "o", "High": "h", "Low": "l", "Close": "c", "Volume": "v"
        }
        return _rename_snapshot_cols(df, {c: rename.get(c, c) for c in cols})

    def replay_since(self, provider: str, data_schema: str, symbol: str,
                     watermark_iso: Optional[str],
                     send: Callable[[dict], None],
                     fields: Optional[Iterable[str]] = None):
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per replay is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol)
        lsn = self._lsn.get(key)
        if not lsn:
            return
        wm_ts = pd.to_datetime(watermark_iso, utc=True) if watermark_iso else None
        for msg in list(lsn.buf):
            ts = pd.to_datetime(msg["timestamp"], utc=True)
            if wm_ts is None or ts > wm_ts:
                send(_filter_fields(msg, fields))

    def attach_client_gapless(self, provider: str, data_schema: str, symbol: str,
                              callback: Callable[[dict], None],
                              fields: Optional[Iterable[str]] = None
                              ) -> Tuple[str, pd.DataFrame, Optional[str]]:
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per gapless attach is allowed. 'symbol' must be a string.")
        handle = self.subscribe(provider, data_schema, symbol, callback, fields=fields)
        snap = self.get_today_snapshot(provider, data_schema, symbol, fields=fields)
        wm = None
        if len(snap) >= 2 and "timestamp" in snap.columns:
            wm = snap.iloc[-2]["timestamp"]
        return handle, snap, wm

# Expose singleton for App Mode
MARKET_FEEDER = MarketFeeder()
SYM_LISTENER = _SymListener
