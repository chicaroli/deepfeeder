"""
MarketFeeder v0.2
- Multi-provider, multi-schema (via SchemaRegistry)
- Server-side listen (App Mode)
- Gapless (subscribe-first -> snapshot -> replay)
- Per-client field masks (column subset)
"""
from __future__ import annotations
import pandas as pd
from collections import deque
from dataclasses import dataclass
from typing import Callable, Dict, Set, Optional, Iterable, Tuple, Any, Union

import pyarrow as pa
from deephaven.table_listener import listen, TableListener, TableUpdate

# --- PROVIDER TABLE IMPORTS (bind your providers here) -----------------------
from providers.binance_schema import (
    binance_trades_table,
    binance_ohlcv_1m
)
from providers.tradingview_schema import (
    tv_quotes_table,
    tv_ohlcv_1m_from_quotes,
    tv_ohlcv_5m_from_quotes
)
# If you add TradingView, etc., import their table getters similarly.


# ---- Schema registry --------------------------------------------------------
@dataclass(frozen=True)
class SchemaSpec:
    # A callable returning a DH Table for this provider+schema (day-scoped if possible)
    table_fn: Callable[[], Any]
    # Column names in that table
    time_col: str
    symbol_col: str
    cols: Tuple[str, ...]       # superset to subscribe to
    bin_period_minutes: Optional[int]

# Minimal registry. Add more entries as you integrate providers/schemas.
SCHEMAS: Dict[Tuple[str, str], SchemaSpec] = {
    ("binance", "ohlcv_1m"): SchemaSpec(
        table_fn=binance_ohlcv_1m,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Trades", "Vwap",
              "BuyerMakerCount"),
        bin_period_minutes=1,
    ),
    ("binance", "trades"): SchemaSpec(
        table_fn=binance_trades_table,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "TradeId", "Price", "Quantity", "BuyerID", "SellerID", "IsBuyerMaker"),
        bin_period_minutes=None,  # trades are not binned
    ),

    ("tradingview", "quotes"): SchemaSpec(
        table_fn=tv_quotes_table,
        time_col="LpTime",
        symbol_col="Symbol",
        cols=("LpTime", "Symbol", "LastPrice", "Bid", "Ask", "Volume", "Change", "ChangePct", "VolDelta"),
        bin_period_minutes=None,  # quotes are not binned
    ),
    ("tradingview", "ohlcv_1m"): SchemaSpec(
        table_fn=tv_ohlcv_1m_from_quotes,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap"),
        bin_period_minutes=1,
    ),
    ("tradingview", "ohlcv_5m"): SchemaSpec(
        table_fn=tv_ohlcv_5m_from_quotes,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap"),
        bin_period_minutes=5,
    ),
    # Add more schemas/providers here as needed
}

# ---- Utils -----------------------------------------------------------------
def _iso(ts) -> str:
    return pd.Timestamp(ts, tz="UTC").isoformat().replace("+00:00","Z")

def _today_expr(spec: SchemaSpec, symbol_expr: str) -> str:
    # Use string literals for time bins in Deephaven expressions
    return (
        f"{symbol_expr} && "
        f"{spec.time_col} >= lowerBin(now(), 'DAY') && "
        f"{spec.time_col} <= lowerBin(now(), 'MINUTE')"
    )

def _symbol_filter_expr(spec: SchemaSpec, symbol: Union[str, Iterable[str]]) -> str:
    col = spec.symbol_col
    if isinstance(symbol, str):
        return f"{col}==`{symbol}`"
    syms = list(symbol)
    if not syms:
        return "false"
    ors = " || ".join(f"{col}==`{s}`" for s in syms)
    return f"({ors})"

def _filter_fields(msg: dict, fields: Optional[Iterable[str]]) -> dict:
    if not fields:
        return msg
    keep = set(fields) | {"version", "type", "symbol", "timestamp"}
    return {k: v for k, v in msg.items() if k in keep}

def _rename_snapshot_cols(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    out = df.rename(columns=mapping).copy()
    if "Timestamp" in out.columns:
        out["Timestamp"] = pd.to_datetime(out["Timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "Symbol" in out.columns and out["Symbol"].dtype == object:
        out["Symbol"] = out["Symbol"].str.lower()
    return out

# ---- Listener per (provider, schema, symbol set) ---------------------------
class _SymListener(TableListener):
    def __init__(self, provider: str, data_schema: str, symbol: str, spec: SchemaSpec, view, emit_completed):
        self.provider = provider
        self.data_schema = data_schema
        self.symbol = symbol
        self.spec = spec
        self.view = view
        self.emit_completed = emit_completed
        self.curr: Optional[dict] = None
        self.buf: deque[dict] = deque(maxlen=256)
        self.handle = listen(view, self)

    def start(self):
        self.handle.start()

    def stop(self):
        try: self.handle.stop()
        except Exception: pass

    def on_update(self, update: TableUpdate, is_replay: bool):
        """
        Forwards all added, updated, and completed rows as separate PyArrow Tables in a batch dict to the callback and buffer.
        Batch structure: {'added': pa.Table, 'updated': pa.Table, 'completed': pa.Table, 'meta': dict}
        OHLCV: completed bars are detected when a new bar is added (previous bar is completed).
        Trades/Quotes: all adds are complete.
        """
        # Prepare lists for completed bars (OHLCV)
        added_tbl = None
        updated_tbl = None
        completed_tbl = None
        # Added rows
        if update.added():
            added_tbl = pa.table(update.added())
            if self.spec.bin_period_minutes:
                # OHLCV: detect completed bars using PyArrow
                time_col_idx = added_tbl.schema.get_field_index(self.spec.time_col)
                completed_rows = []
                for i in range(added_tbl.num_rows):
                    ts = added_tbl.column(time_col_idx)[i].as_py()
                    # Build a dict for the current row
                    row_dict = {name: added_tbl.column(j)[i].as_py() for j, name in enumerate(added_tbl.schema.names)}
                    if self.curr is not None:
                        prev_ts = self.curr[self.spec.time_col]
                        # If new bar, previous bar is completed
                        if ts > prev_ts:
                            completed_rows.append(self.curr)
                    self.curr = row_dict
                if completed_rows:
                    # Build completed_tbl from list of dicts using PyArrow
                    completed_tbl = pa.Table.from_pylist(completed_rows)
            else:
                # Trades/Quotes: all adds are complete
                completed_tbl = added_tbl
        # Updated rows
        if update.modified():
            updated_tbl = pa.table(update.modified())
        batch = {
            'added': added_tbl,
            'updated': updated_tbl,
            'completed': completed_tbl,
            'meta': {
                'provider': self.provider,
                'schema': self.data_schema,
                'symbol': self.symbol,
                'timestamp': pd.Timestamp.now(tz='UTC').isoformat(),
            }
        }
        self.buf.append(batch)
        self.emit_completed(batch)

    def on_error(self, e: Exception):
        print(f"[MarketFeeder] listener error ({self.provider}/{self.data_schema}): {e}")


# ---- MarketFeeder ----------------------------------------------------------
class MarketFeeder:
    def __init__(self):
        self._lsn: Dict[str, _SymListener] = {}
        self._refs: Dict[str, int] = {}
        self._subs: Dict[str, Set[Callable[[dict], None]]] = {}
        self._handles: Dict[str, Tuple[str, Callable[[dict], None], Optional[Iterable[str]]]] = {}

    def _key(self, provider: str, schema: str, symbol: str) -> str:
        """
        Generate a unique key for a subscription. Symbol must be a string.
        """
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
            for h, (k, cb, fields) in list(self._handles.items()):
                if k != key:
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
                  fields: Optional[Iterable[str]] = None) -> str:
        """
        Subscribe to updates for a single symbol. Symbol must be a string.
        """
        if not isinstance(symbol, str):
            raise ValueError("Only one symbol per subscription is allowed. 'symbol' must be a string.")
        key = self._key(provider, data_schema, symbol)
        self._refs[key] = self._refs.get(key, 0) + 1
        self._ensure_listener(provider, data_schema, symbol)
        handle = f"{key}:{id(callback)}"
        self._handles[handle] = (key, callback, set(fields) if fields else None)
        self._subs.setdefault(key, set()).add(callback)
        return handle

    def unsubscribe(self, handle: str) -> None:
        info = self._handles.pop(handle, None)
        if not info:
            return
        key, callback, _ = info
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
        try: t.release()
        except Exception: pass
        rename = {
            spec.time_col: "timestamp",
            spec.symbol_col: "symbol",
            "Open":"o","High":"h","Low":"l","Close":"c","Volume":"v"
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
