from __future__ import annotations
from typing import Dict, List, Optional, Union
from datetime import datetime, timezone
from deephaven.time import to_j_instant
from core.contracts import Tick

Number = Union[int, float]

# --- helpers ---------------------------------------------------------------
def _f(x) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _i(x) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def _b(x) -> bool:
    return bool(x)

def _ts_s_to_ns(ts_seconds: Number) -> int:
    # TV WS `lp_time` is seconds since epoch; normalize to ns
    return int(float(ts_seconds) * 1_000_000_000)

def _ns_to_instant(ns: Optional[int]):
    if ns is None:
        return None
    return to_j_instant(datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc))

# --- adapters --------------------------------------------------------------
def tv_quote_ws_to_tick(
    exchange: Optional[str], symbol_raw: str, v: dict, *,
    ts_ns_override: Optional[int] = None, vol_delta: Optional[float] = None
) -> Tick:
    # lp_time (seconds) -> ns
    ts_ns = ts_ns_override if ts_ns_override is not None else (
        _ts_s_to_ns(v["lp_time"]) if v.get("lp_time") is not None else None
    )
    if ts_ns is None:
        # As a last resort, you can inject "arrival" time upstream and pass via ts_ns_override
        raise ValueError("TradingView quote missing timestamp (lp_time) and ts_ns_override not provided")

    exch = (exchange or "").upper()
    sym  = symbol_raw.upper()

    return Tick(
        provider="tradingview",
        stream="quotes",
        symbol=sym,
        ts_ns=ts_ns,
        seq=None,
        is_final=False,
        payload={
            "exchange":   exch,
            "last_price": _f(v.get("lp")),
            "bid":        _f(v.get("bid")),
            "ask":        _f(v.get("ask")),
            "volume":     _f(v.get("volume")),
            "change":     _f(v.get("ch")),
            "change_pct": _f(v.get("chp")),
            "vol_delta":  _f(vol_delta if vol_delta is not None else 0.0),
        },
    )

def tv_api_bar_to_tick(exchange: Optional[str], symbol_raw: str, bar: dict) -> Tick:
    ts_ns = _ts_s_to_ns(bar["t"])  # TV bar.open in seconds
    exch = (exchange or "").upper()
    sym  = symbol_raw.upper()

    return Tick(
        provider="tradingview",
        stream="ohlcv_1m",
        symbol=sym,
        ts_ns=ts_ns,
        seq=None,
        is_final=True,
        payload={
            "exchange": exch,
            "open":  _f(bar.get("o")),
            "high":  _f(bar.get("h")),
            "low":   _f(bar.get("l")),
            "close": _f(bar.get("c")),
            "volume": _f(bar.get("v")),
        },
    )

# --- flatteners (match DH schema exactly) ----------------------------------
def flatten_quotes(ticks: List[Tick]) -> Dict[str, List]:
    # Matches _QUOTES_DTW: Exchange, Symbol, LpTime(Instant), LastPrice, Bid, Ask, Volume, Change, ChangePct, VolDelta
    return {
        "Exchange":  [t.payload.get("exchange", "") for t in ticks],
        "Symbol":    [t.symbol for t in ticks],
        "LpTime":    [_ns_to_instant(int(t.ts_ns)) for t in ticks],
        "LastPrice": [t.payload.get("last_price") for t in ticks],
        "Bid":       [t.payload.get("bid") for t in ticks],
        "Ask":       [t.payload.get("ask") for t in ticks],
        "Volume":    [t.payload.get("volume") for t in ticks],
        "Change":    [t.payload.get("change") for t in ticks],
        "ChangePct": [t.payload.get("change_pct") for t in ticks],
        "VolDelta":  [t.payload.get("vol_delta") for t in ticks],
    }

def flatten_bar(ticks: List[Tick]) -> Dict[str, List]:
    # Matches _BARS_DTW: Exchange, Symbol, Timestamp(Instant), Open, High, Low, Close, Volume
    return {
        "Exchange":  [t.payload.get("exchange", "") for t in ticks],
        "Symbol":    [t.symbol for t in ticks],
        "Timestamp": [_ns_to_instant(int(t.ts_ns)) for t in ticks],
        "Open":      [t.payload.get("open")   for t in ticks],
        "High":      [t.payload.get("high")   for t in ticks],
        "Low":       [t.payload.get("low")    for t in ticks],
        "Close":     [t.payload.get("close")  for t in ticks],
        "Volume":    [t.payload.get("volume") for t in ticks],
    }

# Optional: if you materialize provisional bars from quotes into a separate table
def flatten_bars_from_quotes(ticks: List[Tick]) -> Dict[str, List]:
    # Only use if your DH table matches these columns; no IsFinal column in _BARS_DTW.
    return flatten_bar(ticks)
