from __future__ import annotations
from typing import Dict, List
from core.contracts import Tick

# TradingView adapter: convert TV JSON to Tick
def quote_json_to_tick(symbol: str, q: dict) -> Tick:
    bid = float(q.get("bid") or 0.0)
    ask = float(q.get("ask") or 0.0)
    mid = float(q.get("mid") or ((bid + ask) / 2.0 if (bid and ask) else 0.0))
    return Tick(
        provider="tradingview",
        stream="quotes",
        symbol=symbol.upper(),
        ts_ns=int(q["ts"]) * 1_000_000_000,   # s→ns
        seq=None,
        payload={"bid": bid, "ask": ask, "mid": mid},
        is_final=False,                       # provisional
    )

def api_bar_to_tick(symbol: str, bar: dict) -> Tick:
    return Tick(
        provider="tradingview",
        stream="ohlcv_1m",
        symbol=symbol.upper(),
        ts_ns=int(bar["t"]) * 1_000_000_000,  # bar open time
        seq=None,
        payload={
            "open": float(bar["o"]),
            "high": float(bar["h"]),
            "low": float(bar["l"]),
            "close": float(bar["c"]),
            "volume": float(bar["v"]),
        },
        is_final=True,                        # authoritative
    )


# Flatten TV quotes ticks into a dict of columns
def flatten_quotes(ticks: List[Tick]) -> Dict[str, List]:
    return {
        "Provider": [t.provider for t in ticks],
        "Stream":   [t.stream   for t in ticks],      # "quotes"
        "Symbol":   [t.symbol   for t in ticks],
        "TsNanos":  [t.ts_ns    for t in ticks],
        "Seq":      [(-1 if t.seq is None else t.seq) for t in ticks],
        "IsFinal":  [t.is_final for t in ticks],      # False here
        "Bid":      [t.payload.get("bid") for t in ticks],
        "Ask":      [t.payload.get("ask") for t in ticks],
        "Mid":      [t.payload.get("mid") for t in ticks],
        # Add more TV-specific cols only if your DH table has them.
    }

def flatten_bar(ticks: List[Tick]) -> Dict[str, List]:
    return {
        "Provider": [t.provider for t in ticks],
        "Stream":   [t.stream   for t in ticks],      # "ohlcv_1m"
        "Symbol":   [t.symbol   for t in ticks],
        "TsNanos":  [t.ts_ns    for t in ticks],
        "Seq":      [(-1 if t.seq is None else t.seq) for t in ticks],
        "IsFinal":  [t.is_final for t in ticks],      # True for API bars
        "Open":     [t.payload.get("open")   for t in ticks],
        "High":     [t.payload.get("high")   for t in ticks],
        "Low":      [t.payload.get("low")    for t in ticks],
        "Close":    [t.payload.get("close")  for t in ticks],
        "Volume":   [t.payload.get("volume") for t in ticks],
    }

# Optional: if you materialize provisional bars from quotes into a separate table
def flatten_bars_from_quotes(ticks: List[Tick]) -> Dict[str, List]:
    out = flatten_bar(ticks)
    # If your DH table expects IsFinal=False here, enforce it:
    out["IsFinal"] = [False] * len(ticks)
    return out
