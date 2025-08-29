from __future__ import annotations
from typing import Dict, List
from core.contracts import Tick

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

def flatten_ohlcv_1m(ticks: List[Tick]) -> Dict[str, List]:
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
def flatten_ohlcv_1m_from_quotes(ticks: List[Tick]) -> Dict[str, List]:
    out = flatten_ohlcv_1m(ticks)
    # If your DH table expects IsFinal=False here, enforce it:
    # out["IsFinal"] = [False] * len(ticks)
    return out
