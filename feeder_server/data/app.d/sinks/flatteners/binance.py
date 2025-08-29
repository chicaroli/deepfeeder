# app.d/sinks/flatteners/binance.py
from typing import Dict, List
from core.contracts import Tick

def flatten_trades(ticks: List[Tick]) -> Dict[str, List]:
    return {
        "Provider": [t.provider for t in ticks],
        "Stream":   [t.stream for t in ticks],
        "Symbol":   [t.symbol for t in ticks],
        "TsNanos":  [t.ts_ns for t in ticks],
        "TradeId":  [t.seq for t in ticks],   # Binance sequence is true tradeId
        "Price":    [t.payload.get("price") for t in ticks],
        "Qty":      [t.payload.get("qty") for t in ticks],
        "Side":     [t.payload.get("side") for t in ticks],
        # any Binance-specific columns welcome here
    }
