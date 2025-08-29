from __future__ import annotations
from core.contracts import Tick

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
            "open": float(bar["o"]), "high": float(bar["h"]),
            "low": float(bar["l"]), "close": float(bar["c"]),
            "volume": float(bar["v"]),
        },
        is_final=True,                        # authoritative
    )
