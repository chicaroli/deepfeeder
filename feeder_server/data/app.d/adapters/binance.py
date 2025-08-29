from __future__ import annotations
from core.contracts import Tick

def trade_json_to_tick(msg: dict) -> Tick:
    # Binance trade payload example fields: s, t (tradeId), T (ms), p, q, m (is buyer market maker)
    return Tick(
        provider="binance",
        stream="trades",
        symbol=msg["s"],
        ts_ns=int(msg["T"]) * 1_000_000,
        seq=int(msg["t"]),
        payload={
            "price": float(msg["p"]),
            "qty":   float(msg["q"]),
            "side":  "sell" if msg.get("m") else "buy",
        },
        is_final=True,
    )
