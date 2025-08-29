from __future__ import annotations
from core.contracts import Tick

"""
Binance WebSocket Trade Stream Fields Definition
    'wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade'

This describes the fields returned in the `btcusdt@trade/ethusdt@trade` stream from the Binance WebSocket API.
Each message is a JSON object containing trade data with the following fields:
- s (str): Symbol of the trading pair (e.g., 'BTCUSDT').
- e (str): Event type, always 'trade' for this stream.
- E (int): Event time, timestamp of the event in milliseconds (Unix epoch).
- T (int): Trade time, timestamp of the trade in milliseconds (Unix epoch).
- p (str): Price at which the trade was executed (e.g., '59234.56').
- q (str): Quantity of the asset traded (e.g., '0.0123').
- b (int): Buyer order ID, unique identifier for the buyer's order.
- a (int): Seller order ID, unique identifier for the seller's order.
- t (int): Trade ID, unique identifier for the trade.
- m (bool): Is buyer the market maker? `True` if the buyer is the market maker (seller initiated), `False` otherwise.
- M (bool): Ignore, used internally by Binance (typically `True`).

sample messages:
{'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1756493800154, 's': 'BTCUSDT', 't': 5202022819, 'p': '108158.12000000', 'q': '0.00252000', 'T': 1756493800154, 'm': True, 'M': True}}
{'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1756493800155, 's': 'BTCUSDT', 't': 5202022820, 'p': '108158.12000000', 'q': '0.00183000', 'T': 1756493800155, 'm': True, 'M': True}}
{'stream': 'ethusdt@trade', 'data': {'e': 'trade', 'E': 1756493800193, 's': 'ETHUSDT', 't': 2798535000, 'p': '4311.63000000', 'q': '0.00120000', 'T': 1756493800193, 'm': True, 'M': True}}

Note:
- The `data` object contains the trade fields, while `stream` indicates the stream name.
- Price (`p`) and quantity (`q`) are strings to preserve precision.
- Timestamps (`E`, `T`) are in milliseconds since the Unix epoch.
"""

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
