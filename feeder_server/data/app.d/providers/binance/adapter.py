# providers/binance/adapter.py
from __future__ import annotations
from typing import Dict, List, Optional
from core.contracts import Tick
from datetime import datetime, timezone
from deephaven.time import to_j_instant

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
            "Price": float(msg["p"]),
            "Quantity": float(msg["q"]),
            "BuyerID": int(msg.get("b", 0)),
            "SellerID": int(msg.get("a", 0)),
            "IsBuyerMaker": bool(msg.get("m", False)),
        },
        is_final=True,
    )



def rest_trade_json_to_tick(r: dict, symbol: Optional[str] = None) -> Tick:
    """Parse a REST-style Binance trade row into a Tick using the canonical payload.

    Expected REST shapes include keys like:
      - id (int), price (str), qty (str), time (ms, int), isBuyerMaker (bool)
    This function is defensive and will try common fallbacks used by Binance.
    """
    # trade id fallbacks
    tid = int( r.get('id') or r.get('tradeId') or r.get('t'))

    # timestamps in ms
    ts_ms = r.get("time") or r.get("T") or r.get("tradeTime") or r.get("timestamp")
    ts_ns = int(ts_ms) * 1_000_000 if ts_ms is not None else 0

    price = r.get("price") or r.get("p")
    qty = r.get("qty") or r.get("q")
    is_buyer_maker = r.get("isBuyerMaker") or r.get("m", False)
    sym = (symbol or r.get("symbol") or r.get("s") or "").upper()

    return Tick(
        provider="binance",
        stream="trades",
        symbol=sym,
        ts_ns=ts_ns,
        seq=tid,
        payload={
            "Price": float(price) if price is not None else 0.0,
            "Quantity": float(qty) if qty is not None else 0.0,
            "BuyerID": int(r.get("b", 0)),
            "SellerID": int(r.get("a", 0)),
            "IsBuyerMaker": bool(is_buyer_maker),
        },
        is_final=True,
    )


def flatten_trades(ticks: List["Tick"]) -> Dict[str, List]:
    """
    Convert Tick -> rowdict matching binance_trades_writer() (10 columns):
      EventType, EventTime, Symbol, TradeID, Price, Quantity, BuyerID, SellerID, Timestamp, IsBuyerMaker
    """
    def _inst(ns: int):
        return to_j_instant(datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc))

    return {
        "EventType":    ["trade"] * len(ticks),
        "EventTime":    [_inst(t.ts_ns) for t in ticks],
        "Symbol":       [t.symbol for t in ticks],
        "TradeID":      [int(t.seq) for t in ticks],
        "Price":        [float(t.payload.get("Price", 0.0)) for t in ticks],
        "Quantity":     [float(t.payload.get("Quantity", 0.0)) for t in ticks],
        "BuyerID":      [int(t.payload.get("b", 0)) for t in ticks],
        "SellerID":     [int(t.payload.get("a", 0)) for t in ticks],
        "Timestamp":    [_inst(t.ts_ns) for t in ticks],
        "IsBuyerMaker": [t.payload.get("IsBuyerMaker", False) for t in ticks],
    }