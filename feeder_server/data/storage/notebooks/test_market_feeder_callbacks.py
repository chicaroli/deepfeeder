"""Example: independent callbacks for added / updated / completed batches.

Non-blocking version:
 - Call subscribe_callbacks() to register callbacks.
 - Use returned handle(s) to unsubscribe later.
 - Does not block the main thread or sleep-loop.
"""
from __future__ import annotations
import deepfeeder as df
from typing import List, Tuple

PROVIDER = "binance"
SCHEMA = "ohlcv_1m"   # or "trades"
SYMBOL = "BTCUSDT"
ONLY_COMPLETED = False  # set True to suppress added/updated dispatch

# --- Independent callbacks ---

def on_added(tbl, meta: dict):
    if not tbl:
        return
    try:
        print(f"ADDED rows={tbl.num_rows} ts={meta.get('timestamp')} symbol={meta.get('symbol')}")
    except Exception:
        print("ADDED (rows=?)")


def on_updated(tbl, meta: dict):
    if not tbl:
        return
    try:
        print(f"UPDATED rows={tbl.num_rows} ts={meta.get('timestamp')} symbol={meta.get('symbol')}")
    except Exception:
        print("UPDATED (rows=?)")


def on_completed(tbl, meta: dict):
    if not tbl or getattr(tbl, "num_rows", 0) == 0:
        return
    # Extract last row without to_pandas() to stay light
    idx = tbl.num_rows - 1
    names = list(tbl.schema.names)
    row = {name: tbl.column(i)[idx].as_py() for i, name in enumerate(names)}
    print(f"COMPLETED bar: {row}")

# --- Wrapper passed to MarketFeeder ---

def feeder_callback(msg: dict):
    meta = msg.get("meta", {})
    if not ONLY_COMPLETED:
        a = msg.get("added")
        if a is not None:
            on_added(a, meta)
        u = msg.get("updated")
        if u is not None:
            on_updated(u, meta)
    c = msg.get("completed")
    if c is not None and getattr(c, "num_rows", 0) > 0:
        on_completed(c, meta)

# --- Public helpers ---

def subscribe_callbacks(symbol: str = SYMBOL, only_completed: bool = ONLY_COMPLETED) -> str:
    """Subscribe using independent callbacks; returns subscription handle."""
    handle = df.fanout.market_feeder.subscribe(PROVIDER, SCHEMA, symbol, feeder_callback, only_completed=only_completed)
    print(f"Subscribed {symbol} handle={handle} only_completed={only_completed}")
    return handle


def unsubscribe(handle: str):
    """Unsubscribe a previously created handle."""
    try:
        df.fanout.market_feeder.unsubscribe(handle)
        print(f"Unsubscribed {handle}")
    except Exception as e:
        print(f"Unsubscribe failed {handle}: {e}")

# Optional batch subscribe helper

def subscribe_many(symbols: List[str]) -> List[str]:
    return [subscribe_callbacks(s, ONLY_COMPLETED) for s in symbols]

# Auto-subscribe on import (comment out if you prefer manual)
default_handle = subscribe_callbacks()

__all__ = [
    "subscribe_callbacks",
    "unsubscribe",
    "subscribe_many",
    "on_added",
    "on_updated",
    "on_completed",
    "feeder_callback",
]
