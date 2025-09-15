from __future__ import annotations
import asyncio
import json
from typing import Optional, Iterable, Dict, Any
from pandas import Timestamp as _PdTs
import pyarrow as pa

from .core import market_feeder, SCHEMAS  # SCHEMAS for schema introspection

# ---------- helpers ----------

def _pa_to_rows(tbl: Optional[pa.Table]) -> Iterable[Dict[str, Any]]:
    if tbl is None:
        return ()
    return tbl.to_pylist()

def _ts_to_iso(ts_val) -> str | None:
    if ts_val is None:
        return None
    try:
        return _PdTs(ts_val, tz="UTC").isoformat().replace("+00:00", "Z")
    except Exception:
        try:
            return _PdTs(ts_val).tz_localize("UTC").isoformat().replace("+00:00", "Z")
        except Exception:
            return str(ts_val)

def _phase_from_part(part: str) -> str:
    return {
        "added": "open",
        "updated": "update",
        "completed": "close",
        "snapshot": "snapshot",
        "trade": "trade"
    }.get(part, "update")

def _res_from_schema(schema: str) -> str | None:
    # e.g., "ohlcv_5m" → "5m"; anything else → None
    if "ohlcv_" in schema:
        return schema.split("ohlcv_", 1)[1]
    return None

# ---------- row mappers (bars/trades) ----------

def adapt_bar_row(row: Dict[str, Any], *, provider: str, schema: str, part: str,
                  fields_list: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    resp = {
        "Type": "bar",
        "Phase": _phase_from_part(part),
        "Provider": row.get("Provider", provider),
        "Symbol": row.get("Symbol"),
        "Timestamp": _ts_to_iso(row.get("Timestamp"))
    }
    if fields_list is None or "Exchange" in fields_list:
        resp["Exchange"] = row.get("Exchange", None)
    if fields_list is None or "BarId" in fields_list:
        resp["BarId"] = int(row.get("BarId")) if row.get("BarId") is not None else None
    if fields_list is None or "Open" in fields_list:
        resp["Open"] = row.get("Open")
    if fields_list is None or "High" in fields_list:
        resp["High"] = row.get("High")
    if fields_list is None or "Low" in fields_list:
        resp["Low"] = row.get("Low")
    if fields_list is None or "Close" in fields_list:
        resp["Close"] = row.get("Close")
    if fields_list is None or "Volume" in fields_list:
        resp["Volume"] = row.get("Volume")
    resp["Res"] = _res_from_schema(schema)
    resp["Closed"] = part in ("completed", "snapshot")
    return resp

def adapt_trade_row(row: Dict[str, Any], *, provider: str, schema: str, part: str,
                    fields_list: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    resp = {
        "Type": "trade",
        "Phase": _phase_from_part(part),
        "Provider": row.get("Provider", provider),
        "Symbol": row.get("Symbol"),
        # Ensure JSON-serializable timestamp (ISO string)
        "Timestamp": _ts_to_iso(row.get("Timestamp") or row.get("Ts")),
    }
    if fields_list is None or "Exchange" in fields_list:
        resp["Exchange"] = row.get("Exchange", None)
    if fields_list is None or "TradeID" in fields_list:
        resp["TradeID"] = row.get("TradeID")
    if fields_list is None or "Price" in fields_list:
        resp["Price"] = row.get("Price")
    if fields_list is None or "Quantity" in fields_list:
        resp["Quantity"] = row.get("Quantity")
    if fields_list is None or "BuyerID" in fields_list:
        resp["BuyerID"] = row.get("BuyerID", None)
    if fields_list is None or "SellerID" in fields_list:
        resp["SellerID"] = row.get("SellerID", None)
    if fields_list is None or "IsBuyerMaker" in fields_list:
        resp["IsBuyerMaker"] = row.get("IsBuyerMaker", None)
    return resp

def _is_bar_schema(provider: str, schema: str) -> bool:
    spec = SCHEMAS.get((provider, schema))
    return bool(spec and spec.bin_period_minutes)

# ---------- per-connection pipe ----------

class ConnPipe:
    """One bounded async queue per client; wires MarketFeeder live callbacks to JSON frames."""
    def __init__(self, provider: str, schema: str, symbol: str, *, maxsize: int = 5000):
        self.provider = provider
        self.schema = schema
        self.symbol = symbol
        self.q: asyncio.Queue[str] = asyncio.Queue(maxsize=maxsize)
        self._handle: Optional[str] = None

    def _on_batch(self, batch: Dict[str, Any]):
        # Convert each part to JSON rows and enqueue
        is_bars = _is_bar_schema(self.provider, self.schema)
        for part in ("added", "updated", "completed"):
            tbl = batch.get(part)
            if tbl is None:
                continue
            for row in _pa_to_rows(tbl):
                msg = (
                    adapt_bar_row(row, provider=self.provider, schema=self.schema, part=part)
                    if is_bars else
                    adapt_trade_row(row, provider=self.provider, schema=self.schema, part=part)
                )
                payload = json.dumps(msg)
                try:
                    self.q.put_nowait(payload)
                except asyncio.QueueFull:
                    # drop-oldest policy to keep stream fresh
                    try:
                        _ = self.q.get_nowait()
                        self.q.put_nowait(payload)
                    except Exception:
                        pass

    def attach(self, *, fields: Optional[Iterable[str]] = None, only_completed: Optional[bool] = None):
        # Default only_completed=True for bars, False for trades
        if only_completed is None:
            only_completed = _is_bar_schema(self.provider, self.schema)
        self._handle = market_feeder.subscribe(
            self.provider, self.schema, self.symbol,
            callback=self._on_batch,
            fields=fields,
            only_completed=only_completed,
        )

    def detach(self):
        try:
            if self._handle:
                market_feeder.unsubscribe(self._handle)
        finally:
            self._handle = None
