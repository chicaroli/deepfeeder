from __future__ import annotations
import asyncio
import json
from typing import Optional, Iterable, Dict, Any, Callable
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
    return {"added": "open", "updated": "update", "completed": "close", "snapshot": "snapshot"}.get(part, "update")

def _res_from_schema(schema: str) -> str | None:
    # e.g., "ohlcv_5m" → "5m"; anything else → None
    if "ohlcv_" in schema:
        return schema.split("ohlcv_", 1)[1]
    return None

# ---------- row mappers (bars/trades) ----------

def adapt_bar_row(row: Dict[str, Any], *, provider: str, schema: str, part: str) -> Dict[str, Any]:
    return {
        "type": "bar",
        "exchange": provider.upper(),                    # adjust if you keep exchange elsewhere
        "symbol": row.get("Symbol"),
        "res": _res_from_schema(schema),
        "bar_id": int(row.get("BarId")) if row.get("BarId") is not None else None,
        "ts": _ts_to_iso(row.get("Timestamp")),
        "open": row.get("Open"),
        "high": row.get("High"),
        "low": row.get("Low"),
        "close": row.get("Close"),
        "volume": row.get("Volume"),
        "closed": part in ("completed", "snapshot"),
        "phase": _phase_from_part(part),
    }

def adapt_trade_row(row: Dict[str, Any], *, provider: str, schema: str, part: str) -> Dict[str, Any]:
    return {
        "type": "trade",
        "exchange": provider.upper(),
        "symbol": row.get("Symbol"),
        "trade_id": row.get("TradeId"),
        "price": row.get("Price"),
        "qty": row.get("Qty"),
        "ts_ns": row.get("Ts"),
        "side": row.get("Side"),
        "phase": "trade",
    }

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
