from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Union, Literal, TypeVar, Generic
from decimal import Decimal

RowType = Literal["bar", "trade"]

from .protocol import ensure_compat, ENVELOPE_VERSION

Phase = Literal["connected", "snapshot", "snapshot_boundary", "live", "replay", "error"]
Part  = Optional[Literal["added", "updated", "completed", "snapshot"]]

def _parse_ts(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # assume ns if huge
        ns = int(val)
        if ns > 10_000_000_000_000:
            return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return datetime.fromtimestamp(float(val), tz=timezone.utc)
    try:
        if isinstance(val, str) and val.endswith("Z"):
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        return datetime.fromisoformat(str(val))
    except Exception:
        return None

def _coerce_float(x: Any) -> Optional[float]:
    if x is None: return None
    try: return float(x)
    except Exception:
        try: return float(Decimal(str(x)))
        except Exception: return None

def _coerce_int(x: Any) -> Optional[int]:
    try: return int(x) if x is not None else None
    except Exception: return None

@dataclass(frozen=True)
class Bar:
    provider: str
    symbol: str
    timestamp: datetime
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    volume: Optional[float]
    exchange: Optional[str] = None
    bar_id: Optional[int] = None
    res: Optional[str] = None
    closed: Optional[bool] = None

    @property
    def row_type(self) -> RowType:
        return "bar"

    @staticmethod
    def from_row(provider: str, row: Dict[str, Any]) -> Bar:
        return Bar(
            provider=provider,
            symbol=str(row.get("Symbol")),
            timestamp=_parse_ts(row.get("Timestamp")) or datetime.now(timezone.utc),
            open=_coerce_float(row.get("Open")),
            high=_coerce_float(row.get("High")),
            low=_coerce_float(row.get("Low")),
            close=_coerce_float(row.get("Close")),
            volume=_coerce_float(row.get("Volume")),
            exchange=row.get("Exchange"),
            bar_id=_coerce_int(row.get("BarId")),
            res=row.get("Res"),
            closed=bool(row.get("Closed")) if row.get("Closed") is not None else None,
        )

@dataclass(frozen=True)
class Trade:
    provider: str
    symbol: str
    timestamp: datetime
    price: Optional[float]
    quantity: Optional[float]
    trade_id: Optional[Union[int, str]] = None
    exchange: Optional[str] = None
    is_buyer_maker: Optional[bool] = None
    buyer_id: Optional[str] = None
    seller_id: Optional[str] = None

    @property
    def row_type(self) -> RowType:
        return "trade"

    @staticmethod
    def from_row(provider: str, row: Dict[str, Any]) -> Trade:
        return Trade(
            provider=provider,
            symbol=str(row.get("Symbol")),
            timestamp=_parse_ts(row.get("Timestamp") or row.get("Ts")) or datetime.now(timezone.utc),
            price=_coerce_float(row.get("Price")),
            quantity=_coerce_float(row.get("Quantity")),
            trade_id=row.get("TradeID"),
            exchange=row.get("Exchange"),
            is_buyer_maker=bool(row.get("IsBuyerMaker")) if row.get("IsBuyerMaker") is not None else None,
            buyer_id=row.get("BuyerID"),
            seller_id=row.get("SellerID"),
        )

T = TypeVar("T", Bar, Trade)

@dataclass(frozen=True)
class Envelope(Generic[T]):
    version: int
    phase: Phase
    part: Part
    provider: str
    schema: str
    symbol: str
    rows: Sequence[T]
    row_type: RowType
    seq: Optional[int] = None
    watermark_ns: Optional[int] = None
    raw_meta: Dict[str, Any] = None

    @property
    def is_snapshot(self) -> bool: return self.phase == "snapshot"
    @property
    def is_live(self) -> bool: return self.phase == "live"
    @property
    def is_boundary(self) -> bool: return self.phase == "snapshot_boundary"
    @property
    def is_bar(self) -> bool: return self.row_type == "bar"
    @property
    def is_trade(self) -> bool: return self.row_type == "trade"

    def rows_dicts(self) -> list[dict]:
        # Convert typed Bar/Trade rows to dicts for quick & dirty printing
        out = []
        for r in self.rows:
            if hasattr(r, "__dict__"):
                d = dict(r.__dict__)
                if "timestamp" in d and hasattr(d["timestamp"], "isoformat"):
                    d["timestamp"] = d["timestamp"].isoformat()
                out.append(d)
            elif isinstance(r, dict):
                out.append(r)
        return out


def adapt_envelope(env: Dict[str, Any]) -> Envelope[Union[Bar, Trade]]:
    server_ver = int(env.get("version", 1))
    ensure_compat(server_ver)  # warn/raise according to policy

    provider = env.get("provider")
    schema   = env.get("data_schema")
    symbol   = env.get("symbol")
    rows     = env.get("rows") or []
    part     = env.get("part")
    meta     = env.get("meta") or {}

    # Prefer server-provided row_type; otherwise infer from schema
    rt: RowType = env.get("row_type") or ("bar" if (schema or "").startswith("ohlcv_") else "trade")
    if rt == "bar":
        rows_parsed = [Bar.from_row(provider, r) for r in rows]
    else:
        rows_parsed = [Trade.from_row(provider, r) for r in rows]

    return Envelope(
        version=server_ver,
        phase=env.get("phase") or "live",
        part=part,
        provider=provider,
        schema=schema,
        symbol=symbol,
        rows=tuple(rows_parsed),
        row_type=rt,
        seq=_coerce_int(meta.get("seq")),
        watermark_ns=_coerce_int(env.get("watermark_ns")),
        raw_meta=meta,
    )
