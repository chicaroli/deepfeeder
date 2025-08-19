"""
utils.py
Utility functions for MarketFeeder.
"""
import pandas as pd
from typing import Dict, Optional, Iterable

def _iso(ts) -> str:
    """Return ISO format UTC timestamp."""
    return pd.Timestamp(ts, tz="UTC").isoformat().replace("+00:00","Z")

def _today_expr(spec, symbol_expr: str) -> str:
    """Build Deephaven expression for today's data for a symbol."""
    return (
        f"{symbol_expr} && "
        f"{spec.time_col} >= lowerBin(now(), 'DAY') && "
        f"{spec.time_col} <= lowerBin(now(), 'MINUTE')"
    )

def _symbol_filter_expr(spec, symbol) -> str:
    col = spec.symbol_col
    if isinstance(symbol, str):
        return f"{col}==`{symbol}`"
    syms = list(symbol)
    if not syms:
        return "false"
    ors = " || ".join(f"{col}==`{s}`" for s in syms)
    return f"({ors})"

def _filter_fields(msg: dict, fields: Optional[Iterable[str]]) -> dict:
    if not fields:
        return msg
    keep = set(fields) | {"version", "type", "symbol", "timestamp"}
    return {k: v for k, v in msg.items() if k in keep}

def _rename_snapshot_cols(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    out = df.rename(columns=mapping).copy()
    if "Timestamp" in out.columns:
        out["Timestamp"] = pd.to_datetime(out["Timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "Symbol" in out.columns and out["Symbol"].dtype == object:
        out["Symbol"] = out["Symbol"].str.lower()
    return out

