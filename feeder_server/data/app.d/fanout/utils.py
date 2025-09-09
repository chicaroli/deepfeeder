# fanout/utils.py
"""
_utils.py
Utility functions for MarketFeeder.
"""
import pandas as pd
from typing import Optional, Iterable

def _iso(ts) -> str:
    """Return ISO format UTC timestamp."""
    return pd.Timestamp(ts, tz="UTC").isoformat().replace("+00:00","Z")

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
    """Column-level filter for Arrow tables inside a listener batch.

    Previous behavior (now replaced) removed the *table* keys (added / updated / completed)
    unless they were explicitly listed in ``fields``. Users typically pass *column* names
    like ["Timestamp","Close"], which unintentionally stripped all data tables leaving only
    ``meta``. This new behavior keeps the structural keys and, when ``fields`` is provided,
    subsets the columns of each non-None pyarrow.Table to those requested (case-insensitive).

    Rules:
      - If ``fields`` falsy: return message unchanged.
      - Always preserve keys: added, updated, completed, meta (and any future structural keys).
      - For each table, select only columns whose lowercase name is in ``fields`` (lowercased).
        If no requested columns exist in that table, the table is set to ``None`` (saves bandwidth).
      - ``meta`` untouched; callers can still pick columns inside tables client-side if they
        omitted filtering here.
    """
    if not fields:
        return msg
    cols_req = {f.lower() for f in fields}
    out = dict(msg)  # shallow copy; Arrow tables are immutable so safe
    for key in ("added", "updated", "completed"):
        tbl = msg.get(key)
        if tbl is None:
            continue
        try:
            names = list(tbl.schema.names)
            keep_cols = [n for n in names if n.lower() in cols_req]
            if keep_cols:
                # pyarrow.Table.select returns a new table with chosen columns
                out[key] = tbl.select(keep_cols)
            else:
                out[key] = None  # nothing requested from this table
        except Exception:
            # On any unexpected failure, leave original table to avoid data loss
            out[key] = tbl
    return out

