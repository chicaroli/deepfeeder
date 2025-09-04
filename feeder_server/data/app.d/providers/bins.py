# providers/bins.py
"""Recent time bin utilities for _feeders.

This module now only exposes ``bins_recent`` – a minimal rolling window of period-
aligned timestamps (current bin plus a fixed number of previous bins). The former
full-day ``bins_today`` helper was removed after shifting bar completion logic to
depend only on the most recent bins, reducing overhead.

Example:
    bins_recent(1)         # current + previous 1-minute bin
    bins_recent(5, 2)      # current + previous 5-minute bin
    bins_recent(5, 12)     # roughly last hour of 5-minute bins (if ever needed)
"""

from __future__ import annotations
from deephaven import time_table, empty_table
from deephaven.table_factory import merge
import datetime as _dt
from typing import Optional, Dict, Any

# Cache keyed by (period_minutes) for current UTC date
_BINS_CACHE: Dict[int, Any] = {}  # legacy (kept for backward compat purge window)
_BINS_CACHE_DAY: Optional[_dt.date] = None  # legacy

# Cache for recent rolling windows keyed by (period_minutes, bars_back)
_RECENT_BINS_CACHE: Dict[tuple[int, int], Any] = {}
_RECENT_BINS_CACHE_DAY: Optional[_dt.date] = None


def bins_recent(period_minutes: int, bars_back: int = 2):
    """Return a lightweight rolling window of recent period-aligned bins.

    Parameters
    ----------
    period_minutes : int
        Bar / bin period in minutes (>0).
    bars_back : int, default 2
        Number of *consecutive* bins to expose ending at the *current* bin alignment.
        ``bars_back=1`` yields only the current (potentially still forming) bin;
        ``bars_back=2`` adds the immediately prior completed bin; etc.

    Notes
    -----
    * This table intentionally does NOT enumerate the whole day – it's minimal.
    * Useful for logic that only needs the latest bar(s) to mark completion or drive
      UI updates without the cost of a full gap-filled history.
    * A simple date rollover reset is implemented (cache invalidated at UTC midnight).
    * For multi-hour periods, set ``bars_back`` explicitly (e.g. a 2h bar wanting two
      bars => ``bars_back=2``) – an hour-based lookback would be ambiguous.
    * Implementation keeps earlier bins static; they are rebuilt after UTC day change.
    """
    global _RECENT_BINS_CACHE, _RECENT_BINS_CACHE_DAY
    if period_minutes <= 0:
        raise ValueError("period_minutes must be positive")
    if bars_back <= 0:
        raise ValueError("bars_back must be positive")

    today = _dt.datetime.now(_dt.timezone.utc).date()
    if _RECENT_BINS_CACHE_DAY != today:
        _RECENT_BINS_CACHE.clear()
        _RECENT_BINS_CACHE_DAY = today

    key = (period_minutes, bars_back)
    cached = _RECENT_BINS_CACHE.get(key)
    if cached is not None:
        return cached

    period_expr = f"{period_minutes} * MINUTE"

    # Static slice covering the requested lookback ending at the current aligned bin.
    # (bars_back - 1) previous bins + current.
    static = empty_table(bars_back).update([
        # Aligned 'now'
        f"AlignedNow = lowerBin(now(), {period_expr})",
        # Oldest timestamp = AlignedNow - (bars_back-1)*period
        f"Timestamp = AlignedNow - ({bars_back - 1}) * {period_expr} + i * {period_expr}",
    ]).view(["Timestamp"])  # drop helper column

    # Forward bins: start from current time (aligned via lowerBin) and progress; we add
    # them so that when the clock advances a *new* current bin appears automatically.
    forward = time_table(f"PT{period_minutes}M").update_view([
        f"Timestamp = lowerBin(Timestamp, {period_expr})",
    ]).view(["Timestamp"]).select_distinct(["Timestamp"])

    recent = merge([static, forward]).select_distinct(["Timestamp"])
    _RECENT_BINS_CACHE[key] = recent
    return recent

__all__ = ["bins_recent"]
