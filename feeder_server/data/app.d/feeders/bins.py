"""Common time bin utilities for feeders.

Provides shared bin tables (any minute period) for the current UTC day so all providers
can gap-fill bar series consistently without duplicating bin generation logic.

Primary API: bins_today(period_minutes: int)
Example: bins_today(1)  # 1-minute bins
         bins_today(5)  # 5-minute bins
"""

from __future__ import annotations
from deephaven import time_table, empty_table
from deephaven.table_factory import merge
import datetime as _dt
from typing import Optional, Dict, Any

# Cache keyed by (period_minutes) for current UTC date
_BINS_CACHE: Dict[int, Any] = {}
_BINS_CACHE_DAY: Optional[_dt.date] = None


def bins_today(period_minutes: int):
    """Return (and cache) a live table of bins for the given period (in minutes) for the current UTC day.

    The table contains a single column 'Timestamp' with one row per period bucket from
    the start of the current UTC day up to (and including) the current period.
    """
    global _BINS_CACHE, _BINS_CACHE_DAY
    if period_minutes <= 0:
        raise ValueError("period_minutes must be positive")
    today = _dt.datetime.utcnow().date()
    if _BINS_CACHE_DAY != today:
        _BINS_CACHE.clear()
        _BINS_CACHE_DAY = today
    cached = _BINS_CACHE.get(period_minutes)
    if cached is not None:
        return cached

    # Base minute sequence for the day (static past + live forward) only built once per day
    if 1 not in _BINS_CACHE:
        old_minutes = empty_table(24 * 60).update([
            "Timestamp = lowerBin(now(), DAY) + i * MINUTE"
        ]).where("Timestamp <= lowerBin(now(), MINUTE)").view(["Timestamp"])
        live_minutes = time_table("PT1M").where("Timestamp >= lowerBin(now(), DAY)").view(["Timestamp"])
        minutes = merge([old_minutes, live_minutes]).select_distinct(["Timestamp"])
        _BINS_CACHE[1] = minutes
    else:
        minutes = _BINS_CACHE[1]

    if period_minutes == 1:
        return minutes

    period_expr = f"{period_minutes} * MINUTE"
    bins = minutes.update_view([f"Timestamp = lowerBin(Timestamp, {period_expr})"]).select_distinct(["Timestamp"])
    _BINS_CACHE[period_minutes] = bins
    return bins

__all__ = ["bins_today"]
