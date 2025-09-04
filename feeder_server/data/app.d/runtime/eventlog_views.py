# runtime/eventlog_views.py
"""Convenience derived views over the raw event log.

These helpers return Deephaven table expressions built off the append-only
eventlog table. They intentionally apply only lightweight filtering so that
callers can further refine (e.g. additional where(), update(), last_by()).
"""
from __future__ import annotations
from deephaven import Table  # type: ignore
from runtime.eventlog_bus import get_eventlog_table

def recent_errors(minutes: int = 10) -> Table:
    """Return ERROR-level events in the last `minutes` minutes."""
    tbl = get_eventlog_table()
    return tbl.where(f"level == 'ERROR' && ts >= now()-MINUTE*{int(minutes)}")

def latest_error_per_instance() -> Table:
    """Return the latest ERROR event per (service,name)."""
    tbl = get_eventlog_table()
    return tbl.where("level == 'ERROR'").last_by(["service","name"])

__all__ = [
    "recent_errors",
    "latest_error_per_instance",
]
