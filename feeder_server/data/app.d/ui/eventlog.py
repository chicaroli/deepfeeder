"""Event log UI helpers.

Contains reusable formatting and a helper to wrap a table into a formatted
ui.table with standard level coloring.
"""
from __future__ import annotations
from deephaven import ui

__all__ = ["eventlog_table", "eventlog_formats"]


def eventlog_formats():
    return [
    # Keep strong highlight for errors
    ui.TableFormat(cols="level", if_='level == "ERROR"', background_color="negative", color="white"),
    # Use only font color (no background) for warnings & info to reduce visual noise
    ui.TableFormat(cols="level", if_='level == "WARN"', color="#f5f061"),  # lighter yellow text
    ui.TableFormat(cols="level", if_='level == "INFO"', color="#66bb6a"),  # lighter green text
    ]


def eventlog_table(tbl):  # type: ignore[no-untyped-def]
    """Return a formatted event log table (best effort on formatting)."""
    try:
        return ui.table(tbl, format_=eventlog_formats())
    except Exception:  # noqa: BLE001
        return ui.table(tbl)
