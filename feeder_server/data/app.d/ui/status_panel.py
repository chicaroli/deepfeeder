# ui/status_panel.py

"""Status panel for DeepFeeder UI.

Provides a function `status_panel` that returns the live feeder status table
with simple row formatting. Restart count derivation was removed for
simplicity and reliability.
"""
from __future__ import annotations
from deephaven import ui
import deepfeeder as dfb

__all__ = ["status_panel"]


def _status_table_formats():
    return [
        ui.TableFormat(cols="alive", if_="alive", background_color="positive", color="white"),
        ui.TableFormat(cols="alive", if_="!alive", background_color="negative", color="white"),
    ]


def status_panel():  # type: ignore[no-untyped-def]
    """Build and return the feeders status panel (no restart counts)."""
    table = dfb.get_status_table()
    return ui.panel(ui.table(table, format_=_status_table_formats()), title="Feeders status (live)")
