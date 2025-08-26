"""Dashboard composition module.

This orchestrates the modular UI pieces (controls, status panel, event log
tabs) into a single exported `FeederDashboard` object. The previous
monolithic implementation has been split across:

* controls.py   - feeder lifecycle toolbar
* status_panel.py - live status table with restart counts
* eventlog.py   - event log formatting helpers

Backwards compatibility: importing `ui.dashboard` still provides
`FeederDashboard` and existing component names are re-exported for
convenience.
"""
from deephaven import ui
import deepfeeder as dfb
from .controls import feeder_controls
from .status_panel import status_panel
from .eventlog import eventlog_table

__all__ = ["FeederDashboard", "feeder_controls", "status_panel", "eventlog_table"]


def _eventlog_tabs():  # type: ignore[no-untyped-def]
    return ui.tabs(
        ui.tab(
            eventlog_table(
                dfb.get_eventlog_table().where("ts >= now() - HOUR").reverse(),
            ),
            title="Last 1h",
        ),
        ui.tab(
            eventlog_table(
                dfb.get_eventlog_table()
                .where('level == "ERROR" && ts >= now() - 10 * MINUTE')
                .reverse(),
            ),
            title="Errors 10m",
        ),
        ui.tab(
            eventlog_table(
                dfb.get_eventlog_table()
                .where('level == "ERROR"')
                .last_by(["service", "name"]),
            ),
            title="Latest Error/Instance",
        ),
    )


FeederDashboard = ui.dashboard(
    ui.column(
        ui.row(
            ui.stack(
                feeder_controls(),
                width=30,
            ),
            ui.stack(
                status_panel(),
                ui.panel(ui.table(dfb.get_configs_table()), title="Configs (live)"),
                ui.panel(ui.table(dfb.get_threads_table()), title="Threads / Services (live)"),
                active_item_index=0,
            ),
            height=25,
        ),
        ui.stack(
            ui.panel(ui.table(dfb.get_binance_trades_table()), title="Binance Trades"),
            ui.panel(ui.table(dfb.get_binance_ohlcv_1m_table()), title="Binance OHLCV 1m"),
            ui.panel(ui.table(dfb.get_tv_quotes_table()), title="TV Quotes (delayed)"),
            ui.panel(ui.table(dfb.get_tv_bars_table()), title="TV Bars (backfill)"),
            ui.panel(ui.table(dfb.get_tv_ohlcv_1m_table()), title="TV OHLCV 1m"),
            ui.panel(ui.table(dfb.get_tv_ohlcv_5m_table()), title="TV OHLCV 5m"),
            ui.panel(ui.table(dfb.get_tv_ohlcv_1m_table()), title="TV OHLCV 1m"),
            ui.panel(ui.table(dfb.get_tv_ohlcv_5m_table()), title="TV OHLCV 5m"),
            ui.panel(
                _eventlog_tabs(),
                title="Event Log",
            ),
            active_item_index=0,
            height=75,
        ),
    ),
)
