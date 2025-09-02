"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting feeders) are
only performed via explicit function calls.
"""

import os
from datetime import datetime, timezone


# --- logging helpers -------------------------------------------------------
def _log(msg: str, *, name: str = "APP", level: str = "INFO", code: str = "APP_START") -> None:
    """Print a console line and mirror it into the unified event log.

    Parameters
    ----------
    msg: Human readable message.
    name: Event name categorizing the log (e.g. APP, AUTOSTART, UI_REGISTER).
    level: Severity level (INFO/ERROR/WARN, etc.).
    code: Optional code for structured logging (default 0).
    """
    # Also print so users can see startup/registration feedback in console
    _ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[deepfeeder {_ts}] {msg}")
    try:  # best-effort: never block startup if event logging fails
        emit_event(service="app", name=name, role="startup", level=level, code=code, message=msg)
    except Exception:
        pass

# --- startup environment summary -------------------------------------------
autostart_env = os.getenv("DEEPFEEDER_AUTOSTART", "1")
register_ui_env = os.getenv("DEEPFEEDER_REGISTER_UI", "1")

_log("package imported: use 'import deepfeeder as dfb'", name="IMPORT")
_log(f"env DEEPFEEDER_AUTOSTART={autostart_env!r} DEEPFEEDER_REGISTER_UI={register_ui_env!r}", name="ENV")


import deepfeeder as dfb
import ui.dashboard
from runtime.eventlog import emit_event
from runtime.orchestrator import Orchestrator
from runtime.feeder_specs import load_feeder_specs
from ingest.factories import make_binance_ws #, make_tradingview_ws, make_tradingview_backfill


# --- optional UI registration ----
if register_ui_env not in ("0", "false", "False"):
    try:
        FeederDashboard = ui.dashboard.FeederDashboard          # noqa: N816 (framework style)
        _log("UI dashboard registered (ui.dashboard.FeederDashboard)", name="UI_REGISTER")
    except Exception as exc:  # noqa: BLE001
        _log(f"UI dashboard registration skipped (error): {exc!r}", name="UI_REGISTER", level="ERROR")
else:
    _log("UI dashboard registration disabled by environment", name="UI_REGISTER")


# --- autostart orchestrator and consumers ---
try:
    from config.paths import PATHS
    from storage.eventstore_duckdb import DuckDbEventStore
    from storage.journal_duckdb import DuckDbJournal
    from core.event_bus import EventBus
    from sinks.registry import WriterRegistry
    from sinks.dh_sink import DhSinkDynamic
    import providers

    # service_container = dfb.get_services_container()
    # service_container.register("event_store",  lambda: DuckDbEventStore(str(PATHS.event_store_db)))
    # service_container.register("journal_store", lambda: DuckDbJournal(str(PATHS.journal_db)))
    # _es = service_container.get("event_store")
    # service_container.register("event_bus", lambda es=_es: EventBus(es, max_envelopes=100_000))

    # Provide a factory that returns the dict bound to your real DH writers.
    # def _make_dh_registry() -> WriterRegistry:
    #     reg = WriterRegistry()
    #     # Wrap your existing DynamicTableWriters here:
    #
    #     # TradingView:
    #     # reg.add(
    #     #     provider="tradingview",
    #     #     stream="quotes",
    #     #     writer=feeders.tradingview.schema.tv_quotes_writer(),
    #     #     flatten=providers.tradingview.adapter.flatten_quotes
    #     #     )
    #     # reg.add(
    #     #     provider="tradingview",
    #     #     stream="bars",
    #     #     writer=feeders.tradingview.schema.tv_bars_writer(),
    #     #     flatten=providers.tradingview.adapter.flatten_ohlcv_1m
    #     #     )
    #
    #     # Binance:
    #     reg.add(
    #         provider="binance",
    #         stream="trades",
    #         writer=providers.binance.schema.binance_trades_writer(),
    #         flatten=providers.binance.adapter.flatten_trades
    #     )
    #
    #     return reg
    # service_container.register("dh_sink", lambda: DhSinkDynamic(_make_dh_registry()))

    bus         = dfb.get_service("event_bus")
    event_store = dfb.get_service("event_store")
    journal     = dfb.get_service("journal_store")
    dh_sink     = dfb.get_service("dh_sink")

    orch = Orchestrator(bus=bus, event_store=event_store, journal=journal, dh_sink=dh_sink)
    # orch.start_consumers()

    specs = load_feeder_specs(os.getenv("DEEPFEEDER_FEEDERS_JSON", "/data/storage/notebooks/feeders.json"))

    # Register producers per spec
    for spec in specs:
        if spec.provider == "binance":
            # orch.register(make_binance_ws(spec, bus))
            # optional: orch.register(make_binance_backfill(spec, bus))
            pass
        elif spec.provider == "tradingview":
            pass
            # orch.register(make_tradingview_ws(spec, bus))
            # orch.register(make_tradingview_backfill(spec, bus))  # final bars

    _log("core runners started (DH & Journal consumers)", name="CORE")

    # Autostart according to your file
    if autostart_env not in ("0", "false", "False"):
        try:
            _log("AUTOSTART enabled: starting configured feeders...", name="AUTOSTART")
            # orch.run_autostart(specs)
            _log("AUTOSTART completed", name="AUTOSTART")
        except Exception as exc:  # noqa: BLE001 broad so app still loads
            _log(f"AUTOSTART failed: {exc!r}", name="AUTOSTART", level="ERROR")
    else:
        _log("AUTOSTART disabled by environment", name="AUTOSTART")

except Exception as exc:
    _log(f"failed to start core runners: {exc!r}", name="CORE", level="ERROR")
