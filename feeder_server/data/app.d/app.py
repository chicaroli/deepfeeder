"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting feeders) are
only performed via explicit function calls.
"""

import os
from datetime import datetime, timezone


# --- logging helpers -------------------------------------------------------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

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
    print(f"[deepfeeder {_ts()}] {msg}")
    try:  # best-effort: never block startup if event logging fails
        emit_event(service="app", name=name, role="startup", level=level, code=code, message=msg)
    except Exception:  # noqa: BLE001
        pass

# --- startup environment summary -------------------------------------------
dev_mode = os.getenv("DEEPFEEDER_DEV_MODE", "1") not in ("0", "false", "False")
autostart_env = os.getenv("DEEPFEEDER_AUTOSTART", "1")
register_ui_env = os.getenv("DEEPFEEDER_REGISTER_UI", "1")

_log("package imported: use 'import deepfeeder as dfb'", name="IMPORT")
_log(f"env DEEPFEEDER_AUTOSTART={autostart_env!r} DEEPFEEDER_REGISTER_UI={register_ui_env!r}", name="ENV")

   
# -------------------------------------------------------------------------------
# --- PRODUCTION MODE -----------------------------------------------------------
# -------------------------------------------------------------------------------
if not dev_mode:
    import deepfeeder as dfb
    import ui.dashboard 
    from runtime.eventlog import emit_event
    from runtime.orchestrator import Orchestrator
    from runtime.feeder_specs import load_feeder_specs
    from ingest.factories import make_binance_ws, make_tradingview_ws, make_tradingview_backfill
    
    # --- optional UI registration ----
    if register_ui_env not in ("0", "false", "False"):
        try:
            FeederDashboard = ui.dashboard.FeederDashboard  # noqa: N816 (framework style)
            _log("UI dashboard registered (ui.dashboard.FeederDashboard)", name="UI_REGISTER")
        except Exception as exc:  # noqa: BLE001
            _log(f"UI dashboard registration skipped (error): {exc!r}", name="UI_REGISTER", level="ERROR")
    else:
        _log("UI dashboard registration disabled by environment", name="UI_REGISTER")


    # --- autostart orchestrator and consumers ---
    try:
        bus      = dfb.get_service("event_bus")
        outbox   = dfb.get_service("outbox")
        journal  = dfb.get_service("journal_store")
        dh_sink  = dfb.get_service("dh_sink")

        orch = Orchestrator(bus=bus, outbox=outbox, journal=journal, dh_sink=dh_sink)
        orch.start_consumers()

        specs = load_feeder_specs(os.getenv("DEEPFEEDER_FEEDERS_JSON", "app.d/feeders.json"))

        # Register producers per spec
        for spec in specs:
            if spec.provider == "binance":
                orch.register(make_binance_ws(spec, bus))
                # optional: orch.register(make_binance_backfill(spec, bus))
            elif spec.provider == "tradingview":
                orch.register(make_tradingview_ws(spec, bus))
                orch.register(make_tradingview_backfill(spec, bus))  # final bars

        _log("core runners started (DH & Journal consumers)", name="CORE")

        # Autostart according to your file
        if autostart_env not in ("0", "false", "False"):
            try:
                _log("AUTOSTART enabled: starting configured feeders...", name="AUTOSTART")
                orch.start_autostart(specs)
                _log("AUTOSTART completed", name="AUTOSTART")
            except Exception as exc:  # noqa: BLE001 broad so app still loads
                _log(f"AUTOSTART failed: {exc!r}", name="AUTOSTART", level="ERROR")
        else:
            _log("AUTOSTART disabled by environment", name="AUTOSTART")

    except Exception as exc:
        _log(f"failed to start core runners: {exc!r}", name="CORE", level="ERROR")

else:
    # -------------------------------------------------------------------------------
    # --- DEV MODE ------------------------------------------------------------------
    # -------------------------------------------------------------------------------
    from sinks.registry import WriterRegistry
    from sinks.dh_sink import DhSinkDynamic
    from providers.binance.flattener import flatten_trades as binance_flatten_trades
    from deephaven import dtypes as dht
    from deephaven import DynamicTableWriter
    
    _DEV_TABLES = {}  # expose in tables() so you can view it

    def _make_dev_binance_trades_writer():
        # Minimal schema for binance trades
        cols = {
            "Provider": dht.string, "Stream": dht.string, "Symbol": dht.string,
            "TsNanos": dht.long, "Seq": dht.long, "IsFinal": dht.bool_,
            "Price": dht.double, "Qty": dht.double, "Side": dht.string,
        }
        w = DynamicTableWriter(cols)
        _DEV_TABLES["dev_binance_trades"] = w.table
        return w

    def _make_dh_registry() -> WriterRegistry:
        reg = WriterRegistry()

        # Dev-only: register one writer for ("binance","trades")
        reg.add(
            provider="binance", stream="trades",
            writer=_make_dev_binance_trades_writer(),
            flatten=binance_flatten_trades
        )
        return reg

    dfb.services.register("dh_sink", lambda: DhSinkDynamic(_make_dh_registry()))