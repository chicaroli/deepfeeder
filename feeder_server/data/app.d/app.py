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
from runtime.feed_manager import FeedManager
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
    bus         = dfb.get_service("event_bus")
    event_store = dfb.get_service("event_store")
    journal     = dfb.get_service("journal_store")
    dh_sink     = dfb.get_service("dh_sink")

    # register orchestrator service for UI access
    fm = FeedManager(bus=bus, event_store=event_store, journal=journal, dh_sink=dh_sink)
    # register feed manager services (new + legacy alias)
    try:
        dfb.services.register("feed_manager", fm)
        _log("FeedManager service registered (dfb.services.get('feed_manager'))", name="UI_REGISTER")
    except Exception as e:
        _log(f"FeedManager service registration skipped (error): {e!r}", name="UI_REGISTER", level="ERROR")

    fm.start_consumers()  # ensure DH / Journal consumers are running
    _log("FeedManager consumers started", name="CORE")


    specs_path = os.getenv("DEEPFEEDER_FEEDERS_JSON", "/data/storage/notebooks/feeders.json")
    specs = load_feeder_specs(specs_path)
    # expose specs path (optional) for UI reload logic
    try:
        dfb.services.register("feeder_specs_path", specs_path)
    except Exception:
        pass

    # Register producers per spec
    for spec in specs:
        if spec.provider == "binance":
            fm.register(make_binance_ws(spec, bus))
        elif spec.provider == "tradingview":
            pass
            # orch.register(make_tradingview_ws(spec, bus))
            # orch.register(make_tradingview_backfill(spec, bus))  # final bars

    _log("core runners started (DH & Journal consumers)", name="CORE")

    # Autostart according to your file
    if autostart_env not in ("0", "false", "False"):
        try:
            _log("AUTOSTART enabled: starting configured feeders...", name="AUTOSTART")
            # fm.start_autostart(specs)
            _log("AUTOSTART completed", name="AUTOSTART")
        except Exception as exc:  # noqa: BLE001 broad so app still loads
            _log(f"AUTOSTART failed: {exc!r}", name="AUTOSTART", level="ERROR")
    else:
        _log("AUTOSTART disabled by environment", name="AUTOSTART")

except Exception as exc:
    _log(f"failed to start core runners: {exc!r}", name="CORE", level="ERROR")
