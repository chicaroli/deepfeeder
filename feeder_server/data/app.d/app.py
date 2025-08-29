"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting feeders) are
only performed via explicit function calls.
"""

import os
from datetime import datetime, timezone
import deepfeeder as dfb
from persistence import ensure_dirs  # type: ignore
from runtime.eventlog import emit_event  # type: ignore

from runtime.heartbeat import Heartbeater
from runtime.dh_thread import spawn_dh_thread
from runtime.consumers import dh_consumer_loop, journal_consumer_loop


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

autostart_env = os.getenv("DEEPFEEDER_AUTOSTART", "1")
register_ui_env = os.getenv("DEEPFEEDER_REGISTER_UI", "1")

_log("package imported: use 'import deepfeeder as dfb'", name="IMPORT")
_log(f"env DEEPFEEDER_AUTOSTART={autostart_env!r} DEEPFEEDER_REGISTER_UI={register_ui_env!r}", name="ENV")

# --- persistence autostart (before feeders) --------------------------------

ensure_dirs()
journal_autostart_env = os.getenv("DEEPFEEDER_JOURNAL_AUTOSTART", "1")
if journal_autostart_env not in ("0", "false", "False"):
    try:
        msg = dfb.start_journal()
        _log(f"journal autostart: {msg}", name="PERSIST")
    except Exception as exc:  # noqa: BLE001
        _log(f"journal autostart failed: {exc!r}", name="PERSIST", level="ERROR")
else:
    _log("journal autostart disabled by environment", name="PERSIST")

# --- optional autostart ----------------------------------------------------

if autostart_env not in ("0", "false", "False"):
    try:
        _log("autostart enabled: starting configured feeders...", name="AUTOSTART")
        result = dfb.feeder_manager.start_all_autostart()
        _log(f"autostart result: {result}", name="AUTOSTART")
    except Exception as exc:  # noqa: BLE001 broad so app still loads
        _log(f"autostart failed: {exc!r}", name="AUTOSTART", level="ERROR")
else:
    _log("autostart disabled by environment", name="AUTOSTART")

# --- optional UI registration ----------------------------------------------

if register_ui_env not in ("0", "false", "False"):
    try:
        import ui.dashboard  # type: ignore  # side-effect import: registers dashboard
        FeederDashboard = ui.dashboard.FeederDashboard  # noqa: N816 (framework style)
        _log("UI dashboard registered (ui.dashboard.FeederDashboard)", name="UI_REGISTER")
    except Exception as exc:  # noqa: BLE001
        _log(f"UI dashboard registration skipped (error): {exc!r}", name="UI_REGISTER", level="ERROR")
        try:
            from deephaven import ui as _ui  # type: ignore
            FeederDashboard = _ui.dashboard(  # type: ignore
                _ui.panel(_ui.text(f"DeepFeeder dashboard failed to load: {exc!r}"), title="DeepFeeder Error")
            )
        except Exception:
            pass
else:
    _log("UI dashboard registration disabled by environment", name="UI_REGISTER")


try:
    bus = dfb.get_service("event_bus")
    outbox = dfb.get_service("outbox")
    journal_store = dfb.get_service("journal_store")
    dh_sink = dfb.get_service("dh_sink")

    hb_dh  = Heartbeater("feeder", "core", "dh_consumer")
    hb_jrn = Heartbeater("feeder", "core", "journal_consumer")

    spawn_dh_thread("feeder","core","dh_consumer", dh_consumer_loop, bus, dh_sink, hb_dh)
    spawn_dh_thread("feeder","core","journal_consumer", journal_consumer_loop, outbox, journal_store, bus, hb_jrn)

    _log("core runners started (DH & Journal consumers)", name="CORE")
except Exception as exc:
    _log(f"failed to start core runners: {exc!r}", name="CORE", level="ERROR")