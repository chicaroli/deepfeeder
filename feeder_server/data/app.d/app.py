"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting feeders) are
only performed via explicit function calls.
"""

import os
from datetime import datetime, timezone
import deepfeeder as dfb

# --- logging helpers -------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _log(msg: str) -> None:
    print(f"[deepfeeder {_ts()}] {msg}")

# --- startup environment summary -------------------------------------------

autostart_env = os.getenv("DEEPFEEDER_AUTOSTART", "1")
register_ui_env = os.getenv("DEEPFEEDER_REGISTER_UI", "1")

_log("package imported: use 'import deepfeeder as dfb'")
_log(f"env DEEPFEEDER_AUTOSTART={autostart_env!r} DEEPFEEDER_REGISTER_UI={register_ui_env!r}")

# --- optional autostart ----------------------------------------------------

if autostart_env not in ("0", "false", "False"):
    try:
        _log("autostart enabled: starting configured feeders...")
        result = dfb.feeder_manager.start_all_autostart()
        # Provide concise summary if possible
        if isinstance(result, dict):
            started = result.get("started") or result.get("success") or result
            _log(f"autostart result: {started}")
        else:
            _log(f"autostart result: {result}")
    except Exception as exc:  # noqa: BLE001 broad so app still loads
        _log(f"autostart failed: {exc!r}")
else:
    _log("autostart disabled by environment")

# --- optional UI registration ----------------------------------------------

if register_ui_env not in ("0", "false", "False"):
    try:
        import ui.dashboard  # type: ignore  # side-effect import: registers dashboard
        FeederDashboard = ui.dashboard.FeederDashboard  # noqa: N816 (framework style)
        _log("UI dashboard registered (ui.dashboard.FeederDashboard)")
    except Exception as exc:  # noqa: BLE001
        _log(f"UI dashboard registration skipped (error): {exc!r}")
else:
    _log("UI dashboard registration disabled by environment")
