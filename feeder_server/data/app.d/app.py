# app.py
"""Deephaven app-mode entrypoint for deepfeeder.

This file imports the explicit binding packages that provide a minimal,
import-safe API for app-mode. Heavy side effects (starting _feeders) are
only performed via explicit function calls.
"""

import os
import time
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
from runtime.warmup import hydrate_dh_from_journal
from ingest.feed_manager import FeedManager
from ingest.feeder_specs import load_feeder_specs
from ingest.factories import create_producers, ensure_runtime_services

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

    # register Manager service for UI access --------------------------------------------
    try:
        fm = FeedManager(bus=bus, event_store=event_store, journal=journal, dh_sink=dh_sink)
        dfb.services.register("feed_manager", fm)
        _log(f"FeedManager service registered: {fm}", name="UI_REGISTER")
    except Exception as e:
        fm = None
        _log(f"FeedManager service registration failed: {e!r}", name="UI_REGISTER", level="ERROR")


    # Warm replay into DH from Journal (if enabled and possible) ------------------------
    WINDOW_DAYS = 2
    since = time.time_ns() - WINDOW_DAYS * 24 * 60 * 60 * 1_000_000_000
    rows, last_ts = hydrate_dh_from_journal(
        journal=journal,
        dh_sink=dh_sink,
        since_ts_ns=since,
        provider_filter=None,  # or {"binance","tradingview"}
        stream_filter=None,  # or {"trades","quotes","bars"}
        page_rows=20_000,
    )
    _log(f"Warmup wrote {rows} rows from Journal (since_ts_ns={since})", name="WARMUP")

    # After warmup, set DH replay cursor so it does NOT replay the WAL.
    # Use the durable event_store's last_committed batch id (not the timestamp returned by warmup).
    try:
        last_committed = event_store.last_committed()
        journal.set_watermark(int(last_committed), "dh_consumer:cursor")
    except Exception:
        # best-effort: if we cannot read event_store, fall back to previous behavior
        try:
            if last_ts and last_ts > 0:
                journal.set_watermark(last_ts, "dh_consumer:cursor")
        except Exception:
            pass

    # Set the bus’ DH cursor from the watermark (fallback to committed tail) ------------
    dh_start = journal.get_watermark("dh_consumer:cursor")
    if dh_start is None:
        dh_start = event_store.last_committed()
    bus.set_dh_cursor(int(dh_start))

    # Start consumers (DH + Journal) ----------------------------------------------------
    try:
        fm.start_consumers()
        _log("FeedManager consumers started", name="CORE")
    except Exception as e:
        _log(f"FeedManager consumers failed to start: {e!r}", name="CORE", level="ERROR")


    # Load feeder specs -----------------------------------------------------------------
    specs_path = os.getenv("DEEPFEEDER_FEEDERS_JSON", "/data/storage/notebooks/feeders.json")
    try:
        specs = load_feeder_specs(specs_path)
        dfb.services.register("feeder_specs_path", specs_path)
    except Exception as e:
        _log(f"Failed to load feeder specs from {specs_path}: {e!r}", name="FEEDERS", level="ERROR")
        specs = []


    # Ensure runtime services -----------------------------------------------------------
    ensure_runtime_services(specs, dfb.services, event_store=event_store, journal=journal, start_monitors=True)


    # Register producers ----------------------------------------------------------------
    registered = []
    for spec in specs:
        for p in create_producers(spec, bus, services=dfb.services):
            fm.register(p)
            registered.append((spec, p))
    _log(f"Producers: {fm.list_producers()}", name="FEEDERS")


    # Autostart according to your file --------------------------------------------------
    if autostart_env not in ("0", "false", "False"):
        try:
            _log("AUTOSTART enabled: starting configured _feeders...", name="AUTOSTART")
            fm.start_autostart(specs)
            _log("AUTOSTART completed", name="AUTOSTART")
        except Exception as exc:  # noqa: BLE001 broad so app still loads
            _log(f"AUTOSTART failed: {exc!r}", name="AUTOSTART", level="ERROR")
    else:
        _log("AUTOSTART disabled by environment", name="AUTOSTART")

except Exception as exc:
    _log(f"failed to start core runners: {exc!r}", name="CORE", level="ERROR")
