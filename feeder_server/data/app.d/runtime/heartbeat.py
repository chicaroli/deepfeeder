"""Heartbeat helper & lightweight supervisor.

Adds a background supervisor thread that periodically refreshes all
registered (still running) heartbeats with a "running" state so that
`last_heartbeat` and computed `uptime_s` stay current in the threads table.
"""
from __future__ import annotations
import json
import time
import os
import threading
import weakref
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Set
from deephaven.time import to_j_instant
from .threads_bus import get_threads_writer
from .eventlog import emit_event

@dataclass
class BeatCtx:
    service: str
    name: str
    role: str = ""
    started_ts: float = 0.0

_HB_LOCK = threading.Lock()
_HB_REFS: Set[weakref.ReferenceType] = set()
_SUP_THREAD: threading.Thread | None = None
_SUP_INTERVAL = float(os.getenv("DEEPFEEDER_HEARTBEAT_INTERVAL", "5"))  # seconds

def _start_supervisor():
    global _SUP_THREAD
    if _SUP_THREAD and _SUP_THREAD.is_alive():
        return
    def _loop():  # pragma: no cover - timing thread
        while True:
            time.sleep(_SUP_INTERVAL)
            with _HB_LOCK:
                refs = list(_HB_REFS)
            for r in refs:
                hb = r()
                if hb is None or hb.is_terminal:
                    continue
                try:
                    hb.beat("running")
                except Exception:  # noqa: BLE001
                    pass
    _SUP_THREAD = threading.Thread(target=_loop, name="heartbeat-supervisor", daemon=True)
    _SUP_THREAD.start()

def _register(hb: "Heartbeater"):
    with _HB_LOCK:
        _HB_REFS.add(weakref.ref(hb))
    _start_supervisor()

class Heartbeater:
    """Emit lifecycle / heartbeat rows into the threads control-plane table.

    Automatically registers itself with a global supervisor that refreshes
    running heartbeats at a fixed interval.
    """
    def __init__(self, service: str, name: str, role: str = ""):
        self.ctx = BeatCtx(service, name, role, started_ts=time.time())
        self._w = get_threads_writer()
        self.is_terminal = False
        self._last_meta_json = "{}"  # preserve last non-empty meta across refresh beats
        _register(self)

    def _now_j(self):
        return to_j_instant(datetime.now(timezone.utc))

    def beat(self, state: str, uptime_s: int | None = None, last_error: str = "", meta: dict | None = None):
        now = time.time()
        uptime = int(uptime_s if uptime_s is not None else now - self.ctx.started_ts)
        if uptime < 0:  # guard against clock adjustments producing negative
            uptime = 0
        if meta is not None:
            # Update cached meta json only when caller supplies one
            try:
                self._last_meta_json = json.dumps(meta or {}, separators=(",", ":"))
            except Exception:
                # fallback to empty on serialization error but retain previous if any
                if self._last_meta_json is None:
                    self._last_meta_json = "{}"
        meta_json = self._last_meta_json
        self._w.write_row(
            self.ctx.service,
            self.ctx.name,
            self.ctx.role,
            state,
            to_j_instant(datetime.fromtimestamp(self.ctx.started_ts, tz=timezone.utc)),
            self._now_j(),
            uptime,
            last_error or "",
            meta_json,
        )
        if state in ("stopped", "error"):
            self.is_terminal = True
