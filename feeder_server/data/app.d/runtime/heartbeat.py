"""Heartbeat helper for emitting standardized service/thread lifecycle rows."""
from __future__ import annotations
import json, time
from dataclasses import dataclass
from datetime import datetime, timezone
from deephaven.time import to_j_instant
from .threads_bus import get_threads_writer

@dataclass
class BeatCtx:
    service: str
    name: str
    role: str = ""
    started_ts: float = 0.0
    restarts: int = 0

class Heartbeater:
    """Emit lifecycle / heartbeat rows into the threads control-plane table."""
    def __init__(self, service: str, name: str, role: str = ""):
        self.ctx = BeatCtx(service, name, role, started_ts=time.time(), restarts=0)
        self._w = get_threads_writer()

    def _now_j(self):
        return to_j_instant(datetime.now(timezone.utc))

    def beat(self, state: str, uptime_s: int | None = None, last_error: str = "", meta: dict | None = None):
        now = time.time()
        uptime = int(uptime_s if uptime_s is not None else now - self.ctx.started_ts)
        self._w.write_row(
            self.ctx.service,
            self.ctx.name,
            self.ctx.role,
            state,
            to_j_instant(datetime.fromtimestamp(self.ctx.started_ts, tz=timezone.utc)),
            self._now_j(),
            uptime,
            int(self.ctx.restarts),
            last_error or "",
            json.dumps(meta or {}, separators=(",", ":")),
        )

    def mark_restart(self):
        self.ctx.restarts += 1
