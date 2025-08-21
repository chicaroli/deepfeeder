# feeders/base.py
from abc import ABC, abstractmethod
from typing import List
import time
from ingest.manager_tables import get_status_writer

class BaseFeeder(ABC):
    """Abstract feeder base class for all provider-specific feeders."""
    def __init__(self, provider: str, name: str, symbols: List[str]):
        self.provider = provider
        self.name = name
        self.symbols = sorted({s.lower() for s in symbols})
        self.msg_count = 0
        self.last_msg_ts = None
        self.last_error = None
        self.started_at = time.time()
        self._last_emit = 0.0
        self._status_writer = get_status_writer()

    @abstractmethod
    def start(self): ...
    @abstractmethod
    def stop(self): ...
    @abstractmethod
    def is_alive(self) -> bool: ...

    def emit_status(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_emit) < 1.0:
            return  # throttle to 1/s
        self._last_emit = now
        uptime = int(now - self.started_at)
        if uptime < 0:
            uptime = 0
        self._status_writer.write_row(
            self.provider,
            self.name,
            self.is_alive(),
            ",".join(self.symbols),
            int(self.msg_count),
            self.last_msg_ts,
            uptime,
            self.last_error,
        )
