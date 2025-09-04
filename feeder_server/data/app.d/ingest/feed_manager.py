# ingest/feed_manager.py
from __future__ import annotations
from typing import List, Dict, Optional, Iterable
from core.contracts import EventBus, DhSink, JournalStore, EventStore, Producer
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
import os
DEBUG_IO = os.getenv("DF_DEBUG_IO", "0") not in ("0", "false", "False")

from runtime.consumers import dh_consumer_loop, journal_consumer_loop
from .feeder_specs import FeederSpec

class FeedManager:
    """Coordinates the ingestion pipeline (producers + DH/Journal consumers).
    """
    def __init__(
        self,
        *,
        bus: EventBus,
        event_store: EventStore,
        journal: Optional[JournalStore],
        dh_sink: Optional[DhSink],
    ) -> None:
        self.bus = bus
        self.event_store = event_store
        self.journal = journal
        self.dh_sink = dh_sink
        self._producers: Dict[str, Producer] = {}
        self._threads: Dict[str, object] = {}

    # -------------------- Consumers (core) --------------------
    def start_consumers(self) -> None:
        if self.dh_sink is not None and "dh_consumer" not in self._threads:
            self._threads["dh_consumer"] = spawn(
                "feeder", "core", "dh_consumer", dh_consumer_loop, self.bus, self.dh_sink
            )
        if self.journal is not None and "journal_consumer" not in self._threads:
            self._threads["journal_consumer"] = spawn(
                "feeder",
                "core",
                "journal_consumer",
                journal_consumer_loop,
                self.event_store,
                self.journal,
                self.bus,
            )

    def stop_consumers(self, timeout: float = 2.0) -> None:
        for key, t in list(self._threads.items()):
            try:
                if hasattr(t, "stop"):
                    t.stop()  # type: ignore[attr-defined]
                if hasattr(t, "join"):
                    t.join(timeout=timeout)  # type: ignore[attr-defined]
            except Exception:
                pass
            finally:
                self._threads.pop(key, None)

    # -------------------- Producers registry --------------------
    def register(self, producer: Producer) -> None:
        self._producers[producer.name] = producer
        emit_event("feeder", producer.name, "producer", "INFO", "REGISTER", "Producer registered")

    def list_producers(self) -> List[str]:
        return list(self._producers.keys())

    def get_producer(self, name: str) -> Producer:
        return self._producers[name]

    # -------------------- Single producer control --------------------
    def start_producer(self, name: str) -> None:
        p = self._producers[name]
        p.start()
        emit_event("feeder", p.name, "producer", "INFO", "START", "Producer started")

    def stop_producer(self, name: str, *, join: bool = False, timeout: Optional[float] = None) -> None:
        p = self._producers[name]
        p.stop()
        emit_event("feeder", p.name, "producer", "INFO", "STOP", "Producer stopped")
        if join and hasattr(p, "join"):
            try:
                p.join(timeout=timeout)  # type: ignore[attr-defined]
            except Exception:
                pass

    # -------------------- Bulk producer control --------------------
    def start_all_producers(self, names: Optional[Iterable[str]] = None) -> None:
        targets = list(names) if names is not None else self.list_producers()
        for n in targets:
            self.start_producer(n)

    def stop_all_producers(
        self,
        names: Optional[Iterable[str]] = None,
        *,
        join: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        targets = list(names) if names is not None else self.list_producers()
        to_join: list[Producer] = []
        for n in targets:
            p = self._producers[n]
            p.stop()
            to_join.append(p)
        if join:
            for p in to_join:
                if hasattr(p, "join"):
                    try:
                        p.join(timeout=timeout)  # type: ignore[attr-defined]
                    except Exception:
                        pass

    # -------------------- Autostart --------------------
    def start_autostart(self, specs: List[FeederSpec]) -> None:
        for s in specs:
            if not s.autostart:
                continue
            for st in s.streams:
                pname = f"{s.provider}:{st}:{s.name}"
                try:
                    self.start_producer(pname)
                    emit_event("feeder", "feed_manager", "autostart", "INFO", "STARTED", pname)
                except Exception as e:
                    emit_event("feeder", "feed_manager", "autostart", "ERROR", "START_FAIL", f"{pname}: {e!r}")
