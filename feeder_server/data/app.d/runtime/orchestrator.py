# app.d/runtime/orchestrator.py
from __future__ import annotations
from typing import List, Dict, Optional, Iterable
from core.contracts import EventBus, DhSink, JournalStore, EventStore, Producer
from runtime.dh_thread import spawn
from runtime.consumers import dh_consumer_loop, journal_consumer_loop
from runtime.feeder_specs import FeederSpec

class Orchestrator:
    """
    Coordinates core consumers and *producers* (feeders).
    Bulk controls here apply to PRODUCERS ONLY (not the core DH/Journal consumers).
    """
    def __init__(
        self,
        *,
        bus: EventBus,
        event_store: EventStore,
        journal: Optional[JournalStore],
        dh_sink: Optional[DhSink],
    ):
        self.bus = bus
        self.event_store = event_store
        self.journal = journal
        self.dh_sink = dh_sink
        self._producers: Dict[str, Producer] = {}  # name -> producer

    # -------------------- Consumers (core) --------------------
    def start_consumers(self) -> None:
        """Start core DH + Journal consumer loops (not producers)."""
        if self.dh_sink is not None:
            spawn("feeder", "core", "dh_consumer", dh_consumer_loop,self.bus, self.dh_sink)
        if self.journal is not None:
            spawn(
                "feeder",
                "core",
                "journal_consumer",
                journal_consumer_loop,
                self.event_store,
                self.journal,
                self.bus,
            )

    # -------------------- Producers registry --------------------
    def register(self, producer: Producer) -> None:
        """Register a producer (feeder) by its unique .name."""
        if not hasattr(producer, "name") or not hasattr(producer, "start") or not hasattr(producer, "stop"):
            raise TypeError("producer must implement Producer protocol (name: str; start(); stop())")
        self._producers[producer.name] = producer

    def list_producers(self) -> List[str]:
        return list(self._producers.keys())

    def get_producer(self, name: str) -> Producer:
        return self._producers[name]

    # -------------------- Producers control (single) --------------------
    def start_producer(self, name: str) -> None:
        """Start a single producer by name (producers only)."""
        self._producers[name].start()

    def stop_producer(self, name: str, *, join: bool = False, timeout: Optional[float] = None) -> None:
        """Stop a single producer by name (producers only). Optionally join if it supports it."""
        p = self._producers[name]
        p.stop()
        if join and hasattr(p, "join"):
            try:
                p.join(timeout=timeout)  # type: ignore[attr-defined]
            except Exception:
                pass

    # -------------------- Producers control (bulk) --------------------
    def start_all_producers(self, names: Optional[Iterable[str]] = None) -> None:
        """
        Start all registered producers, or only those whose names are provided.
        Applies to producers only (does not affect core consumers).
        """
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
        """
        Stop all registered producers (or a subset).
        If join=True, call join(timeout) when available.
        """
        targets = list(names) if names is not None else self.list_producers()
        # Stop first, then optionally join
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
    def run_autostart(self, specs: List[FeederSpec]) -> None:
        """Start only the producers that have autostart=True in specs."""
        to_start = {f"{s.provider}:{s.name}" for s in specs if s.autostart}
        for name in self.list_producers():
            if name in to_start:
                self.start_producer(name)
