# ingest/feed_manager.py
from __future__ import annotations
from typing import List, Dict, Optional, Iterable
from core.contracts import EventBus, DhSink, JournalStore, EventStore, Producer
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
import os
from datetime import datetime, timezone
from ingest.manager_tables import get_status_writer
from deephaven.time import to_j_instant

DEBUG_IO = os.getenv("DF_DEBUG_IO", "0") not in ("0", "false", "False")

from runtime.consumers import dh_consumer_loop, journal_consumer_loop
from .feeder_specs import FeederSpec


class FeedManager:
    """Coordinates the ingestion pipeline (producers + DH/Journal consumers).

    This manager keeps a registry of Producer objects and exposes simple
    lifecycle operations. It also updates the shared status DynamicTableWriter
    via a single helper method so the rest of the codebase remains clean.
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
        # Cache the shared status DynamicTableWriter (best-effort)
        try:
            self._status_writer = get_status_writer()
        except Exception:
            self._status_writer = None

    def _write_status(self, producer_or_name, alive: bool, *, msg_count: int = 0,
                      last_error: Optional[str] = None, uptime: int = 0) -> None:
        """Write a single status row into the shared status writer.

        Accepts either a Producer object or a producer-name string. This is a
        best-effort helper: failures are swallowed so lifecycle operations
        never raise due to metrics/logging.
        """
        w = getattr(self, "_status_writer", None)
        if w is None:
            return

        # Normalize inputs
        try:
            if hasattr(producer_or_name, "name"):
                p = producer_or_name
                name = getattr(p, "name")
                provider = getattr(p, "provider", name.split(":", 1)[0] if ":" in name else "")
                symbols = ",".join(getattr(p, "symbols", [])) if getattr(p, "symbols", None) is not None else ""
            else:
                name = str(producer_or_name)
                provider = name.split(":", 1)[0] if ":" in name else ""
                symbols = ""
        except Exception:
            name = str(producer_or_name)
            provider = name.split(":", 1)[0] if ":" in name else ""
            symbols = ""

        try:
            w.write_row(
                provider,
                name,
                bool(alive),
                symbols,
                int(msg_count),
                to_j_instant(datetime.now(timezone.utc)),
                int(uptime),
                last_error or "",
            )
        except Exception as e:
            print(f"Failed to write status for {name!r}: {e!r}")


    def update_status(self, producer_or_name, alive: bool, *, msg_count: int = 0,
                      last_error: Optional[str] = None, uptime: int = 0) -> None:
        """Public method to update the Feeder status table.

        Thin wrapper around _write_status so external callers (UI/dashboard)
        can update status without touching internals. _write_status already
        swallows errors so this method is simple and does not need its own
        try/except.
        """
        self._write_status(producer_or_name, alive, msg_count=msg_count, last_error=last_error, uptime=uptime)

    # -------------------- Consumers (core) --------------------
    def start_consumers(self) -> None:
        if self.dh_sink is not None and "dh_consumer" not in self._threads:
            self._threads["dh_consumer"] = spawn(
                "feeder", "core", "dh_consumer", dh_consumer_loop, self.bus, self.dh_sink
            )
            emit_event("feeder", "CORE-DH", "consumer", "INFO", "START", "DH consumer started")
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
            emit_event("feeder", "CORE-Journal", "consumer", "INFO", "START", "Journal consumer started")

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
                emit_event("feeder", key, "consumer", "INFO", "STOP", f"{key} stopped")

    # -------------------- Producers registry --------------------
    def register(self, producer: Producer) -> None:
        """Register a Producer object with the manager and emit a snapshot row.

        The snapshot is non-alive by default so the UI shows configured entries
        even before producers are started.
        """
        self._producers[producer.name] = producer
        emit_event("feeder", producer.name, "producer", "INFO", "REGISTER", "Producer registered")
        # Snapshot row (non-alive) via public API
        self.update_status(producer, False)

    def list_producers(self) -> List[str]:
        return list(self._producers.keys())

    def get_producer(self, name: str) -> Producer:
        return self._producers[name]

    # -------------------- Single producer control --------------------
    def start_producer(self, name: str) -> None:
        p = self._producers[name]
        p.start()
        emit_event("feeder", p.name, "producer", "INFO", "START", "Producer started")
        self.update_status(p, True)

    def stop_producer(self, name: str, *, join: bool = False, timeout: Optional[float] = None) -> None:
        p = self._producers[name]
        p.stop()
        emit_event("feeder", p.name, "producer", "INFO", "STOP", "Producer stopped")
        self.update_status(p, False)
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

    def stop_all_producers(self, names: Optional[Iterable[str]] = None, *, join: bool = False,
                           timeout: Optional[float] = None) -> None:
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
