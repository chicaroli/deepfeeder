# app.d/runtime/orchestrator.py
from typing import List, Dict
from core.contracts import EventBus, DhSink, JournalStore, Outbox
from runtime.dh_thread import spawn_dh_thread
from runtime.heartbeat import Heartbeater
from runtime.consumers import dh_consumer_loop, journal_consumer_loop
from runtime.feeder_specs import FeederSpec

class Orchestrator:
    def __init__(self, *, bus: EventBus, outbox: Outbox, journal: JournalStore, dh_sink: DhSink):
        self.bus, self.outbox, self.journal, self.dh_sink = bus, outbox, journal, dh_sink
        self._producers: Dict[str, object] = {}  # name -> producer

    # Consumers
    def start_consumers(self):
        hb_dh  = Heartbeater("feeder","core","dh_consumer")
        hb_jrn = Heartbeater("feeder","core","journal_consumer")
        spawn_dh_thread("feeder","core","dh_consumer", dh_consumer_loop, self.bus, self.dh_sink, hb_dh)
        spawn_dh_thread("feeder","core","journal_consumer", journal_consumer_loop, self.outbox, self.journal, self.bus, hb_jrn)

    # Producers
    def register(self, producer) -> None:
        self._producers[producer.name] = producer
    def start_autostart(self, specs: List[FeederSpec]):
        to_start = {f"{s.provider}:{s.name}" for s in specs if s.autostart}
        for name, p in self._producers.items():
            if name in to_start: p.start()
    def start_by_name(self, name: str):
        self._producers[name].start()
    def stop_by_name(self, name: str):
        self._producers[name].stop()
