from __future__ import annotations
from core.contracts import EventBus, DhSink, JournalStore, EventStore

def dh_consumer_loop(stop_event, bus: EventBus, sink: DhSink):
    last = 0
    while not stop_event.is_set():
        envs = bus.drain_for_dh(max_n=256, timeout=0.2)
        if not envs:
            continue
        for env in envs:
            sink.write_envelope(env)
            last = env.batch_id
        bus.ack_dh(last)

def journal_consumer_loop(stop_event, event_store: EventStore, journal: JournalStore, bus: EventBus):
    next_id = event_store.last_committed() + 1
    while not stop_event.is_set():
        envs = event_store.next_from(next_id, 256)
        if not envs:
            # Optionally peek from in-memory bus for freshness
            envs = bus.drain_for_journal(256, 0.2)
            if not envs:
                continue
        for env in envs:
            added = journal.append_batch(env.rows)   # idempotent
            event_store.mark_committed(env.batch_id)
            next_id = env.batch_id + 1
        bus.ack_journal(envs[-1].batch_id)
