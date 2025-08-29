from __future__ import annotations
from core.contracts import EventBus, DhSink, JournalStore, Outbox

def dh_consumer_loop(bus: EventBus, sink: DhSink, heartbeater=None):
    last = 0
    while True:
        envs = bus.drain_for_dh(max_n=256, timeout=0.2)
        if not envs:
            if heartbeater: heartbeater.beat("idle")
            continue
        for env in envs:
            sink.write_envelope(env)
            last = env.batch_id
        bus.ack_dh(last)
        if heartbeater: heartbeater.beat("running", meta={"last_batch_id": last, "batch_count": len(envs)})

def journal_consumer_loop(outbox: Outbox, journal: JournalStore, bus: EventBus, heartbeater=None):
    next_id = outbox.last_committed() + 1
    while True:
        envs = outbox.next_from(next_id, 256)
        if not envs:
            # Optionally peek from in-memory bus for freshness
            envs = bus.drain_for_journal(256, 0.2)
            if not envs:
                if heartbeater: heartbeater.beat("idle")
                continue
        for env in envs:
            added = journal.append_batch(env.rows)   # idempotent
            outbox.mark_committed(env.batch_id)
            next_id = env.batch_id + 1
        bus.ack_journal(envs[-1].batch_id)
        if heartbeater: heartbeater.beat("running", meta={"last_committed": next_id - 1})
