from __future__ import annotations
from core.contracts import EventBus, DhSink, JournalStore, EventStore
from runtime.eventlog import emit_event
import time


def dh_consumer_loop(stop_event, bus: EventBus, sink: DhSink):
    last = 0
    while not stop_event.is_set():
        try:
            envs = bus.drain_for_dh(max_n=256, timeout=0.2)
            if not envs:
                continue
            for env in envs:
                try:
                    sink.write_envelope(env)
                except Exception as e:  # isolate per-envelope errors
                    emit_event("feeder", "dh_consumer", "consumer", "ERROR", "DH_WRITE_ERR", f"batch={env.batch_id} err={e!r}")
                last = env.batch_id
            bus.ack_dh(last)
        except Exception as loop_err:
            emit_event("feeder", "dh_consumer", "consumer", "ERROR", "DH_LOOP_ERR", f"{loop_err!r}")
            time.sleep(0.5)


def journal_consumer_loop(stop_event, event_store: EventStore, journal: JournalStore, bus: EventBus):
    try:
        next_id = event_store.last_committed() + 1
    except Exception:
        next_id = 1
    while not stop_event.is_set():
        try:
            envs = event_store.next_from(next_id, 256)
            if not envs:
                envs = bus.drain_for_journal(256, 0.2)
                if not envs:
                    continue
            for env in envs:
                try:
                    added = journal.append_batch(env.rows)   # idempotent
                except Exception as e:
                    emit_event("feeder", "journal_consumer", "consumer", "ERROR", "JOURNAL_APPEND_ERR", f"batch={env.batch_id} err={e!r}")
                    continue
                try:
                    event_store.mark_committed(env.batch_id)
                except Exception as e:
                    emit_event("feeder", "journal_consumer", "consumer", "ERROR", "JOURNAL_MARK_ERR", f"batch={env.batch_id} err={e!r}")
                next_id = env.batch_id + 1
            try:
                bus.ack_journal(envs[-1].batch_id)
            except Exception as e:
                emit_event("feeder", "journal_consumer", "consumer", "ERROR", "JOURNAL_ACK_ERR", f"err={e!r}")
        except Exception as loop_err:
            emit_event("feeder", "journal_consumer", "consumer", "ERROR", "JOURNAL_LOOP_ERR", f"{loop_err!r}")
            time.sleep(0.5)
