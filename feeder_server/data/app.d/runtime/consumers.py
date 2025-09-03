from __future__ import annotations
from core.contracts import EventBus, DhSink, JournalStore, EventStore
from runtime.eventlog import emit_event
import time, threading


def dh_consumer_loop(stop_event, bus: EventBus, sink: DhSink):
    hb = getattr(threading.current_thread(), "_hb", None)

    last = -1               # last batch_id successfully processed
    envs_total = 0
    rows_total = 0
    last_beat = 0.0

    # optional initial beat
    if hb:
        hb.beat("running", meta={"envs": 0, "rows": 0, "cursor": last})

    while not stop_event.is_set():
        try:
            envs = bus.drain_for_dh(max_n=1024, timeout=0.25)
            if not envs:
                # idle heartbeat every ~5s
                now = time.time()
                if hb and (now - last_beat) > 5.0:
                    hb.beat("running", meta={"envs": envs_total, "rows": rows_total, "cursor": last})
                    last_beat = now
                continue

            # process batch
            for env in envs:
                try:
                    sink.write_envelope(env)
                except Exception as e:
                    emit_event("feeder", "dh_consumer", "consumer", "ERROR", "DH_WRITE_ERR",
                               f"batch={env.batch_id} err={e!r}")
                    # keep going; do not ack this batch as 'last'
                    continue
                envs_total += 1
                rows_total += len(env.rows)
                last = env.batch_id

            # advance DH cursor after the batch
            if last >= 0:
                bus.ack_dh(last)

            # active heartbeat while busy (about 2/s)
            now = time.time()
            if hb and (now - last_beat) > 0.5:
                hb.beat("running", meta={"envs": envs_total, "rows": rows_total, "cursor": last})
                last_beat = now

        except Exception as loop_err:
            emit_event("feeder", "dh_consumer", "consumer", "ERROR", "DH_LOOP_ERR", f"{loop_err!r}")
            if hb:
                hb.beat("error", last_error=str(loop_err),
                        meta={"envs": envs_total, "rows": rows_total, "cursor": last})
            time.sleep(0.5)


def journal_consumer_loop(stop_event, event_store: EventStore, journal: JournalStore, bus: EventBus):
    hb = getattr(threading.current_thread(), "_hb", None)

    # progress counters
    envs_total = 0           # envelopes processed & committed
    rows_total = 0           # rows appended into journal
    last_committed = -1      # last batch_id successfully marked committed
    last_beat = 0.0

    # start position
    try:
        last_committed = int(event_store.last_committed())
        next_id = last_committed + 1
    except Exception:
        last_committed = -1
        next_id = 1

    # initial heartbeat
    if hb:
        hb.beat("running", meta={
            "envs": envs_total, "rows": rows_total,
            "committed_to": last_committed, "cursor": next_id,
        })

    while not stop_event.is_set():
        try:
            # Prefer persisted backlog first (replay/recovery-safe)
            envs = event_store.next_from(next_id, 256)
            if not envs:
                # Otherwise drain from the live bus
                envs = bus.drain_for_journal(256, 0.2)
                if not envs:
                    # idle beat every ~5s
                    now = time.time()
                    if hb and (now - last_beat) > 5.0:
                        hb.beat("running", meta={
                            "envs": envs_total, "rows": rows_total,
                            "committed_to": last_committed, "cursor": next_id,
                        })
                        last_beat = now
                    continue

            # process batch
            last_bid = None
            for env in envs:
                try:
                    added = journal.append_batch(env.rows)   # idempotent
                except Exception as e:
                    emit_event("feeder", "journal_consumer", "consumer", "ERROR",
                               "JOURNAL_APPEND_ERR", f"batch={env.batch_id} err={e!r}")
                    # don't mark committed; skip to next env
                    continue

                rows_total += (int(added) if isinstance(added, int) and added >= 0 else len(env.rows))

                try:
                    event_store.mark_committed(env.batch_id)
                    last_committed = env.batch_id
                    next_id = env.batch_id + 1
                    last_bid = env.batch_id
                    envs_total += 1
                except Exception as e:
                    emit_event("feeder", "journal_consumer", "consumer", "ERROR",
                               "JOURNAL_MARK_ERR", f"batch={env.batch_id} err={e!r}")
                    # don't advance next_id past failing batch
                    break

            # ack the bus only up to what we actually handled
            if last_bid is not None:
                try:
                    bus.ack_journal(last_bid)
                except Exception as e:
                    emit_event("feeder", "journal_consumer", "consumer", "ERROR",
                               "JOURNAL_ACK_ERR", f"err={e!r}")

            # active heartbeat (more frequent while processing)
            now = time.time()
            if hb and (now - last_beat) > 0.5:
                hb.beat("running", meta={
                    "envs": envs_total, "rows": rows_total,
                    "committed_to": last_committed, "cursor": next_id,
                })
                last_beat = now

        except Exception as loop_err:
            emit_event("feeder", "journal_consumer", "consumer", "ERROR",
                       "JOURNAL_LOOP_ERR", f"{loop_err!r}")
            if hb:
                hb.beat("error", last_error=str(loop_err), meta={
                    "envs": envs_total, "rows": rows_total,
                    "committed_to": last_committed, "cursor": next_id,
                })
            time.sleep(0.5)