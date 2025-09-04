from __future__ import annotations
import time, threading
from runtime.eventlog import emit_event
from core.contracts import EventBus, DhSink, JournalStore, EventStore


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

    envs_total = 0
    rows_total = 0
    last_committed = -1
    last_beat = 0.0

    # one-time logging flags
    logged_check = False
    in_recovery = False
    logged_recovery_none = False
    logged_live_start = False
    rec_envs = 0
    rec_rows = 0
    rec_t0 = 0.0

    # start position
    try:
        last_committed = int(event_store.last_committed())
        next_id = last_committed + 1
    except Exception:
        last_committed = -1
        next_id = 1

    if hb:
        hb.beat("running", meta={
            "envs": envs_total, "rows": rows_total,
            "committed_to": last_committed, "cursor": next_id,
        })

    while not stop_event.is_set():
        try:
            # --- Persisted replay path
            envs = event_store.next_from(next_id, 256)

            if not logged_check:
                emit_event("feeder","journal_consumer","recovery","INFO","RECOVERY_CHECK",
                           f"last_committed={last_committed} next_id={next_id} have_persisted={bool(envs)}")
                logged_check = True
                if not envs and not logged_recovery_none:
                    emit_event("feeder","journal_consumer","recovery","INFO","RECOVERY_NONE",
                               "No persisted envelopes to replay (already up-to-date)")
                    logged_recovery_none = True

            from_store = bool(envs)

            if not envs:
                # --- Live bus path
                envs = bus.drain_for_journal(256, 0.2)
                if envs and not logged_live_start and not in_recovery:
                    emit_event("feeder","journal_consumer","live","INFO","LIVE_START",
                               "Switching to live drain from bus")
                    logged_live_start = True
                if not envs:
                    # idle heartbeat every ~5s
                    now = time.time()
                    if hb and (now - last_beat) > 5.0:
                        hb.beat("running", meta={
                            "envs": envs_total, "rows": rows_total,
                            "committed_to": last_committed, "cursor": next_id,
                        })
                        last_beat = now
                    continue

            # if we just entered recovery, announce it
            if from_store and not in_recovery:
                in_recovery = True
                rec_envs = rec_rows = 0
                rec_t0 = time.time()
                emit_event("feeder","journal_consumer","recovery","INFO","RECOVERY_START",
                           f"Persisted replay starting at batch_id={next_id}")

            # process a batch
            last_bid = None
            for env in envs:
                try:
                    added = journal.append_batch(env.rows)   # idempotent
                except Exception as e:
                    emit_event("feeder","journal_consumer","consumer","ERROR","JOURNAL_APPEND_ERR",
                               f"batch={env.batch_id} err={e!r}")
                    continue

                rows_added = int(added) if isinstance(added, int) and added >= 0 else len(env.rows)
                rows_total += rows_added
                if from_store:
                    rec_rows += rows_added

                try:
                    event_store.mark_committed(env.batch_id)
                    last_committed = env.batch_id
                    next_id = env.batch_id + 1
                    last_bid = env.batch_id
                    envs_total += 1
                    if from_store:
                        rec_envs += 1
                except Exception as e:
                    emit_event("feeder","journal_consumer","consumer","ERROR","JOURNAL_MARK_ERR",
                               f"batch={env.batch_id} err={e!r}")
                    break

            # if we were replaying and this batch came from the bus, recovery just ended
            if not from_store and in_recovery:
                dur = max(0.0, time.time() - rec_t0)
                emit_event("feeder","journal_consumer","recovery","INFO","RECOVERY_DONE",
                           f"Persisted replay completed: envs={rec_envs} rows={rec_rows} "
                           f"committed_to={last_committed} took={dur:.2f}s")
                in_recovery = False

            # ack bus up to last processed
            if last_bid is not None:
                try:
                    bus.ack_journal(last_bid)
                except Exception as e:
                    emit_event("feeder","journal_consumer","consumer","ERROR","JOURNAL_ACK_ERR", f"err={e!r}")

            # active heartbeat every ~0.5s while busy
            now = time.time()
            if hb and (now - last_beat) > 0.5:
                hb.beat("running", meta={
                    "envs": envs_total, "rows": rows_total,
                    "committed_to": last_committed, "cursor": next_id,
                })
                last_beat = now

        except Exception as loop_err:
            if in_recovery:
                dur = max(0.0, time.time() - rec_t0)
                emit_event("feeder","journal_consumer","recovery","ERROR","RECOVERY_ABORTED",
                           f"envs={rec_envs} rows={rec_rows} committed_to={last_committed} "
                           f"took={dur:.2f}s err={loop_err!r}")
                in_recovery = False
            emit_event("feeder","journal_consumer","consumer","ERROR","JOURNAL_LOOP_ERR", f"{loop_err!r}")
            if hb:
                hb.beat("error", last_error=str(loop_err), meta={
                    "envs": envs_total, "rows": rows_total,
                    "committed_to": last_committed, "cursor": next_id,
                })
            time.sleep(0.5)