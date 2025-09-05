# core/event_bus.py

import time
import threading as th
from collections import deque
from .contracts import EventBus as EventBusProto, EventStore, Envelope, Tick


class EventBus(EventBusProto):
    """
    One ordered bus of normalized events (Ticks) with:
      - single publish()
      - two independent drains (DH / Journal)
      - durable event store + bounded memory
    """
    def __init__(self, event_store: EventStore, max_envelopes: int = 100_000):
        self._event_store = event_store
        self._buf = deque()              # in-memory ring of Envelopes
        self._max = max_envelopes
        self._lock = th.RLock()
        self._cv = th.Condition(self._lock)
        _last_commit = event_store.last_committed()
        self._jr_cursor = _last_commit
        self._dh_cursor = _last_commit

    def set_dh_cursor(self, batch_id: int) -> None:
        self._dh_cursor = int(batch_id)

    def publish(self, rows: list[Tick]) -> int:
        produced_ns = time.time_ns()
        # Persist first; fetch DB-assigned id
        env = Envelope(batch_id=-1, produced_at_ns=produced_ns, first_offset=None, last_offset=None, rows=rows)
        batch_id = self._event_store.append(env)
        env = Envelope(
            batch_id=batch_id,
            produced_at_ns=env.produced_at_ns,
            first_offset=env.first_offset,
            last_offset=env.last_offset,
            rows=env.rows,
        )

        with self._lock:
            self._buf.append(env)
            # bounded ring eviction (only if both drains have passed head)
            while self._buf and len(self._buf) > self._max:
                head = self._buf[0]
                if self._dh_cursor >= head.batch_id and self._jr_cursor >= head.batch_id:
                    self._buf.popleft()
                else:
                    break
            self._cv.notify_all()

        return batch_id

    def _drain(self, last_id: int, max_n: int, timeout: float) -> list[Envelope]:
        deadline = time.time() + timeout
        with self._lock:
            while True:
                out = [e for e in self._buf if e.batch_id > last_id][:max_n]
                if out:
                    return out
                # Fallback to durable event store (journal / restart)
                out = self._event_store.next_from(last_id + 1, max_n)
                if out:
                    return out
                remaining = deadline - time.time()
                if remaining <= 0:
                    return []
                self._cv.wait(remaining)

    def drain_for_dh(self, max_n: int, timeout: float) -> list[Envelope]:
        return self._drain(self._dh_cursor, max_n, timeout)

    def drain_for_journal(self, max_n: int, timeout: float) -> list[Envelope]:
        return self._drain(self._jr_cursor, max_n, timeout)

    def ack_dh(self, last_batch_id: int) -> None:
        with self._lock:
            self._dh_cursor = max(self._dh_cursor, last_batch_id)
            self._cv.notify_all()

    def ack_journal(self, last_batch_id: int) -> None:
        with self._lock:
            self._jr_cursor = max(self._jr_cursor, last_batch_id)
            self._cv.notify_all()
