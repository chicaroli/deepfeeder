import time, threading as th
from collections import deque
from typing import List
from .contracts import EventBus as EventBusProto, Outbox, Envelope, Tick

class EventBus(EventBusProto):
    """
    One ordered bus of normalized events (Ticks) with:
      - single publish()
      - two independent drains (DH / Journal)
      - durable outbox + bounded memory
    """
    def __init__(self, outbox: Outbox, max_envelopes: int = 100_000):
        self._outbox = outbox
        self._buf = deque()              # in-memory ring of Envelopes
        self._max = max_envelopes
        self._next_id = 0
        self._lock = th.RLock()
        self._cv = th.Condition(self._lock)
        self._dh_cursor = -1
        self._jr_cursor = outbox.last_committed()

    def publish(self, rows: List[Tick]) -> int:
        with self._lock:
            self._next_id += 1
            env = Envelope(self._next_id, time.time_ns(), None, None, rows)
            self._outbox.append(env)     # durable first
            self._buf.append(env)

            # Evict only after both readers consumed
            while len(self._buf) > self._max:
                head = self._buf[0]
                if self._dh_cursor >= head.batch_id and self._jr_cursor >= head.batch_id:
                    self._buf.popleft()
                else:
                    break
            self._cv.notify_all()
            return env.batch_id

    def _drain(self, last_id: int, max_n: int, timeout: float) -> List[Envelope]:
        deadline = time.time() + timeout
        with self._lock:
            while True:
                out = [e for e in self._buf if e.batch_id > last_id][:max_n]
                if out:
                    return out
                # Fallback to durable outbox (journal / restart)
                out = self._outbox.next_from(last_id + 1, max_n)
                if out:
                    return out
                remaining = deadline - time.time()
                if remaining <= 0:
                    return []
                self._cv.wait(remaining)

    def drain_for_dh(self, max_n: int, timeout: float) -> List[Envelope]:
        return self._drain(self._dh_cursor, max_n, timeout)

    def drain_for_journal(self, max_n: int, timeout: float) -> List[Envelope]:
        return self._drain(self._jr_cursor, max_n, timeout)

    def ack_dh(self, last_batch_id: int) -> None:
        with self._lock:
            self._dh_cursor = max(self._dh_cursor, last_batch_id)
            self._cv.notify_all()

    def ack_journal(self, last_batch_id: int) -> None:
        with self._lock:
            self._jr_cursor = max(self._jr_cursor, last_batch_id)
            self._cv.notify_all()
