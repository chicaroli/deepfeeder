# providers/binance/producer_backfill.py
from __future__ import annotations
import time
from typing import Optional, List
from dataclasses import dataclass

from runtime.dh_thread import spawn, DHThread
from runtime.eventlog import emit_event
from core.contracts import Producer, EventBus, Tick
from runtime.backfill.planner import BackfillPlanner, Task
from .rest_client import BinanceRest

# simple token-bucket style limiter
class _RateLimiter:
    def __init__(self, rps: float = 4.0, burst: int = 8):
        self.tokens = float(burst)
        self.rate = float(rps)
        self.max_tokens = float(burst)
        self.last = time.time()

    def acquire(self):
        now = time.time()
        self.tokens = min(self.max_tokens, self.tokens + (now - self.last) * self.rate)
        self.last = now
        if self.tokens < 1.0:
            need = (1.0 - self.tokens) / self.rate
            time.sleep(max(need, 0.0))
            self.tokens = 0.0
        else:
            self.tokens -= 1.0


@dataclass
class _Cfg:
    page_size: int = 5000
    rps: float = 4.0
    burst: int = 8


class BinanceBackfillProducer(Producer):
    """
    Producer that consumes backfill tasks from BackfillPlanner and emits recovered
    Binance trades via EventBus, using the *same* path and schema as realtime.

    Name format (recommended): "binance:backfill:<service_name>"
    """

    def __init__(
        self,
        name: str,
        bus: EventBus,
        planner: BackfillPlanner,
        rest: BinanceRest,
        *,
        cfg: Optional[_Cfg] = None,
    ):
        self.name = name
        self.bus = bus
        self.planner = planner
        self.rest = rest
        self.cfg = cfg or _Cfg()
        self.limiter = _RateLimiter(self.cfg.rps, self.cfg.burst)
        self._t: Optional[DHThread] = None
        self._running = False

    # ----- Producer protocol ------------------------------------------------
    def start(self):
        if self._t and self._t.is_alive():
            return
        self._running = True
        self._t = spawn("feeder", self.name, "worker", self._run)

    def stop(self):
        self._running = False
        if self._t:
            self._t.stop()
            try:
                self._t.join(timeout=2.0)
            except Exception:
                pass

    def is_alive(self) -> bool:
        return bool(self._t and self._t.is_alive())

    def join(self, timeout: Optional[float] = None):
        if self._t:
            self._t.join(timeout=timeout)

    # ----- Worker loop ------------------------------------------------------
    def _run(self, stop_event):
        emit_event("feeder", self.name, "worker", "INFO", "START", "backfill producer started")
        while not stop_event.is_set() and self._running:
            task = self.planner.claim_task("binance")
            if not task:
                time.sleep(0.2)
                continue
            try:
                self._process_task(task, stop_event)
                self.planner.mark_done(task.task_id)
            except Exception as e:  # noqa: BLE001
                emit_event("feeder", self.name, "worker", "ERROR", "TASK_ERR", repr(e),
                           meta={"task_id": task.task_id})
                self.planner.mark_failed(task.task_id, repr(e))

    # ----- Task processing --------------------------------------------------
    def _process_task(self, task: Task, stop_event):
        cur = int(task.start_id)
        end = int(task.end_id)
        total_rows = 0
        last_id = cur - 1

        while cur <= end and not stop_event.is_set():
            self.limiter.acquire()
            # 1) cap the page size to the remaining range
            limit = min(self.cfg.page_size, end - cur + 1)
            rows = self.rest.get_trades(task.symbol, from_id=cur, limit=limit)
            if not rows:
                break

            # 2) normalize and HARD-CAP to end_id
            ticks = []
            for r in rows:
                tid = int(r["id"])
                if tid <= last_id:
                    continue
                if tid > end:
                    break  # stop at the first row beyond end
                ts_ns = int(r["time"]) * 1_000_000
                ticks.append(Tick(
                    provider="binance", stream="trades", symbol=task.symbol,
                    ts_ns=ts_ns, seq=tid,
                    payload={"price": r["price"], "qty": r["qty"],
                             "isBuyerMaker": r["isBuyerMaker"],
                             "source": "rest_backfill", "task_id": task.task_id},
                    is_final=True,
                ))
                last_id = tid

            if ticks:
                self.bus.publish(ticks)
                total_rows += len(ticks)
                cur = last_id + 1
            else:
                # Safety: advance to avoid looping on stale pages
                cur = min(cur + limit, end + 1)

            # 3) exit when we hit the end exactly
            if last_id >= end:
                break

        emit_event("feeder", self.name, "worker", "INFO", "TASK_DONE",
                   f"{task.symbol} [{task.start_id},{task.end_id}] rows={total_rows}")
