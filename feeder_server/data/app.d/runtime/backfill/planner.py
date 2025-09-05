# runtime/backfill/planner.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Deque, Optional, Tuple, List
from collections import deque
import threading
import uuid
import time

from runtime.eventlog import emit_event

# UI mirroring (best-effort)
try:
    from ingest.manager_tables import (
        get_backfill_gaps_writer,
        get_backfill_tasks_writer,
    )
except Exception:
    get_backfill_gaps_writer = lambda: None   # type: ignore
    get_backfill_tasks_writer = lambda: None  # type: ignore


@dataclass(frozen=True)
class Gap:
    provider: str
    symbol: str
    start_id: int  # inclusive
    end_id: int    # inclusive
    discovered_at_ns: int
    priority: int = 0


@dataclass
class Task:
    task_id: str
    provider: str
    symbol: str
    start_id: int
    end_id: int
    created_at_ns: int
    status: str = "pending"
    try_count: int = 0
    last_error: str = ""


class BackfillPlanner:
    """
    In-memory planner that:
      - receives gaps
      - coalesces & slices into tasks
      - allows producers to claim tasks (per provider)
      - mirrors state to UI tables (DTWs), best-effort
    """

    def __init__(self, *, max_ids_per_task: int = 5000, gap_merge_distance: int = 0):
        self.max_ids_per_task = int(max_ids_per_task)
        self.gap_merge_distance = int(gap_merge_distance)
        self._lock = threading.RLock()
        self._queues: Dict[str, Deque[Task]] = {}     # provider -> tasks queue
        self._tasks: Dict[str, Task] = {}             # task_id -> Task

        # lazy DTWs
        self._gaps_w = None
        self._tasks_w = None

    # ----- UI writers (lazy) ------------------------------------------------
    def _g_writer(self):
        if self._gaps_w is None:
            try:
                self._gaps_w = get_backfill_gaps_writer()
            except Exception:
                self._gaps_w = None
        return self._gaps_w

    def _t_writer(self):
        if self._tasks_w is None:
            try:
                self._tasks_w = get_backfill_tasks_writer()
            except Exception:
                self._tasks_w = None
        return self._tasks_w

    # ----- Gap ingestion ----------------------------------------------------
    def add_gap(self, gap: Gap) -> List[Task]:
        """Add a detected gap; returns the list of Tasks enqueued."""
        tasks: List[Task] = []
        # UI mirror
        try:
            gw = self._g_writer()
            if gw:
                gw.write_row(
                    gap.provider, gap.symbol, int(gap.start_id), int(gap.end_id),
                    int(gap.discovered_at_ns), "pending", int(gap.priority)
                )
        except Exception:
            pass

        # Slice into tasks
        cur = gap.start_id
        while cur <= gap.end_id:
            end = min(cur + self.max_ids_per_task - 1, gap.end_id)
            t = Task(
                task_id=str(uuid.uuid4()),
                provider=gap.provider,
                symbol=gap.symbol,
                start_id=cur,
                end_id=end,
                created_at_ns=time.time_ns(),
                status="pending",
            )
            self._enqueue_task(t)
            tasks.append(t)
            cur = end + 1

        emit_event("feeder", f"{gap.provider}:{gap.symbol}", "planner", "INFO", "GAP_ENQUEUED",
                   f"{gap.symbol} [{gap.start_id},{gap.end_id}] -> {len(tasks)} task(s)")

        return tasks

    def _enqueue_task(self, t: Task) -> None:
        with self._lock:
            q = self._queues.setdefault(t.provider, deque())
            q.append(t)
            self._tasks[t.task_id] = t
            # UI mirror
            try:
                tw = self._t_writer()
                if tw:
                    tw.write_row(t.task_id, t.provider, t.symbol, t.start_id, t.end_id,
                                 int(t.created_at_ns), t.status, t.try_count, t.last_error)
            except Exception:
                pass

    # ----- Claim / complete -------------------------------------------------
    def claim_task(self, provider: str) -> Optional[Task]:
        with self._lock:
            q = self._queues.get(provider)
            if not q:
                return None
            t = q.popleft()
            t.status = "running"
            t.try_count += 1
            self._mirror_task(t)
            return t

    def mark_done(self, task_id: str) -> None:
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return
            t.status = "done"
            self._mirror_task(t)

    def mark_failed(self, task_id: str, error: str) -> None:
        with self._lock:
            t = self._tasks.get(task_id)
            if not t:
                return
            t.status = "failed"
            t.last_error = error
            self._mirror_task(t)

    def _mirror_task(self, t: Task) -> None:
        try:
            tw = self._t_writer()
            if tw:
                tw.write_row(t.task_id, t.provider, t.symbol, t.start_id, t.end_id,
                             int(t.created_at_ns), t.status, t.try_count, t.last_error)
        except Exception:
            pass
