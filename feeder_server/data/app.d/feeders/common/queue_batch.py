"""Common queue + batching mixin for feeders.

Provides:
  - Bounded deque with lock
  - Config-driven batch flush (size or interval)
  - Writer thread lifecycle helpers
  - Queue / handler metrics emission via heartbeats

Expected attributes set by concrete feeder BEFORE __init__ of this mixin:
  self._cfg  : object with batch_size, flush_interval_s, metrics_enabled,
               metrics_interval, metrics_min_q_delta
  self._writer: object exposing write_row(*row)

Concrete feeder may override:
  _write_batch(rows) -> custom batch writing (default iter write_row)
  enrich_metrics(base: dict) -> add provider-specific metrics

Concrete feeder must still implement network/listener side and call enqueue(row).
"""
from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Deque, Iterable, Any
import time

from runtime.eventlog import emit_event
from runtime.dh_thread import spawn

class QueueBatchMixin:
    def __init__(self, *_, queue_maxlen: int = 10000, **__):  # noqa: D401
        self._q: Deque[tuple] = deque(maxlen=queue_maxlen)
        self._q_lock = Lock()
        self._batch_size = getattr(self._cfg, 'batch_size')
        self._flush_interval_s = getattr(self._cfg, 'flush_interval_s')
        # Metrics
        self._last_flush_ts = 0.0
        self._avg_handler_ms = 0.0
        self._dropped_msgs = 0
        self._last_metrics_emit = 0.0
        self._metrics_enabled = getattr(self._cfg, 'metrics_enabled', True)
        self._metrics_interval = getattr(self._cfg, 'metrics_interval', 60.0)
        self._metrics_min_q_delta = getattr(self._cfg, 'metrics_min_q_delta', 500)
        self._last_metrics_snapshot = (0, 0, 0.0)  # (q_len, dropped, avg_handler_ms)
        self.writer_worker = None

    # Public API -------------------------------------------------
    def enqueue(self, row: tuple):
        """Attempt to enqueue a row; increments drop counter if full."""
        with self._q_lock:
            if len(self._q) < self._q.maxlen:
                self._q.append(row)
            else:
                self._dropped_msgs += 1

    def start_writer(self, provider: str, name: str):
        if self.writer_worker is None or not self.writer_worker.is_alive():
            self.writer_worker = spawn("feeder", f"{provider}:{name}", "writer", self._writer_loop)
        return self.writer_worker

    def stop_writer(self):
        try:
            if self.writer_worker is not None:
                stop_method = getattr(self.writer_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass

    # Hooks ------------------------------------------------------
    def _write_batch(self, rows: list[tuple]):  # pragma: no cover - thin loop
        for r in rows:
            self._writer.write_row(*r)

    def enrich_metrics(self, base: dict) -> dict:
        return base

    # Writer loop ------------------------------------------------
    def _writer_loop(self, stop_event):
        last_flush = time.time()
        batch: list[tuple] = []
        while not stop_event.is_set():
            now = time.time()
            with self._q_lock:
                while self._q and len(batch) < self._batch_size:
                    batch.append(self._q.popleft())
            if batch and (len(batch) >= self._batch_size or (now - last_flush) >= self._flush_interval_s):
                try:
                    self._write_batch(batch)
                except Exception as e:  # pragma: no cover
                    emit_event("feeder", getattr(self, 'provider', 'unknown') + f":{getattr(self, 'name', 'unknown')}", "writer", "ERROR", "BATCH_ERR", f"Batch write error: {e}")
                batch.clear()
                last_flush = now
            if (now - self._last_flush_ts) >= 5:
                # force status emit (feeder override can call super().emit_status)
                if hasattr(self, 'emit_status'):
                    try:
                        self.emit_status(force=True)  # type: ignore
                    except Exception:
                        pass
                self._last_flush_ts = now
            time.sleep(0.01)
        # Final drain
        with self._q_lock:
            while self._q:
                batch.append(self._q.popleft())
        try:
            self._write_batch(batch)
        except Exception:
            pass

    # Metrics emission (invoked by feeder emit_status) ----------
    def _emit_queue_metrics(self):
        if not self._metrics_enabled:
            return
        now = time.time()
        q_len = len(self._q)
        dropped = self._dropped_msgs
        avg_ms = round(self._avg_handler_ms, 3)
        prev_q, prev_dropped, prev_avg = self._last_metrics_snapshot
        q_delta = abs(q_len - prev_q)
        avg_delta = abs(avg_ms - prev_avg)
        dropped_increase = dropped > prev_dropped
        interval_ok = (now - self._last_metrics_emit) >= self._metrics_interval
        significant_change = q_delta >= self._metrics_min_q_delta or dropped_increase or avg_delta >= 0.5
        if interval_ok or significant_change:
            base = {
                'q_len': q_len,
                'dropped': dropped,
                'avg_handler_ms': avg_ms,
                'batch_size': self._batch_size,
                'q_delta': q_delta,
                'interval_s': round(now - self._last_metrics_emit, 1) if self._last_metrics_emit else None,
            }
            metrics = self.enrich_metrics(base)
            # Update writer heartbeat if available
            try:
                if self.writer_worker is not None:
                    hb = getattr(self.writer_worker, '_hb', None)
                    if hb is not None:
                        hb.beat('running', meta=metrics)
            except Exception:
                pass
            self._last_metrics_emit = now
            self._last_metrics_snapshot = (q_len, dropped, avg_ms)

    # Helper for listener handlers to record processing time -----
    def _update_handler_timing(self, dur_ms: float):
        self._avg_handler_ms = dur_ms if self._avg_handler_ms == 0 else (self._avg_handler_ms * 0.9 + dur_ms * 0.1)

__all__ = ["QueueBatchMixin"]
