# feeders/base.py
"""Base feeder implementation: contract + queue/batching behavior.

This module contains the canonical `BaseFeeder` which provides the common
feeder state, queueing/batching writer loop, metrics emission, and the
abstract contract (`start`, `stop`, `is_alive`). It replaces the old split
implementation that used a separate `QueueBatchMixin` module.
"""
from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Deque, List
import time

from runtime.eventlog import emit_event
from runtime.dh_thread import spawn
from abc import ABC, abstractmethod
from ingest.manager_tables import get_status_writer


__all__ = ["BaseFeeder"]

class BaseFeeder(ABC):
	"""Merged feeder base class: contract + queue/batching behavior.

	Backwards compatibility:
	  - Feeders that previously called BaseFeeder.__init__(provider,name,symbols)
		and then QueueBatchMixin.__init__(...) will still work: the queue
		internals are initialized by this ctor and using getattr on _cfg where
		appropriate.
	  - New feeders can call super().__init__(provider, name, symbols,
		queue_maxlen=...)
	"""

	def __init__(self, provider: str = None, name: str = None, symbols: List[str] | None = None, queue_maxlen: int = 10000, **__):  # noqa: D401
		# If provider/name/symbols provided -> initialize shared feeder state
		if provider is not None:
			self.provider = provider
			self.name = name
			self.symbols = sorted({s.lower() for s in symbols}) if symbols is not None else []
			self.msg_count = 0
			self.last_msg_ts = None
			self.last_error = None
			self.started_at = time.time()
			self._last_emit = 0.0
			# status writer kept for legacy status emissions
			try:
				self._status_writer = get_status_writer()
			except Exception:
				self._status_writer = None

		# Initialize queue/batching internals. This step is safe to call
		# multiple times; it will (re)create the queue and metrics state.
		self._q: Deque[tuple] = deque(maxlen=queue_maxlen)
		self._q_lock = Lock()
		# _cfg is expected to be set by the concrete feeder before calling
		# this ctor when using the legacy two-step pattern; use getattr so
		# callers that haven't set _cfg don't crash (they should set it).
		self._batch_size = getattr(self, '_cfg', None) and self._cfg.batch_size
		self._flush_interval_s = getattr(self, '_cfg', None) and self._cfg.flush_interval_s
		# Metrics
		self._last_flush_ts = 0.0
		self._avg_handler_ms = 0.0
		self._dropped_msgs = 0
		self._last_metrics_emit = 0.0
		self._metrics_enabled = getattr(getattr(self, '_cfg', None), 'metrics_enabled', True)
		self._metrics_interval = getattr(getattr(self, '_cfg', None), 'metrics_interval', 60.0)
		self._metrics_min_q_delta = getattr(getattr(self, '_cfg', None), 'metrics_min_q_delta', 500)
		self._last_metrics_snapshot = (0, 0, 0.0)  # (q_len, dropped, avg_handler_ms)
		self.writer_worker = None

	# Abstract contract -------------------------------------------------
	@abstractmethod
	def start(self): ...

	@abstractmethod
	def stop(self): ...

	@abstractmethod
	def is_alive(self) -> bool: ...


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

	# Status emission (originally on BaseFeeder) ---------------------------
	def emit_status(self, force: bool = False):
		"""Emit feeder status via the configured status writer.

		This method mirrors the previous BaseFeeder.emit_status implementation and
		will be used by feeders that extend this base. It also calls
		_emit_queue_metrics when appropriate (feeders may call it themselves
		after calling super().emit_status()).
		"""
		try:
			now = time.time()
			if not force and (now - self._last_emit) < 1.0:
				return  # throttle to 1/s
			self._last_emit = now
			uptime = int(now - getattr(self, 'started_at', now))
			if uptime < 0:
				uptime = 0
			sw = getattr(self, '_status_writer', None)
			if sw is not None:
				# provider, name, is_alive, symbols, msg_count, last_msg_ts, uptime, last_error
				try:
					sw.write_row(
						getattr(self, 'provider', 'unknown'),
						getattr(self, 'name', 'unknown'),
						self.is_alive() if hasattr(self, 'is_alive') else False,
						",".join(getattr(self, 'symbols', [])),
						int(getattr(self, 'msg_count', 0)),
						getattr(self, 'last_msg_ts', None),
						uptime,
						getattr(self, 'last_error', None),
					)
				except Exception:
					pass
		except Exception:
			# Be defensive: status emission must not raise
			pass
