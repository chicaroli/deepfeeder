# feeders/binance/feeder.py
from __future__ import annotations
from typing import List, Deque, Optional
from threading import Event, Lock
import json, websocket, time, collections
from datetime import datetime, timezone

from deephaven.time import to_j_instant

from feeders.base import BaseFeeder
from runtime.eventlog import emit_event
from runtime.dh_thread import spawn
from .schema import binance_trades_writer
import os

class BinanceFeeder(BaseFeeder):
    """Binance trade stream feeder with decoupled listener (network) and writer workers.

    listener_worker: receives websocket messages, parses, enqueues
    writer_worker: flushes queue to Deephaven table in batches
    """

    def __init__(self, name: str, symbols: List[str]):
        super().__init__('binance', name, symbols)
        self._trades_writer = binance_trades_writer()
        self.ws = None
        self.listener_worker = None
        self.writer_worker = None
        # Queue & batching
        self._q = collections.deque(maxlen=10000)  # (tuple rows)
        self._q_lock = Lock()
        self._batch_size = int(os.getenv('DEEPFEEDER_BINANCE_BATCH_SIZE', '400'))
        self._flush_interval_s = float(os.getenv('DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S', '0.1'))
        # Metrics
        self._last_flush_ts = 0.0
        self._dropped_msgs = 0
        self._consecutive_timeouts = 0
        self._avg_handler_ms = 0.0
        # metrics emission control (enabled by default; now uses heartbeat meta not event log)
        self._metrics_enabled = os.getenv('DEEPFEEDER_BINANCE_METRICS_ENABLED', '1') not in ('0', 'false', 'False')
        self._metrics_interval = float(os.getenv('DEEPFEEDER_BINANCE_METRICS_INTERVAL', '60'))  # seconds
        self._metrics_min_q_delta = int(os.getenv('DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA', '500'))
        self._last_metrics_emit = 0.0
        self._last_metrics_snapshot = (0, 0, 0.0)  # (q_len, dropped, avg_handler_ms)

    # Lifecycle -------------------------------------------------
    def is_alive(self) -> bool:
        return self.listener_worker is not None and self.listener_worker.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return 'already running'
        self.started_at = time.time()
        self.last_error = None
        self.writer_worker = spawn("feeder", f"{self.provider}:{self.name}", "writer", self._writer_loop)
        self.listener_worker = spawn("feeder", f"{self.provider}:{self.name}", "listener", self._run)
        try:
            emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        except Exception:
            pass
        self.emit_status(force=True)
        return 'started'

    def stop(self):
        try:
            if self.listener_worker is not None:
                stop_method = getattr(self.listener_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        try:
            if self.writer_worker is not None:
                stop_method = getattr(self.writer_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive() and self.listener_worker is not None:
            self.listener_worker.join(timeout=3)
        try:
            emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "STOP", "Feeder stopping")
        except Exception:
            pass
        self.emit_status(force=True)
        return 'stopped'

    # Status / metrics ------------------------------------------
    def emit_status(self, force: bool = False):  # type: ignore[override]
        super().emit_status(force=force)
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
            metrics = {
                'q_len': q_len,
                'dropped': dropped,
                'avg_handler_ms': avg_ms,
                'batch_size': self._batch_size,
                'q_delta': q_delta,
                'interval_s': round(now - self._last_metrics_emit, 1) if self._last_metrics_emit else None,
            }
            # Update writer thread heartbeat meta (primary place for queue metrics)
            try:
                if self.writer_worker is not None:
                    # Access underlying Heartbeater (_hb) to update meta
                    hb = getattr(self.writer_worker, '_hb', None)
                    if hb is not None:
                        hb.beat('running', meta=metrics)
            except Exception:
                pass
            # Also refresh listener heartbeat with lightweight counters
            try:
                if self.listener_worker is not None:
                    hb_l = getattr(self.listener_worker, '_hb', None)
                    if hb_l is not None:
                        hb_l.beat('running', meta={'msg_count': int(self.msg_count), 'avg_handler_ms': avg_ms, 'dropped': dropped})
            except Exception:
                pass
            self._last_metrics_emit = now
            self._last_metrics_snapshot = (q_len, dropped, avg_ms)

    # WebSocket message handler --------------------------------
    def _on_message(self, _ws, message: str):
        start = time.time()
        try:
            m = json.loads(message)
            d = m.get('data', m)
            ts_event = to_j_instant(datetime.fromtimestamp(int(d.get('E')) / 1000, tz=timezone.utc))
            ts_trade = to_j_instant(datetime.fromtimestamp(int(d.get('T')) / 1000, tz=timezone.utc))
            row = (
                d.get('e'), ts_event, d.get('s'), int(d.get('t')),
                float(d.get('p')), float(d.get('q')),
                int(d.get('b', 0)), int(d.get('a', 0)),
                ts_trade, bool(d.get('m'))
            )
            with self._q_lock:
                if len(self._q) < self._q.maxlen:
                    self._q.append(row)
                    self.msg_count += 1
                    self.last_msg_ts = ts_trade
                else:
                    self._dropped_msgs += 1
        except Exception as ex:
            self.last_error = str(ex)
            try:
                emit_event("feeder", f"binance:{self.name}", "listener", "ERROR", "MSG_ERR", f"Message handling error: {ex}")
            except Exception:
                pass
        finally:
            dur_ms = (time.time() - start) * 1000.0
            self._avg_handler_ms = dur_ms if self._avg_handler_ms == 0 else (self._avg_handler_ms * 0.9 + dur_ms * 0.1)
            if (self.msg_count % 100) == 0 and self.msg_count:  # occasional
                self.emit_status()

    # Writer loop -----------------------------------------------
    def _writer_loop(self, stop_event: Event):
        last_flush = time.time()
        batch: list[tuple] = []
        while not stop_event.is_set():
            now = time.time()
            with self._q_lock:
                while self._q and len(batch) < self._batch_size:
                    batch.append(self._q.popleft())
            if batch and (len(batch) >= self._batch_size or (now - last_flush) >= self._flush_interval_s):
                try:
                    for r in batch:
                        self._trades_writer.write_row(*r)
                except Exception as e:
                    try:
                        emit_event("feeder", f"binance:{self.name}", "writer", "ERROR", "BATCH_ERR", f"Batch write error: {e}")
                    except Exception:
                        pass
                batch.clear()
                last_flush = now
            if (now - self._last_flush_ts) >= 5:
                self.emit_status(force=True)
                self._last_flush_ts = now
            time.sleep(0.01)
        # Flush on stop
        with self._q_lock:
            while self._q:
                batch.append(self._q.popleft())
        for r in batch:
            try:
                self._trades_writer.write_row(*r)
            except Exception:
                pass

    # Listener loop ---------------------------------------------
    def _run(self, stop_event: Event):
        url = 'wss://stream.binance.com:9443/stream?streams=' + '/'.join(f"{s}@trade" for s in self.symbols)
        delay = 1
        while not stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda _ws, e: emit_event("feeder", f"binance:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket error callback: {e}"),
                    on_close=lambda *_: emit_event("feeder", f"binance:{self.name}", "listener", "WARN", "WS_CLOSED", "WebSocket closed"),
                    on_open=lambda *_: emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "WS_OPEN", "WebSocket open"),
                )
                emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "WS_CONNECT", "Connecting to Binance WS")
                self.ws.run_forever(ping_interval=25, ping_timeout=15)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                try:
                    emit_event("feeder", f"binance:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket run error: {e}", {"backoff_s": delay})
                except Exception:
                    pass
            finally:
                self.ws = None
                if not stop_event.is_set():
                    import random, time as _t
                    _t.sleep(delay + random.uniform(0, 0.5))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

