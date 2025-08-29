# feeders/binance/feeder.py
from __future__ import annotations
from typing import List
from threading import Event
import json, websocket, time
from datetime import datetime, timezone, timedelta

from deephaven.time import to_j_instant

from feeders.base import BaseFeeder
from feeders.binance.backfill import BinanceGapFiller
from runtime.eventlog import emit_event
from runtime.dh_thread import spawn
from .schema import binance_trades_writer
from .config import load_config

class BinanceFeeder(BaseFeeder):
    """Binance trade stream feeder.

    Architecture:
      - Network listener thread (websocket) parses messages -> builds trade row -> enqueue().
      - Writer thread (from QueueBatchMixin) drains bounded deque and writes rows in batches based
        on batch_size or flush_interval.
      - Queue / batching / metrics (q_len, dropped, avg_handler_ms) centralized in QueueBatchMixin
        and emitted via writer heartbeat meta; listener heartbeat includes msg_count.

    Configuration (env overrides parsed in binance/config.py):
      DEEPFEEDER_BINANCE_BATCH_SIZE, DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S,
      DEEPFEEDER_BINANCE_METRICS_ENABLED, DEEPFEEDER_BINANCE_METRICS_INTERVAL,
      DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA

    Responsibilities kept here are Binance message schema parsing and websocket lifecycle.
    """

    def __init__(self, name: str, symbols: List[str]):
        # Load configuration and set on self before initializing BaseFeeder
        cfg = load_config()
        self._cfg = cfg
        # Initialize BaseFeeder (also initializes queue internals)
        super().__init__('binance', name, symbols, queue_maxlen=10000)
        self._trades_writer = binance_trades_writer()
        self.ws = None
        self.listener_worker = None
        self._writer = self._trades_writer
        # Multiple GapFiller workers
        self.gap_fillers = []

    def is_alive(self) -> bool:
        return self.listener_worker is not None and self.listener_worker.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return 'already running'
        self.started_at = time.time()
        self.last_error = None

        # Optional warm replay before opening WS (env-gated)
        try:
            if self._cfg.warm_replay_on_start:
                secs = int(self._cfg.warm_replay_window_secs)
                now = datetime.now(timezone.utc)
                t0 = (now - timedelta(seconds=secs)).isoformat().replace("+00:00", "Z")
                t1 = now.isoformat().replace("+00:00", "Z")
                import deepfeeder as dfb  # lazy import to avoid circulars
                for sym in self.symbols:
                    msg = dfb.replay("binance", sym, t0, t1)
        except Exception as exc:
            emit_event("journal", f"{self.provider}:{self.name}", "replay", "ERROR", "REPLAY_ERR", f"Replay error: {exc}")

        # Start listener worker
        self.start_writer(self.provider, self.name)
        self.listener_worker = spawn("feeder", f"{self.provider}:{self.name}", "listener", self._run)
        emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        self.emit_status(force=True)
        
        # Start GapFillers for all symbols
        self.gap_fillers = []
        for symbol in self.symbols:
            gap_filler = BinanceGapFiller(symbol=symbol)
            gap_filler.start(
                scan_interval=int(getattr(self._cfg, 'gap_scan_interval', 60)),
                api_key=getattr(self._cfg, 'binance_api_key', None),
                dh_table=self._trades_writer.table,
            )
            self.gap_fillers.append(gap_filler)

        return 'started'

    def stop(self):
        try:
            if self.listener_worker is not None:
                stop_method = getattr(self.listener_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        self.stop_writer()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive() and self.listener_worker is not None:
            self.listener_worker.join(timeout=3)
        
        # Stop all gap fillers if running
        try:
            for gap_filler in self.gap_fillers:
                gap_filler.stop(timeout=3)
        except Exception:
            pass

        emit_event("feeder", f"binance:{self.name}", "listener", "INFO", "STOP", "Feeder stopping")
        self.emit_status(force=True)
        return 'stopped'

    def emit_status(self, force: bool = False):  # type: ignore[override]
        super().emit_status(force=force)
        self._emit_queue_metrics()
        try:
            if self.listener_worker is not None:
                hb_l = getattr(self.listener_worker, '_hb', None)
                if hb_l is not None:
                    hb_l.beat('running', meta={'msg_count': int(self.msg_count)})
        except Exception:
            pass

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
            self.enqueue(row)
            self.msg_count += 1
            self.last_msg_ts = ts_trade
        except Exception as ex:
            self.last_error = str(ex)
            emit_event("feeder", f"binance:{self.name}", "listener", "ERROR", "MSG_ERR", f"Message handling error: {ex}")
        finally:
            dur_ms = (time.time() - start) * 1000.0
            self._update_handler_timing(dur_ms)
            if (self.msg_count % 100) == 0 and self.msg_count:
                self.emit_status()

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
                emit_event("feeder", f"binance:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket run error: {e}", {"backoff_s": delay})
            finally:
                self.ws = None
                if not stop_event.is_set():
                    import random, time as _t
                    _t.sleep(delay + random.uniform(0, 0.5))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

