# feeders/binance/feeder.py
from __future__ import annotations
from typing import List
from threading import Event
import json, websocket, time
from datetime import datetime, timezone

from deephaven.time import to_j_instant

from feeders.base import BaseFeeder
from runtime.eventlog import emit_event
from runtime.dh_thread import spawn
from .schema import binance_trades_writer

class BinanceFeeder(BaseFeeder):
    def __init__(self, name: str, symbols: List[str]):
        super().__init__('binance', name, symbols)
        self._trades_writer = binance_trades_writer()
        self.ws = None
        self.thread = None  # will be created on start()

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return 'already running'
        # self.stop_event.clear()
        self.started_at = time.time()
        self.last_error = None
        
        self.thread = spawn("feeder", f"{self.provider}:{self.name}", "ws_loop", self._run)
        try:
            emit_event("feeder", f"binance:{self.name}", "ws_loop", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        except Exception:
            pass
        self.emit_status(force=True)
        return 'started'

    def stop(self):
        # Signal managed thread to stop
        try:
            if self.thread is not None:
                # DHThread exposes stop() method
                stop_method = getattr(self.thread, "stop", None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive():
            self.thread.join(timeout=3)
        try:
            emit_event("feeder", f"binance:{self.name}", "ws_loop", "INFO", "STOP", "Feeder stopping")
        except Exception:
            pass
        self.emit_status(force=True)
        return 'stopped'

    def _on_message(self, _ws, message: str):
        try:
            m = json.loads(message)
            d = m.get('data', m)
            ts_event = to_j_instant(datetime.fromtimestamp(int(d.get('E')) / 1000, tz=timezone.utc))
            ts_trade = to_j_instant(datetime.fromtimestamp(int(d.get('T')) / 1000, tz=timezone.utc))

            self._trades_writer.write_row(
                d.get('e'),
                ts_event,
                d.get('s'),
                int(d.get('t')),
                float(d.get('p')),
                float(d.get('q')),
                int(d.get('b', 0)),
                int(d.get('a', 0)),
                ts_trade,
                bool(d.get('m'))
            )

            self.msg_count += 1
            self.last_msg_ts = ts_trade
            self.emit_status()
        except Exception as ex:
            self.last_error = str(ex)
            self.emit_status(force=True)
            try:
                emit_event("feeder", f"binance:{self.name}", "ws_loop", "ERROR", "MSG_ERR", f"Message handling error: {ex}")
            except Exception:
                pass

    def _run(self, stop_event: Event):
        url = 'wss://stream.binance.com:9443/stream?streams=' + '/'.join(f"{s}@trade" for s in self.symbols)
        delay = 1
        while not stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda _ws, e: (print(f"[binance:{self.name}] ws error: {e}"), emit_event("feeder", f"binance:{self.name}", "ws_loop", "ERROR", "WS_ERR", f"WebSocket error callback: {e}")),
                    on_close=lambda *_: (print(f"[binance:{self.name}] ws closed"), emit_event("feeder", f"binance:{self.name}", "ws_loop", "WARN", "WS_CLOSED", "WebSocket closed")),
                    on_open=lambda *_: (print(f"[binance:{self.name}] ws open"), emit_event("feeder", f"binance:{self.name}", "ws_loop", "INFO", "WS_OPEN", "WebSocket open")),
                )
                emit_event("feeder", f"binance:{self.name}", "ws_loop", "INFO", "WS_CONNECT", "Connecting to Binance WS")
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                print(f"[binance:{self.name}] ws error: {e}")
                try:
                    emit_event("feeder", f"binance:{self.name}", "ws_loop", "ERROR", "WS_ERR", f"WebSocket run error: {e}", {"backoff_s": delay})
                except Exception:
                    pass
            finally:
                self.ws = None
                if not stop_event.is_set():
                    import random, time as _t
                    _t.sleep(delay + random.uniform(0, 0.5))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

