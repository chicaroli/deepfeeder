# app.d/providers/binance_feeder.py
from __future__ import annotations
from threading import Thread, Event
from typing import List
import json, websocket, jpy, time

from deephaven import DynamicTableWriter  # only for typing
from core.base import BaseFeeder
from core.bus import get_trades_writer

JInstant = jpy.get_type("java.time.Instant")

class BinanceFeeder(BaseFeeder):
    def __init__(self, name: str, symbols: List[str]):
        super().__init__("binance", name, symbols)
        self._trades_writer = get_trades_writer()
        self.stop_event = Event()
        self.ws = None
        self.thread = Thread(target=self._run, daemon=True, name=f"bf:{name}")

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return "already running"
        self.stop_event.clear()
        self.started_at = time.time()
        self.last_error = None
        self.thread = Thread(target=self._run, daemon=True, name=f"bf:{self.name}")
        self.thread.start()
        self.emit_status(force=True)
        return "started"

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive():
            self.thread.join(timeout=3)
        self.emit_status(force=True)
        return "stopped"

    def _on_message(self, _ws, message: str):
        try:
            m = json.loads(message); d = m.get("data", m)
            ts = JInstant.ofEpochMilli(int(d.get("T") or d.get("E")))
            sym = (d.get("s") or "").lower()
            self._trades_writer.write_row(ts, sym, float(d["p"]), float(d["q"]), message, self.provider)
            self.msg_count += 1
            self.last_msg_ts = ts
            self.emit_status()  # throttled
        except Exception as e:
            self.last_error = str(e)
            self.emit_status(force=True)

    def _run(self):
        url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in self.symbols)
        delay = 1
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda _ws, e: print(f"[binance:{self.name}] ws error: {e}"),
                    on_close=lambda *_: print(f"[binance:{self.name}] ws closed"),
                    on_open=lambda *_: print(f"[binance:{self.name}] ws open"),
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                print(f"[binance:{self.name}] ws error: {e}")
            finally:
                self.ws = None
                if not self.stop_event.is_set():
                    import random, time as _t
                    _t.sleep(delay + random.uniform(0, 0.5))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)
