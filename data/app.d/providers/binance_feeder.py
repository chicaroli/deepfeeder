# app.d/providers/binance_feeder.py
from __future__ import annotations
from typing import List
import json, websocket, time
from threading import Thread, Event
from datetime import datetime, timezone

from deephaven import DynamicTableWriter
from deephaven.time import to_j_instant

from core.base import BaseFeeder
from core.bus import get_trades_writer
from providers.binance_schema import binance_trades_writer


class BinanceFeeder(BaseFeeder):
    def __init__(self, name: str, symbols: List[str]):
        super().__init__("binance", name, symbols)
        self._detailed_writer = binance_trades_writer()     # provider-specific
        self._bus_trades_writer = get_trades_writer()       # provider-agnostic
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
            m = json.loads(message)
            d = m.get("data", m)
            ts_event = to_j_instant(datetime.fromtimestamp(int(d.get("E")) / 1000, tz=timezone.utc))
            ts_trade = to_j_instant(datetime.fromtimestamp(int(d.get("T")) / 1000, tz=timezone.utc))
            symbol = d.get("s").lower()
            price = float(d.get("p"))
            qty = float(d.get("q"))

            # 1) provider-specific detailed row
            self._detailed_writer.write_row(
                d.get("e"),         # event type
                ts_event,
                d.get("s"),         # symbol
                int(d.get("t")),    # trade id
                price,
                qty,
                int(d.get("b", 0)), # buyer order id
                int(d.get("a", 0)), # seller order id
                ts_trade,
                bool(d.get("m"))    # is buyer maker
            )

            # 2) canonical bus row (skinny)
            # bus trades schema: ts, provider, symbol, price, qty, raw_json
            self._bus_trades_writer.write_row(ts_trade, self.provider, symbol, price, qty, message)

            # health/status
            self.msg_count += 1
            self.last_msg_ts = ts_trade
            self.emit_status()  # throttled
        except Exception as ex:
            self.last_error = str(ex)
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
