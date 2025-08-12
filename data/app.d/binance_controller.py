# /data/app.d/binance_controller.py
from __future__ import annotations
from deephaven.appmode import get_app_state
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import json, sys, types, time
from threading import Thread, Event
from typing import Dict, List
import websocket, jpy

JInstant = jpy.get_type("java.time.Instant")  # Java Instant for ts column

# ---- Minimal feeder --------------------------------------------------------
class BinanceFeeder:
    def __init__(self, name: str, symbols: List[str], writer: DynamicTableWriter):
        self.name, self.symbols, self.writer = name, [s.lower() for s in symbols], writer
        self.stop_event = Event()
        self.ws = None
        self.thread = Thread(target=self._run, daemon=True, name=f"bf:{name}")
        self.msg_count = 0
        self.last_msg_ts = None
        self.last_error = None
        self.started_at = time.time()
        self._last_emit = 0.0

    def start(self):
        # Allow restart
        if self.thread and self.thread.is_alive():
            self._emit_status(force=True)
            return "already running"
        self.stop_event.clear()
        self.started_at = time.time()
        self.last_error = None
        self.thread = Thread(target=self._run, daemon=True, name=f"bf:{self.name}")
        self.thread.start()
        self._emit_status(force=True)
        return "started"

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)
        self._emit_status(force=True)
        return "stopped"

    def _on_message(self, _ws, message: str):
        try:
            m = json.loads(message); d = m.get("data", m)  # multi-stream or single
            ts = JInstant.ofEpochMilli(int(d.get("T") or d.get("E")))
            sym = (d.get("s") or "").lower()
            self.writer.write_row(ts, sym, float(d["p"]), float(d["q"]), message)
            self.msg_count += 1
            self.last_msg_ts = ts  # JInstant
            self._emit_status()
        except Exception as e:
            print(f"[{self.name}] parse/write error: {e}")

    def _run(self):
        url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in self.symbols)
        delay = 1
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda _ws, e: print(f"[{self.name}] ws error: {e}"),
                    on_close=lambda *_: print(f"[{self.name}] ws closed"),
                    on_open=lambda *_: print(f"[{self.name}] ws open"),
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
                delay = 1   # reset delay on success
            except Exception as e:
                self.last_error = str(e)
                print(f"[{self.name}] ws error: {e}")
            finally:
                self.ws = None
                if not self.stop_event.is_set():
                    import random, time as _t
                    _t.sleep(delay + random.uniform(0, 0.5))
                    delay = min(delay * 2, 15)
                self._emit_status(force=True)

    def _emit_status(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_emit) < 1.0:
            return  # at most once per second
        self._last_emit = now
        status_writer.write_row(
            self.name,
            self.thread.is_alive(),
            ",".join(sorted(set(self.symbols))),
            int(self.msg_count),
            self.last_msg_ts,  # JInstant or None is OK
            int(now - self.started_at),
            self.last_error,
        )


# ---- App wiring ------------------------------------------------------------
app = get_app_state()

# Trades writer
writer = DynamicTableWriter({"ts": dht.Instant, "symbol": dht.string,
                             "price": dht.double, "qty": dht.double, "raw": dht.string})
binance_trades = writer.table

# NEW: Status writer (one row per event; UI will show last_by)
status_writer = DynamicTableWriter({
    "feeder": dht.string,
    "alive": dht.bool_,
    "symbols": dht.string,
    "msg_count": dht.long,
    "last_msg_ts": dht.Instant,
    "uptime_s": dht.long,
    "last_error": dht.string,
})

# the live, deduped view we’ll expose
from deephaven import agg
status_table = status_writer.table.last_by("feeder")  # latest row per feeder


_feeders: Dict[str, BinanceFeeder] = {}

def start_feeder(name: str, symbols: List[str]) -> str:
    if name in _feeders: return f"Feeder '{name}' already running."
    f = BinanceFeeder(name, symbols, writer); _feeders[name] = f; f.start()
    return f"Feeder '{name}' started."

def stop_feeder(name: str) -> str:
    f = _feeders.pop(name, None)
    if not f: return f"Feeder '{name}' not found."
    f.stop(); return f"Feeder '{name}' stopped."

def status_feeders() -> dict:
    # return {n: {"symbols": f.symbols, "alive": f.thread.is_alive()} for n, f in _feeders.items()}
    out = {}
    for n, f in _feeders.items():
        out[n] = {
            "symbols": f.symbols,
            "alive": f.thread.is_alive(),
            "msg_count": f.msg_count,
            "last_msg_ts": str(f.last_msg_ts) if f.last_msg_ts else None,
            "uptime_s": int(time.time() - f.started_at),
            "last_error": f.last_error,
        }
    return out

# Console bindings: import deepfeeder_bindings as dfb
_bind = types.ModuleType("deepfeeder_bindings")
_bind.start_feeder, _bind.stop_feeder, _bind.status_feeders = start_feeder, stop_feeder, status_feeders
# _bind.binance_trades = binance_trades
_bind.binance_trades = writer.table
_bind.status_table = status_table
_bind.status_events = status_writer.table
sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")
