# /data/app.d/binance_controller.py
from __future__ import annotations
from abc import ABC, abstractmethod
from deephaven.appmode import get_app_state
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import json, sys, types, time
from threading import Thread, Event, Lock
from typing import Dict, List
import websocket, jpy

JInstant = jpy.get_type("java.time.Instant")  # Java Instant for ts column

# ---- Minimal feeder --------------------------------------------------------
class BaseFeeder(ABC):
    def __init__(self, provider: str, name: str, symbols: List[str]):
        self.provider = provider
        self.name = name
        self.symbols = sorted({s.lower() for s in symbols})
        self.msg_count = 0
        self.last_msg_ts = None
        self.last_error = None
        self.started_at = time.time()
        self._last_emit = 0.0

    @abstractmethod
    def start(self): ...
    @abstractmethod
    def stop(self): ...
    @abstractmethod
    def is_alive(self) -> bool: ...

    def _emit_status(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_emit) < 1.0:
            return  # throttle to 1/s
        self._last_emit = now
        status_writer.write_row(
            self.provider,
            self.name,
            self.is_alive(),
            ",".join(self.symbols),
            int(self.msg_count),
            self.last_msg_ts,
            int(now - self.started_at),
            self.last_error,
        )

class BinanceFeeder(BaseFeeder):
    def __init__(self, name: str, symbols: List[str], writer: DynamicTableWriter):
        super().__init__("binance", name, symbols)
        self.writer = writer
        self.stop_event = Event()
        self.ws = None
        self.thread = Thread(target=self._run, daemon=True, name=f"bf:{name}")

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.is_alive():
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
        if self.is_alive():
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
            self.last_msg_ts = ts
            self._emit_status()
        except Exception as e:
            self.last_error = str(e)
            self._emit_status(force=True)
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

    # def _emit_status(self, force: bool = False):
    #     now = time.time()
    #     if not force and (now - self._last_emit) < 1.0:
    #         return  # at most once per second
    #     self._last_emit = now
    #     status_writer.write_row(
    #         self.name,
    #         self.thread.is_alive(),
    #         ",".join(sorted(set(self.symbols))),
    #         int(self.msg_count),
    #         self.last_msg_ts,  # JInstant or None is OK
    #         int(now - self.started_at),
    #         self.last_error,
    #     )


# ---- App wiring ------------------------------------------------------------
app = get_app_state()

# Trades writer
writer = DynamicTableWriter({"ts": dht.Instant, "symbol": dht.string,
                             "price": dht.double, "qty": dht.double, "raw": dht.string})
binance_trades = writer.table

# NEW: Status writer (one row per event; UI will show last_by)
status_writer = DynamicTableWriter({
    "provider": dht.string,
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
status_table = status_writer.table.last_by(["provider", "feeder"])

class FeederRegistry:
    def __init__(self):
        self._lock = Lock()
        self._feeders: Dict[tuple, BaseFeeder] = {}

    def _make(self, provider: str, name: str, symbols: List[str]) -> BaseFeeder:
        provider = provider.lower()
        if provider == "binance":
            return BinanceFeeder(name, symbols, writer)
        # future: elif provider == "tradingview": return TradingViewFeeder(...)
        raise ValueError(f"Unknown provider '{provider}'")

    def start(self, provider: str, name: str, symbols: List[str]) -> str:
        key = (provider.lower(), name)
        with self._lock:
            if key in self._feeders:
                return f"Feeder '{provider}:{name}' already running."
            f = self._make(provider, name, symbols)
            self._feeders[key] = f
        return f.start() or f"Feeder '{provider}:{name}' started."

    def stop(self, provider: str, name: str) -> str:
        key = (provider.lower(), name)
        with self._lock:
            f = self._feeders.pop(key, None)
        if not f:
            return f"Feeder '{provider}:{name}' not found."
        return f.stop() or f"Feeder '{provider}:{name}' stopped."

    def update_symbols(self, provider: str, name: str, symbols: List[str]) -> str:
        # simplest: stop/start with new symbols
        self.stop(provider, name)
        return self.start(provider, name, symbols)

    def status(self) -> dict:
        with self._lock:
            items = list(self._feeders.items())
        out = {}
        for (prov, name), f in items:
            out[f"{prov}:{name}"] = {
                "provider": prov,
                "name": name,
                "symbols": f.symbols,
                "alive": f.is_alive(),
                "msg_count": f.msg_count,
                "last_msg_ts": str(f.last_msg_ts) if f.last_msg_ts else None,
                "uptime_s": int(time.time() - f.started_at),
                "last_error": f.last_error,
            }
        return out

REG = FeederRegistry()

# _feeders: Dict[str, BinanceFeeder] = {}

def start_feeder(provider: str, name: str, symbols: List[str]) -> str:
    return REG.start(provider, name, symbols)

def start_feeder_csv(provider: str, name: str, symbols_csv: str) -> str:
    symbols = [s.strip().lower() for s in symbols_csv.split(",") if s.strip()]
    return REG.start(provider, name, symbols)

def stop_feeder(provider: str, name: str) -> str:
    return REG.stop(provider, name)

def update_symbols(provider: str, name: str, symbols: List[str]) -> str:
    return REG.update_symbols(provider, name, symbols)

def status_feeders() -> dict:
    return REG.status()

# Bind ONLY tables and functions you want; avoid top-level vars in Panels
_bind = types.ModuleType("deepfeeder_bindings")
_bind.start_feeder = start_feeder
_bind.start_feeder_csv = start_feeder_csv
_bind.stop_feeder = stop_feeder
_bind.update_symbols = update_symbols
_bind.status_feeders = status_feeders
_bind.binance_trades = writer.table
_bind.status_table = status_table     # live latest per (provider, feeder)
sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")
