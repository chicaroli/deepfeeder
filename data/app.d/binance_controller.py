# /data/app.d/binance_controller.py
from __future__ import annotations

from deephaven.appmode import ApplicationState, get_app_state
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

from dataclasses import dataclass
from threading import Thread, Event, Lock
from typing import Dict, List
import json, time, traceback
import websocket
import sys, types
import pandas as pd
from deephaven import pandas as dhpd
import jpy

# Java Instant for timestamp column
JInstant = jpy.get_type("java.time.Instant")


# -------------------- Feeder --------------------
class BinanceTradeFeeder:
    def __init__(self, name: str, symbols: List[str], dt_writer: DynamicTableWriter):
        self.name = name
        self.symbols = [s.lower() for s in symbols]
        self.dt_writer = dt_writer
        self.stop_event = Event()
        self.thread = Thread(target=self._run, name=f"Feeder-{name}", daemon=True)
        self.ws = None

    def start(self):
        if not self.thread.is_alive():
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass
        if self.thread.is_alive():
            self.thread.join(timeout=5)

    def _on_message(self, _ws, message: str):
        try:
            msg = json.loads(message)
            data = msg.get("data", msg)  # handle multi-stream or single
            symbol = (data.get("s") or data.get("symbol") or "").lower()
            price = float(data.get("p") or data.get("price"))
            qty = float(data.get("q") or data.get("quantity"))
            ts_ms = int(data.get("T") or data.get("E"))
            ts = JInstant.ofEpochMilli(ts_ms)  # real Java Instant
            self.dt_writer.write_row(ts, symbol, price, qty, message)
        except Exception:
            traceback.print_exc()

    def _on_error(self, _ws, err):
        print(f"[{self.name}] WS error:", err)

    def _on_close(self, _ws, _a, _b):
        print(f"[{self.name}] WS closed")

    def _run(self):
        streams = "/".join([f"{s}@trade" for s in self.symbols])
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception:
                traceback.print_exc()
            finally:
                self.ws = None
            if not self.stop_event.is_set():
                time.sleep(1)


# -------------------- Controller --------------------
@dataclass
class FeederInfo:
    name: str
    kind: str
    symbols: List[str]

class FeederController:
    def __init__(self, writer: DynamicTableWriter):
        self._lock = Lock()
        self._registry: Dict[str, BinanceTradeFeeder] = {}
        self._meta: Dict[str, FeederInfo] = {}
        self._writer = writer

    def start_binance(self, name: str, symbols: List[str]) -> str:
        with self._lock:
            if name in self._registry:
                return f"Feeder '{name}' already running."
            feeder = BinanceTradeFeeder(name=name, symbols=symbols, dt_writer=self._writer)
            self._registry[name] = feeder
            self._meta[name] = FeederInfo(name=name, kind="binance_trade", symbols=symbols)
            feeder.start()
            return f"Feeder '{name}' started."

    def stop(self, name: str) -> str:
        with self._lock:
            feeder = self._registry.pop(name, None)
            self._meta.pop(name, None)
        if feeder is None:
            return f"Feeder '{name}' not found."
        feeder.stop()
        return f"Feeder '{name}' stopped."

    def stop_all(self) -> list[str]:
        with self._lock:
            names = list(self._registry.keys())
        msgs = []
        for n in names:
            msgs.append(self.stop(n))
        return msgs

    def status(self) -> dict:
        with self._lock:
            return {
                name: {
                    "kind": self._meta[name].kind,
                    "symbols": list(self._meta[name].symbols),
                    "alive": self._registry[name].thread.is_alive(),
                }
                for name in self._registry.keys()
            }


# -------------------- App entry --------------------
def start(app: ApplicationState):
    # Live output table (always visible)
    writer = DynamicTableWriter({
        "ts": dht.Instant,
        "symbol": dht.string,
        "price": dht.double,
        "qty": dht.double,
        "raw": dht.string,
    })
    trades = writer.table
    app["binance_trades"] = trades

    controller = FeederController(writer=writer)

    # Control functions
    def start_feeder(name: str, symbols: list[str]):
        return controller.start_binance(name=name, symbols=symbols)

    def stop_feeder(name: str):
        return controller.stop(name)

    def stop_all_feeders():
        return controller.stop_all()

    def status_feeders() -> dict:
        return controller.status()

    def build_status_table():
        st = status_feeders()
        rows = [
            {"name": n, "kind": v["kind"], "symbols": ",".join(v["symbols"]), "alive": v["alive"]}
            for n, v in st.items()
        ]
        app["binance_status"] = dhpd.to_table(pd.DataFrame(rows, columns=["name","kind","symbols","alive"]))
        return "binance_status"

    # Export to Applications panel (your DH version shows these under Applications)
    app["start_feeder"] = start_feeder
    app["stop_feeder"] = stop_feeder
    app["stop_all_feeders"] = stop_all_feeders
    app["status_feeders"] = status_feeders
    app["build_status_table"] = build_status_table

    # Console bindings (always available)
    _bindings = types.ModuleType("deepfeeder_bindings")
    _bindings.start_feeder = start_feeder
    _bindings.stop_feeder = stop_feeder
    _bindings.stop_all_feeders = stop_all_feeders
    _bindings.status_feeders = status_feeders
    _bindings.build_status_table = build_status_table
    _bindings.binance_trades = trades
    sys.modules["deepfeeder_bindings"] = _bindings
    print("[deepfeeder] bindings module installed: import deepfeeder_bindings as dfb")

    # Optional autostart
    AUTOSTART: list[tuple[str, list[str]]] = [
        # ("btc_only", ["btcusdt"]),
    ]
    for n, syms in AUTOSTART:
        try:
            print(start_feeder(n, syms))
        except Exception:
            traceback.print_exc()


# Standard Script-Application bootstrap
def initialize(func):
    app = get_app_state()
    func(app)

initialize(start)
