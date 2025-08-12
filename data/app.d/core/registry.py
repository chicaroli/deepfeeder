# app.d/core/registry.py
from threading import Lock
from typing import Dict, Tuple, List
from core.base import BaseFeeder
from providers.binance_feeder import BinanceFeeder
import time

class FeederRegistry:
    def __init__(self):
        self._lock = Lock()
        self._feeders: Dict[Tuple[str, str], BaseFeeder] = {}

    def _make(self, provider: str, name: str, symbols: List[str]) -> BaseFeeder:
        p = provider.lower()
        if p == "binance":
            return BinanceFeeder(name, symbols)
        raise ValueError(f"Unknown provider '{provider}'")

    def start(self, provider: str, name: str, symbols: List[str]) -> str:
        key = (provider.lower(), name)
        with self._lock:
            if key in self._feeders:
                f = self._feeders[key]
                f.emit_status(force=True)
                return f"Feeder '{provider}:{name}' already running."
            f = self._make(provider, name, symbols)
            self._feeders[key] = f
        return f.start()

    def stop(self, provider: str, name: str) -> str:
        key = (provider.lower(), name)
        with self._lock:
            f = self._feeders.pop(key, None)
        if not f:
            return f"Feeder '{provider}:{name}' not found."
        return f.stop()

    def update_symbols(self, provider: str, name: str, symbols: List[str]) -> str:
        self.stop(provider, name)
        return self.start(provider, name, symbols)

    def status(self) -> dict:
        with self._lock:
            items = list(self._feeders.items())
        out = {}
        for (prov, name), f in items:
            out[f"{prov}:{name}"] = {
                "provider": prov, "name": name, "symbols": f.symbols,
                "alive": f.is_alive(), "msg_count": f.msg_count,
                "last_msg_ts": str(f.last_msg_ts) if f.last_msg_ts else None,
                "uptime_s": int(time.time() - f.started_at),
                "last_error": f.last_error,
            }
        return out

REGISTRY = FeederRegistry()
