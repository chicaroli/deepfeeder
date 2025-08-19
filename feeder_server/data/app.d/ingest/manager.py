# app.d/ingest/manager.py
from __future__ import annotations

import json
import os
import tempfile
import time
from threading import Lock
from typing import Dict, Tuple, List, TypedDict, Optional

# updated imports to top-level feeders layout
from feeders.base import BaseFeeder
from feeders.binance import BinanceFeeder
from feeders.tradingview import TradingViewFeeder
from .utils import normalize_symbols
from .manager_tables import get_configs_writer

from deephaven.time import to_j_instant
from datetime import datetime, timezone

DEFAULT_CONFIG_PATH = "/data/storage/notebooks/feeders.json"


class FeederCfg(TypedDict):
    provider: str
    name: str
    symbols: List[str]
    autostart: bool


def _key(provider: str, name: str) -> Tuple[str, str]:
    return (provider.lower(), name)


class FeederManager:
    """
    Manages running feeders in-memory and persists feeder configs to JSON.
    Also mirrors configs into a live Deephaven table (via ingest.bus) for the UI.
    """

    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        self._lock = Lock()
        self._feeders: Dict[Tuple[str, str], BaseFeeder] = {}
        self._configs: Dict[Tuple[str, str], FeederCfg] = {}
        self._config_path = config_path

        # Live configs writer (for UI)
        self._cfg_writer = get_configs_writer()
        self._load_configs()

    # ----------------- helpers: live configs topic -----------------

    def _emit_cfg_row(
        self,
        provider: str,
        name: str,
        symbols_csv: str,
        autostart: bool,
        deleted: bool = False,
    ) -> None:
        """Emit a snapshot row of a config into the live configs topic."""
        now = to_j_instant(datetime.now(timezone.utc))
        self._cfg_writer.write_row(
            provider.lower(),
            name,
            symbols_csv,
            bool(autostart),
            bool(deleted),
            now,
        )

    # ----------------- persistence -----------------

    def _load_configs(self) -> None:
        with self._lock:
            self._configs.clear()
            os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
            if not os.path.exists(self._config_path):
                return
            with open(self._config_path, "r", encoding="utf-8") as f:
                arr = json.load(f) or []

            for item in arr:
                provider = str(item.get("provider", "")).lower().strip()
                name = str(item.get("name", "")).strip()
                symbols = normalize_symbols(item.get("symbols", []))
                autostart = bool(item.get("autostart", False))
                if provider and name:
                    self._configs[_key(provider, name)] = {
                        "provider": provider,
                        "name": name,
                        "symbols": symbols,
                        "autostart": autostart,
                    }
                    # publish to live configs table
                    self._emit_cfg_row(provider, name, ",".join(symbols), autostart, deleted=False)

    def _save_configs(self) -> None:
        with self._lock:
            snapshot = [
                {
                    "provider": p,
                    "name": n,
                    "symbols": cfg["symbols"],
                    "autostart": cfg["autostart"],
                }
                for (p, n), cfg in sorted(self._configs.items())
            ]

        os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix="feeders.", suffix=".json", dir=os.path.dirname(self._config_path)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self._config_path)
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

    # ----------------- factory -----------------

    def _make(self, provider: str, name: str, symbols: List[str]) -> BaseFeeder:
        p = provider.lower()
        if p == "binance":
            return BinanceFeeder(name, symbols)
        elif p == "tradingview":
            return TradingViewFeeder(name, symbols)
        # future providers:
        # ...
        raise ValueError(f"Unknown provider '{provider}'")

    # ----------------- config CRUD -----------------

    def upsert_config(
        self, provider: str, name: str, symbols: List[str], autostart: bool = False
    ) -> str:
        provider = provider.lower().strip()
        name = name.strip()
        syms = normalize_symbols(symbols)
        if not provider or not name:
            return "Provider and name are required."
        if not syms:
            return "Symbols list is empty."

        key = _key(provider, name)
        with self._lock:
            self._configs[key] = {
                "provider": provider,
                "name": name,
                "symbols": syms,
                "autostart": bool(autostart),
            }
            self._save_configs()

        # mirror to live configs table
        self._emit_cfg_row(provider, name, ",".join(syms), autostart, deleted=False)
        return f"Config '{provider}:{name}' saved."

    def remove_config(self, provider: str, name: str) -> str:
        provider = provider.lower()
        key = _key(provider, name)
        with self._lock:
            existed = self._configs.pop(key, None) is not None
            self._save_configs()

        if existed:
            # mark deleted
            self._emit_cfg_row(provider, name, "", False, deleted=True)
            # also stop if running
            self.stop(provider, name)
            return f"Config '{provider}:{name}' removed."
        return f"Config '{provider}:{name}' not found."

    def list_configs(self) -> List[FeederCfg]:
        with self._lock:
            return [v.copy() for v in self._configs.values()]

    def get_config(self, provider: str, name: str) -> Optional[FeederCfg]:
        with self._lock:
            return self._configs.get(_key(provider, name))

    def reload_configs(self) -> str:
        self._load_configs()
        return f"Loaded {len(self._configs)} configs from disk."

    # ----------------- runtime control -----------------

    def start(self, provider: str, name: str, symbols: Optional[List[str]] = None) -> str:
        """
        Start a feeder. If symbols is None or empty, use the saved config.
        """
        provider_l = provider.lower()
        key = _key(provider_l, name)

        with self._lock:
            if key in self._feeders:
                f = self._feeders[key]
                try:
                    f.emit_status(force=True)
                except Exception:
                    pass
                return f"Feeder '{provider_l}:{name}' already running."

            use_symbols = normalize_symbols(symbols or [])
            if not use_symbols:
                cfg = self._configs.get(key)
                if not cfg:
                    return f"No config for '{provider_l}:{name}'."
                use_symbols = cfg["symbols"]

            f = self._make(provider_l, name, use_symbols)
            self._feeders[key] = f

        return f.start()

    def stop(self, provider: str, name: str) -> str:
        key = _key(provider.lower(), name)
        with self._lock:
            f = self._feeders.pop(key, None)
        if not f:
            return f"Feeder '{key[0]}:{key[1]}' not found."
        return f.stop()

    def stop_all(self) -> str:
        with self._lock:
            keys = list(self._feeders.keys())
        stopped = 0
        for p, n in keys:
            res = self.stop(p, n)
            if res.endswith("stopped") or res.endswith("stopped."):
                stopped += 1
        return f"Stopped {stopped} feeders."

    def update_symbols(self, provider: str, name: str, symbols: List[str]) -> str:
        syms = normalize_symbols(symbols)
        self.stop(provider, name)
        key = _key(provider.lower(), name)
        with self._lock:
            if key in self._configs:
                self._configs[key]["symbols"] = syms
                self._save_configs()
        # reflect change in live table
        self._emit_cfg_row(key[0], name, ",".join(syms), self._configs.get(key, {}).get("autostart", False), deleted=False)
        return self.start(provider, name, syms)

    def start_all_autostart(self) -> str:
        with self._lock:
            items = list(self._configs.values())
        started = 0
        for cfg in items:
            if cfg.get("autostart"):
                res = self.start(cfg["provider"], cfg["name"])
                if "started" in res:
                    started += 1
        return f"Started {started} feeders."

    def start_all(self) -> str:
        with self._lock:
            items = list(self._configs.values())
        started = 0
        for cfg in items:
            res = self.start(cfg["provider"], cfg["name"])
            if "started" in str(res).lower():
                started += 1
        return f"Started {started} feeders."

    def status(self) -> dict:
        with self._lock:
            items = list(self._feeders.items())
        out = {}
        now = time.time()
        for (prov, name), f in items:
            out[f"{prov}:{name}"] = {
                "provider": prov,
                "name": name,
                "symbols": list(getattr(f, "symbols", [])),
                "alive": f.is_alive(),
                "msg_count": getattr(f, "msg_count", None),
                "last_msg_ts": str(getattr(f, "last_msg_ts", None)) if getattr(f, "last_msg_ts", None) else None,
                "uptime_s": int(now - getattr(f, "started_at", now)),
                "last_error": getattr(f, "last_error", None),
            }
        return out


# Singleton instance
feeder_manager = FeederManager()
