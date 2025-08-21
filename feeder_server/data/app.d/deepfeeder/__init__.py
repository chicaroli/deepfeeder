"""Public API for deepfeeder in Deephaven app-mode.

Provides:
- services container (lazy singletons)
- feeder_manager lazy proxy (access methods directly)
- live table getters (status, configs, provider data, threads)
"""
from __future__ import annotations
from typing import Any, Dict

from ingest.manager import FeederManager
from ingest.manager_tables import get_status_table, get_configs_table
from feeders.binance import (
    binance_trades_table as get_binance_trades_table,
    binance_ohlcv_1m as get_binance_ohlcv_1m_table,
    binance_ohlcv_1m_filled as get_binance_ohlcv_1m_filled_table,
    binance_ohlcv_5m as get_binance_ohlcv_5m_table,
    binance_ohlcv_5m_filled as get_binance_ohlcv_5m_filled_table,
)
from feeders.tradingview import (
    tv_quotes_table as get_tv_quotes_table,
    tv_ohlcv_1m_from_quotes as get_tv_ohlcv_1m_table,
    tv_ohlcv_5m_from_quotes as get_tv_ohlcv_5m_table,
    tv_ohlcv_1m_filled as get_tv_ohlcv_1m_filled_table,
    tv_ohlcv_5m_filled as get_tv_ohlcv_5m_filled_table,
)
from feeders.bins import bins_today
from fanout import get_fanout_stats_table as fanout_get_stats_table
from runtime.threads_bus import get_threads_table
from runtime.eventlog_bus import get_eventlog_table
from runtime.services import Services

import feeders  # convenience namespace
import ingest   # convenience namespace
import fanout   # convenience namespace
import ui       # convenience namespace

# --- services container ----------------------------------------------------
services = Services()
services.register("feeder_manager", lambda: FeederManager())

def get_service(name: str):
    """Retrieve a service instance by name (lazy)."""
    return services.get(name)

class _ServiceProxy:
    """Lazy proxy to a named service in the container."""
    def __init__(self, service_name: str):
        self._service_name = service_name
    def _resolve(self):
        return services.get(self._service_name)
    def __getattr__(self, item: str):  # delegate attribute access
        return getattr(self._resolve(), item)
    def __repr__(self) -> str:  # pragma: no cover - representational
        return f"<ServiceProxy {self._service_name} -> {self._resolve()!r}>"

# Lazy feeder manager instance
feeder_manager = _ServiceProxy("feeder_manager")

# Lightweight alias for Deephaven table objects
Table = Any

# --- table helpers ---------------------------------------------------------

def tables() -> Dict[str, Table]:
    """Return dict of commonly used live Deephaven tables."""
    return {
        "status": get_status_table(),
        "configs": get_configs_table(),
        "threads": get_threads_table(),
        "eventlog": get_eventlog_table(),
        "binance_trades": get_binance_trades_table(),
        "binance_ohlcv_1m": get_binance_ohlcv_1m_table(),
        "binance_ohlcv_1m_filled": get_binance_ohlcv_1m_filled_table(),
        "binance_ohlcv_5m": get_binance_ohlcv_5m_table(),
        "binance_ohlcv_5m_filled": get_binance_ohlcv_5m_filled_table(),
        "tv_quotes": get_tv_quotes_table(),
        "tv_ohlcv_1m": get_tv_ohlcv_1m_table(),
        "tv_ohlcv_1m_filled": get_tv_ohlcv_1m_filled_table(),
        "tv_ohlcv_5m": get_tv_ohlcv_5m_table(),
        "tv_ohlcv_5m_filled": get_tv_ohlcv_5m_filled_table(),
        "bins_1m_today": bins_today(1),
        "bins_5m_today": bins_today(5),
    }

# Convenience re-export
get_fanout_stats_table = fanout_get_stats_table  # type: ignore

__all__ = [
    # Services API
    "services", "get_service", "feeder_manager",
    # Table getters
    "get_status_table", "get_configs_table", "get_threads_table",
    "get_binance_trades_table", "get_binance_ohlcv_1m_table", "get_binance_ohlcv_1m_filled_table",
    "get_binance_ohlcv_5m_table", "get_binance_ohlcv_5m_filled_table",
    "get_tv_quotes_table", "get_tv_ohlcv_1m_table", "get_tv_ohlcv_1m_filled_table",
    "get_tv_ohlcv_5m_table", "get_tv_ohlcv_5m_filled_table",
    # Helpers
    "tables", "get_fanout_stats_table", "bins_today",
    # Namespaces
    "feeders", "ingest", "fanout", "ui",
    # Event log
    "get_eventlog_table",
]
