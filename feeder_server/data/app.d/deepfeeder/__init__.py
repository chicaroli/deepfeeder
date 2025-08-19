"""Public API for deepfeeder in Deephaven app-mode.

This module now exposes a minimal, explicit surface:

- feeder_manager: singleton FeederManager instance (start/stop/list/reload)
- get_*_table functions: access to live Deephaven tables (status, configs, provider data)
- provider/ingest/fanout/ui namespaces optionally re-exported (convenience only)

Lazy attribute indirection and lifecycle aliases (start_feeder, stop_feeder, etc.)
have been removed for clarity. Call methods directly on feeder_manager.

Usage example:

    import deepfeeder as df
    df.feeder_manager.start("binance", "default", ["btcusdt"])  # start feeder
    status_dict = df.feeder_manager.status()  # snapshot dict
    t_status = df.get_status_table()          # Deephaven live table
    all_tables = df.tables()                  # {'status': Table, 'configs': Table, ...}
"""

from ingest.manager import feeder_manager
from ingest.manager_tables import get_status_table, get_configs_table
from feeders.binance import (
    binance_trades_table as get_binance_trades_table,
    binance_ohlcv_1m as get_binance_ohlcv_1m_table,
)
from feeders.tradingview import (
    tv_quotes_table as get_tv_quotes_table,
    tv_ohlcv_1m_from_quotes as get_tv_ohlcv_1m_table,
    tv_ohlcv_5m_from_quotes as get_tv_ohlcv_5m_table,
)
from typing import Any, Dict

import feeders  # convenience namespace
import ingest   # convenience namespace
import fanout   # convenience namespace
import ui       # convenience namespace

# Lightweight alias for Deephaven table objects (avoids hard dependency at type-check time)
Table = Any

# Helper to collect common tables (evaluated at call time)
def tables() -> Dict[str, Table]:
    """Return a dict of commonly used live Deephaven tables.

    Keys: status, configs, binance_trades, binance_ohlcv_1m, tv_quotes, tv_ohlcv_1m, tv_ohlcv_5m.
    """
    return {
        "status": get_status_table(),
        "configs": get_configs_table(),
        "binance_trades": get_binance_trades_table(),
        "binance_ohlcv_1m": get_binance_ohlcv_1m_table(),
        "tv_quotes": get_tv_quotes_table(),
        "tv_ohlcv_1m": get_tv_ohlcv_1m_table(),
        "tv_ohlcv_5m": get_tv_ohlcv_5m_table(),
    }

__all__ = [
    # Tables
    "get_status_table",
    "get_configs_table",
    "get_binance_trades_table",
    "get_binance_ohlcv_1m_table",
    "get_tv_quotes_table",
    "get_tv_ohlcv_1m_table",
    "get_tv_ohlcv_5m_table",
    # Helper
    "tables",
    # Namespaces (convenience)
    "feeders",
    "ingest",
    "fanout",
    "ui",
    # Manager
    "feeder_manager",
]
