"""Public API for deepfeeder in Deephaven app-mode.

Backwards-compatible flat function API plus convenient access to top-level
modules (feeders, ingest, fanout, ui). Avoids circular imports by using
absolute imports of sibling packages instead of relative re-exports.
"""

from deepfeeder._impl import (
    get_status_table,
    get_configs_table,
    get_binance_trades_table,
    get_binance_ohlcv_1m_table,
    get_tv_quotes_table,
    get_tv_ohlcv_1m_table,
    get_tv_ohlcv_5m_table,
    start_feeder,
    stop_feeder,
    stop_all,
    start_all,
    list_configs,
    reload_configs,
    start_all_autostart,
)

# Absolute imports of sibling top-level packages (no relative import -> no circular)
import feeders  # provider implementations & schemas
import ingest   # feeder manager / lifecycle
import fanout   # fanout layer schemas & listener
import ui       # UI components

__all__ = [
    # Flat API
    "get_status_table",
    "get_configs_table",
    "get_binance_trades_table",
    "get_binance_ohlcv_1m_table",
    "get_tv_quotes_table",
    "get_tv_ohlcv_1m_table",
    "get_tv_ohlcv_5m_table",
    "start_feeder",
    "stop_feeder",
    "stop_all",
    "start_all",
    "list_configs",
    "reload_configs",
    "start_all_autostart",
    # Namespaces
    "feeders",
    "ingest",
    "fanout",
    "ui",
]

# Backwards-compatible function alias
def configs_list(*args, **kwargs):
    """Compatibility wrapper for `list_configs()` exposing the older name used
    in UI code.
    """
    return list_configs(*args, **kwargs)

# Lazy attribute mapping for module-level attribute access used by UI/dashboard.
_ATTR_LAZY_MAP = {
    "status_table": get_status_table,
    "configs_live_table": get_configs_table,
    "binance_trades": get_binance_trades_table,
    "binance_ohlcv_1m": get_binance_ohlcv_1m_table,
    "tv_quotes": get_tv_quotes_table,
    "tv_ohlcv_1m": get_tv_ohlcv_1m_table,
    "tv_ohlcv_5m": get_tv_ohlcv_5m_table,
}


def __getattr__(name: str):
    """Provide lazy module attributes for backwards compatibility.

    Accessing these attributes will call the corresponding getter and return
    its result. This avoids constructing heavy tables at import time.
    """
    if name in _ATTR_LAZY_MAP:
        return _ATTR_LAZY_MAP[name]()
    if name == "configs_list":
        return configs_list
    raise AttributeError(f"module {__name__} has no attribute {name}")


def __dir__():
    """Include lazy attribute names in module dir()."""
    return sorted(list(globals().keys()) + list(_ATTR_LAZY_MAP.keys()) + ["configs_list"])
