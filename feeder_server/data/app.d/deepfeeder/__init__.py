"""Public API for deepfeeder in Deephaven app-mode.

This package exposes a minimal, documented surface that is safe to import in
Deephaven app-mode. Importing this package does not start feeders or perform
expensive work — callers must explicitly call control functions such as
``start_feeder`` or ``start_all_autostart`` to perform lifecycle actions.

The module exports both function-style APIs (recommended) and provides
backwards-compatible module attribute aliases for existing code that used
``deepfeeder_bindings`` (for example ``status_table``, ``configs_live_table``,
``binance_trades``, ...). Attribute access is lazy and will only construct
heavy tables when requested.
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

__all__ = [
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
]

# Backwards-compatible function alias for older code that used configs_list
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
