"""Implementation helpers for deepfeeder package.

Expose a minimal, lazy API that is safe to import in Deephaven app-mode
without performing expensive side-effects at import time.
"""
from typing import Any, List, Optional

from core.registry import REGISTRY
from core.bus import get_status_table as _get_status_table, get_configs_table as _get_configs_table
from providers.binance_schema import binance_trades_table, binance_ohlcv_1m
from providers.tradingview_schema import (
    tv_quotes_table,
    tv_ohlcv_1m_from_quotes,
    tv_ohlcv_5m_from_quotes,
)

# Lazy cached tables
_status_table: Optional[Any] = None
_configs_table: Optional[Any] = None
_binance_trades: Optional[Any] = None
_binance_ohlcv_1m: Optional[Any] = None
_tv_quotes: Optional[Any] = None
_tv_ohlcv_1m: Optional[Any] = None
_tv_ohlcv_5m: Optional[Any] = None


def get_status_table() -> Any:
    """Return (and cache) the status table from the bus."""
    global _status_table
    if _status_table is None:
        _status_table = _get_status_table()
    return _status_table


def get_configs_table() -> Any:
    """Return (and cache) the live configs table from the bus."""
    global _configs_table
    if _configs_table is None:
        _configs_table = _get_configs_table()
    return _configs_table


def get_binance_trades_table() -> Any:
    """Return a table with Binance trades (constructed lazily)."""
    global _binance_trades
    if _binance_trades is None:
        _binance_trades = binance_trades_table()
    return _binance_trades


def get_binance_ohlcv_1m_table() -> Any:
    """Return derived 1m OHLCV for Binance (lazily)."""
    global _binance_ohlcv_1m
    if _binance_ohlcv_1m is None:
        _binance_ohlcv_1m = binance_ohlcv_1m()
    return _binance_ohlcv_1m


def get_tv_quotes_table() -> Any:
    """Return TradingView quotes table (lazily)."""
    global _tv_quotes
    if _tv_quotes is None:
        _tv_quotes = tv_quotes_table()
    return _tv_quotes


def get_tv_ohlcv_1m_table() -> Any:
    """Return TradingView 1m OHLCV derived from quotes (lazily)."""
    global _tv_ohlcv_1m
    if _tv_ohlcv_1m is None:
        _tv_ohlcv_1m = tv_ohlcv_1m_from_quotes()
    return _tv_ohlcv_1m


def get_tv_ohlcv_5m_table() -> Any:
    """Return TradingView 5m OHLCV derived from quotes (lazily)."""
    global _tv_ohlcv_5m
    if _tv_ohlcv_5m is None:
        _tv_ohlcv_5m = tv_ohlcv_5m_from_quotes()
    return _tv_ohlcv_5m


# Registry/control helpers


def start_feeder(provider: str, name: str, symbols: List[str]) -> Any:
    """Start a feeder via the REGISTRY."""
    return REGISTRY.start(provider, name, symbols)


def stop_feeder(provider: str, name: str) -> Any:
    """Stop a feeder via the REGISTRY."""
    return REGISTRY.stop(provider, name)


def stop_all() -> Any:
    """Stop all feeders."""
    return REGISTRY.stop_all()


def start_all() -> Any:
    """Start all configured feeders."""
    return REGISTRY.start_all()


def list_configs() -> List[dict]:
    """Return the list of configurations."""
    return REGISTRY.list_configs()


def reload_configs() -> Any:
    """Reload configurations from storage."""
    return REGISTRY.reload_configs()


def start_all_autostart() -> Any:
    """Start only feeders marked for autostart.

    This mirrors the previous behaviour of calling REGISTRY.start_all_autostart()
    from app.py but makes the action explicit and import-safe.
    """
    return REGISTRY.start_all_autostart()
