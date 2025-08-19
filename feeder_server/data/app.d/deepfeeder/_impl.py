"""Implementation helpers for deepfeeder package.

Provides lazy accessors for status/config tables and provider data tables.
Lifecycle control should now be invoked directly via `ingest.manager.feeder_manager`
or the aliases exposed at the deepfeeder package level.
"""
from typing import Any, Optional

# Consolidated imports
from ingest import manager as ingest_mgr
from ingest import manager_tables as mt
import feeders.binance as binance
import feeders.tradingview as tv

# Alias to the singleton manager (still exported indirectly for advanced users)
feeder_manager = ingest_mgr.feeder_manager

# Lazy cached tables
_status_table: Optional[Any] = None
_configs_table: Optional[Any] = None
_binance_trades: Optional[Any] = None
_binance_ohlcv_1m: Optional[Any] = None
_tv_quotes: Optional[Any] = None
_tv_ohlcv_1m: Optional[Any] = None
_tv_ohlcv_5m: Optional[Any] = None

# ----------------------- Table getter helpers -----------------------


def get_status_table() -> Any:
    """Return (and cache) the feeder status table."""
    global _status_table
    if _status_table is None:
        _status_table = mt.get_status_table()
    return _status_table


def get_configs_table() -> Any:
    """Return (and cache) the live feeder configs table."""
    global _configs_table
    if _configs_table is None:
        _configs_table = mt.get_configs_table()
    return _configs_table


def get_binance_trades_table() -> Any:
    """Return (and cache) the raw Binance trades table."""
    global _binance_trades
    if _binance_trades is None:
        _binance_trades = binance.binance_trades_table()
    return _binance_trades


def get_binance_ohlcv_1m_table() -> Any:
    """Return (and cache) the derived 1m OHLCV table for Binance trades."""
    global _binance_ohlcv_1m
    if _binance_ohlcv_1m is None:
        _binance_ohlcv_1m = binance.binance_ohlcv_1m()
    return _binance_ohlcv_1m


def get_tv_quotes_table() -> Any:
    """Return (and cache) the TradingView quotes table."""
    global _tv_quotes
    if _tv_quotes is None:
        _tv_quotes = tv.tv_quotes_table()
    return _tv_quotes


def get_tv_ohlcv_1m_table() -> Any:
    """Return (and cache) the TradingView 1m OHLCV table."""
    global _tv_ohlcv_1m
    if _tv_ohlcv_1m is None:
        _tv_ohlcv_1m = tv.tv_ohlcv_1m_from_quotes()
    return _tv_ohlcv_1m


def get_tv_ohlcv_5m_table() -> Any:
    """Return (and cache) the TradingView 5m OHLCV table."""
    global _tv_ohlcv_5m
    if _tv_ohlcv_5m is None:
        _tv_ohlcv_5m = tv.tv_ohlcv_5m_from_quotes()
    return _tv_ohlcv_5m

