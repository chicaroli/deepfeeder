"""
schemas.py
Schema registry and provider table imports for MarketFeeder.
"""
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Optional, Dict

from feeders.binance import binance_trades_table, binance_ohlcv_1m
import feeders.binance.schema as binance_sch
from feeders.tradingview import tv_quotes_table, tv_ohlcv_1m_from_quotes, tv_ohlcv_5m_from_quotes
import feeders.tradingview.schema as tv_sch


@dataclass(frozen=True)
class SchemaSpec:
    """Defines the schema for a provider's table."""
    table_fn: Callable[[], Any]
    time_col: str
    symbol_col: str
    cols: Tuple[str, ...]
    bin_period_minutes: Optional[int]

SCHEMAS: Dict[Tuple[str, str], SchemaSpec] = {
    # Binance schemas via provider-owned metadata constants
    ("binance", "ohlcv_1m"): SchemaSpec(
        table_fn=binance_ohlcv_1m,
        time_col=binance_sch.BINANCE_OHLCV_1M_TIME_COL,
        symbol_col=binance_sch.BINANCE_OHLCV_1M_SYMBOL_COL,
        cols=binance_sch.BINANCE_OHLCV_1M_SCHEMA_COLS,
        bin_period_minutes=1,
    ),
    ("binance", "trades"): SchemaSpec(
        table_fn=binance_trades_table,
        time_col=binance_sch.BINANCE_TRADES_TIME_COL,
        symbol_col=binance_sch.BINANCE_TRADES_SYMBOL_COL,
        cols=binance_sch.BINANCE_TRADES_SCHEMA_COLS,
        bin_period_minutes=None,
    ),
    # TradingView schemas via provider-owned metadata constants
    ("tradingview", "quotes"): SchemaSpec(
        table_fn=tv_quotes_table,
        time_col=tv_sch.TV_QUOTES_TIME_COL,
        symbol_col=tv_sch.TV_QUOTES_SYMBOL_COL,
        cols=tv_sch.TV_QUOTES_SCHEMA_COLS,
        bin_period_minutes=None,
    ),
    ("tradingview", "ohlcv_1m"): SchemaSpec(
        table_fn=tv_ohlcv_1m_from_quotes,
        time_col=tv_sch.TV_OHLCV_TIME_COL,
        symbol_col=tv_sch.TV_OHLCV_SYMBOL_COL,
        cols=tv_sch.TV_OHLCV_1M_SCHEMA_COLS,
        bin_period_minutes=1,
    ),
    ("tradingview", "ohlcv_5m"): SchemaSpec(
        table_fn=tv_ohlcv_5m_from_quotes,
        time_col=tv_sch.TV_OHLCV_TIME_COL,
        symbol_col=tv_sch.TV_OHLCV_SYMBOL_COL,
        cols=tv_sch.TV_OHLCV_5M_SCHEMA_COLS,
        bin_period_minutes=5,
    ),
}
