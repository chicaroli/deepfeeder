# fanout/schemas.py
"""
schemas.py
Schema registry and provider table imports for MarketFeeder.

Note: Only one schema per (provider, frequency) is exposed for OHLCV.
We point the canonical name (e.g. ohlcv_1m) to the *filled* table
that includes IsEmpty placeholders. The fanout layer suppresses
placeholder-only batches, so clients receive only meaningful bar
completions without needing to choose between sparse vs filled.
"""
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Optional, Dict

from providers import tv_ohlcv_5m
from providers.binance import (
    binance_trades_table,
    binance_ohlcv_1m_filled,
    binance_ohlcv_5m_filled,
)
import providers.binance.schema as binance_sch
from providers.tradingview import (
    tv_quotes_table,
    tv_ohlcv_1m,
)
import providers.tradingview.schema as tv_sch


@dataclass(frozen=True)
class SchemaSpec:
    """Defines the schema for a provider's table."""
    table_fn: Callable[[], Any]
    time_col: str
    symbol_col: str
    cols: Tuple[str, ...]
    bin_period_minutes: Optional[int]

SCHEMAS: Dict[Tuple[str, str], SchemaSpec] = {

    # Binance
    ("binance", "trades"): SchemaSpec(
        table_fn=binance_trades_table,
        time_col=binance_sch.BINANCE_TRADES_TIME_COL,
        symbol_col=binance_sch.BINANCE_TRADES_SYMBOL_COL,
        cols=binance_sch.BINANCE_TRADES_SCHEMA_COLS,
        bin_period_minutes=None,
    ),
    ("binance", "ohlcv_1m"): SchemaSpec(
        table_fn=binance_ohlcv_1m_filled,
        time_col=binance_sch.BINANCE_OHLCV_TIME_COL,
        symbol_col=binance_sch.BINANCE_OHLCV_SYMBOL_COL,
        cols=binance_sch.BINANCE_OHLCV_FILLED_SCHEMA_COLS,
        bin_period_minutes=1,
    ),
    ("binance", "ohlcv_5m"): SchemaSpec(
        table_fn=binance_ohlcv_5m_filled,
        time_col=binance_sch.BINANCE_OHLCV_TIME_COL,
        symbol_col=binance_sch.BINANCE_OHLCV_SYMBOL_COL,
        cols=binance_sch.BINANCE_OHLCV_FILLED_SCHEMA_COLS,
        bin_period_minutes=5,
    ),

    # TradingView OHLCV
    ("tradingview", "quotes"): SchemaSpec(
        table_fn=tv_quotes_table,
        time_col=tv_sch.TV_QUOTES_TIME_COL,
        symbol_col=tv_sch.TV_QUOTES_SYMBOL_COL,
        cols=tv_sch.TV_QUOTES_SCHEMA_COLS,
        bin_period_minutes=None,
    ),
    ("tradingview", "ohlcv_1m"): SchemaSpec(
        table_fn=tv_ohlcv_1m,
        time_col=tv_sch.TV_BARS_TIME_COL,
        symbol_col=tv_sch.TV_BARS_SYMBOL_COL,
        cols=tv_sch.TV_BARS_SCHEMA_COLS,
        bin_period_minutes=1,
    ),
    ("tradingview", "ohlcv_5m"): SchemaSpec(
        table_fn=tv_ohlcv_5m,
        time_col=tv_sch.TV_BARS_TIME_COL,
        symbol_col=tv_sch.TV_BARS_SYMBOL_COL,
        cols=tv_sch.TV_BARS_SCHEMA_COLS,
        bin_period_minutes=5,
    ),
}
