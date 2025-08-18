"""
schemas.py
Schema registry and provider table imports for MarketFeeder.
"""
from dataclasses import dataclass
from typing import Callable, Any, Tuple, Optional, Dict

from providers.binance_schema import binance_trades_table, binance_ohlcv_1m
from providers.tradingview_schema import tv_quotes_table, tv_ohlcv_1m_from_quotes, tv_ohlcv_5m_from_quotes

@dataclass(frozen=True)
class SchemaSpec:
    """Defines the schema for a provider's table."""
    table_fn: Callable[[], Any]
    time_col: str
    symbol_col: str
    cols: Tuple[str, ...]
    bin_period_minutes: Optional[int]

SCHEMAS: Dict[Tuple[str, str], SchemaSpec] = {
    ("binance", "ohlcv_1m"): SchemaSpec(
        table_fn=binance_ohlcv_1m,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Trades", "Vwap",
              "BuyerMakerCount"),
        bin_period_minutes=1,
    ),
    ("binance", "trades"): SchemaSpec(
        table_fn=binance_trades_table,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "TradeId", "Price", "Quantity", "BuyerID", "SellerID", "IsBuyerMaker"),
        bin_period_minutes=None,
    ),
    ("tradingview", "quotes"): SchemaSpec(
        table_fn=tv_quotes_table,
        time_col="LpTime",
        symbol_col="Symbol",
        cols=("LpTime", "Symbol", "LastPrice", "Bid", "Ask", "Volume", "Change", "ChangePct", "VolDelta"),
        bin_period_minutes=None,
    ),
    ("tradingview", "ohlcv_1m"): SchemaSpec(
        table_fn=tv_ohlcv_1m_from_quotes,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap"),
        bin_period_minutes=1,
    ),
    ("tradingview", "ohlcv_5m"): SchemaSpec(
        table_fn=tv_ohlcv_5m_from_quotes,
        time_col="Timestamp",
        symbol_col="Symbol",
        cols=("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap"),
        bin_period_minutes=5,
    ),
}

