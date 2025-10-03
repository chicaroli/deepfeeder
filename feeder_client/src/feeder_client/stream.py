"""
feeder_client.stream: Stream configuration helpers for DeepFeeder.

Contains:
    - Stream: Stream configuration dataclass with helpers for bars and trades.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Iterable


@dataclass(frozen=True, slots=True)
class Stream:
    """
    Stream configuration for a DeepFeeder data stream.

    Attributes:
        provider: Data provider name (e.g., 'binance').
        schema: Data schema (e.g., 'ohlcv_1m', 'trades').
        symbol: Market symbol (e.g., 'BTCUSD').
        exchange: Optional exchange name.
        fields: Tuple of field names to request.
        only_completed: If True, only completed bars are sent.
    """

    provider: str
    schema: str
    symbol: str
    exchange: Optional[str] = None
    fields: tuple[str, ...] = field(default_factory=tuple)
    only_completed: Optional[bool] = None

    # --- helpers -------------------------------------------------------------

    @staticmethod
    def bars(
        provider: str,
        schema: str,
        symbol: str,
        *,
        exchange: Optional[str] = None,
        fields: Iterable[str] = ("Timestamp", "Open", "High", "Low", "Close", "Volume", "BarId"),
        only_completed: bool | None = True,
    ) -> Stream:
        """
        Create a Stream configuration for OHLCV bars.

        Args:
            provider: Data provider name.
            schema: Data schema name.
            symbol: Market symbol.
            exchange: Optional exchange name.
            fields: Iterable of field names for the OHLCV data.
            only_completed: If True, only completed bars are included.

        Returns:
            Stream: Configured Stream instance for bars.
        """
        return Stream(
            provider=provider,
            schema=schema,
            symbol=symbol,
            exchange=exchange,
            fields=tuple(fields),
            only_completed=only_completed,
        )

    @staticmethod
    def trades(
        provider: str,
        symbol: str,
        *,
        exchange: Optional[str] = None,
        fields: Iterable[str] = ("ts", "price", "qty", "trade_id"),
    ) -> Stream:
        """
        Create a Stream configuration for trades.

        Args:
            provider: Data provider name.
            symbol: Market symbol.
            exchange: Optional exchange name.
            fields: Iterable of field names for the trade data.

        Returns:
            Stream: Configured Stream instance for trades.
        """
        return Stream(
            provider=provider,
            schema="trades",
            symbol=symbol,
            exchange=exchange,
            fields=tuple(fields),
            only_completed=None,
        )

    @property
    def key(self) -> str:
        """Stable key for registries / dicts / logs."""
        ex = f"@{self.exchange}" if self.exchange else ""
        return f"{self.provider}/{self.schema}:{self.symbol}{ex}"

    def to_subscribe_kwargs(self) -> dict:
        """Translate to DeepFeederClient.subscribe(**kwargs)."""
        return {
            "provider": self.provider,
            "schema": self.schema,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "fields": ",".join(self.fields) if self.fields else None,
            "only_completed": self.only_completed,
        }
