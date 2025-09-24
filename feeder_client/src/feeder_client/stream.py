# feeder_client/src/feeder_client/stream.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Iterable


@dataclass(frozen=True, slots=True)
class Stream:
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
