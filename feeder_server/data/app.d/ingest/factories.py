# ingest/factories.py
from __future__ import annotations
from core.contracts import EventBus, Producer
from .feeder_specs import FeederSpec
from providers.binance.producer_ws import BinanceWsProducer
from providers.tradingview.producer_ws import TradingViewWsProducer
from providers.tradingview.producer_api import TradingViewApiProducer
from sinks.registry import WriterRegistry
import providers.tradingview as tv
import providers.binance as bn

PRODUCER_REGISTRY: dict[tuple[str,str], type[Producer]] = {
    ("binance",     "trades"):  BinanceWsProducer,
    ("tradingview", "quotes"):  TradingViewWsProducer,
    ("tradingview", "bars"):    TradingViewApiProducer,
}


def create_producers(spec: FeederSpec, bus: EventBus) -> list[Producer]:
    producers: list[Producer] = []
    for st in spec.streams:
        stream_cfg = spec.extra.get(st, {})
        cls = PRODUCER_REGISTRY[(spec.provider, st)]
        name = f"{spec.provider}:{st}:{spec.name}"
        producers.append(cls(name, spec.symbols, bus, **stream_cfg))        # type: ignore
    return producers


def register_writers() -> WriterRegistry:
    """Create and register all WriterRegistry writers for supported providers/streams."""
    reg = WriterRegistry()
    # TradingView:
    reg.add(
        provider="tradingview",
        stream="quotes",
        writer=tv.schema.tv_quotes_writer(),
        flatten=tv.adapter.flatten_quotes
    )
    reg.add(
        provider="tradingview",
        stream="bars",
        writer=tv.schema.tv_bars_writer(),
        flatten=tv.adapter.flatten_bar
    )
    # Binance:
    reg.add(
        provider="binance",
        stream="trades",
        writer=bn.schema.binance_trades_writer(),
        flatten=bn.adapter.flatten_trades
    )
    return reg
