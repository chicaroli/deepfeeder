# ingest/factories.py
from __future__ import annotations
from core.contracts import EventBus, Producer
from .feeder_specs import FeederSpec
from providers.binance.producer_ws import BinanceWsProducer
from providers.tradingview.producer_ws import TradingViewWsProducer
# from providers.tradingview.producer_rest import TradingViewBarsRestProducer


PRODUCER_REGISTRY: dict[tuple[str,str], type[Producer]] = {
    ("binance",     "trades"):  BinanceWsProducer,
    ("tradingview", "quotes"):  TradingViewWsProducer,
    # ("tradingview", "bars"):    TradingViewBarsRestProducer,
}


def create_producers(spec: FeederSpec, bus: EventBus) -> list[Producer]:
    bs  = int(spec.extra.get("batch_size", 64))
    fls = float(spec.extra.get("flush_interval_s", 0.25))
    producers: list[Producer] = []
    for st in spec.streams:
        cls = PRODUCER_REGISTRY[(spec.provider, st)]
        name = f"{spec.provider}:{st}:{spec.name}"   # canonical per-producer name
        producers.append(cls(name, spec.symbols, bus, batch_size=bs, flush_interval_s=fls))    # type: ignore[arg-type]
    return producers