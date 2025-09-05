# ingest/factories.py
from __future__ import annotations
from typing import Optional, Iterable

from core.contracts import EventBus, Producer, EventStore, JournalStore
from sinks.registry import WriterRegistry
from runtime.services import Services
from runtime.backfill.planner import BackfillPlanner
from runtime.gap_monitors.binance_gap_monitor import BinanceGapMonitor
from .feeder_specs import FeederSpec

import providers.tradingview as tv
import providers.binance as bn


PRODUCER_REGISTRY: dict[tuple[str,str], type[Producer]] = {
    ("binance",     "trades"):      bn.producer_ws.BinanceWsProducer,
    ("binance",     "backfill"):    bn.producer_backfill.BinanceBackfillProducer,
    ("tradingview", "quotes"):      tv.producer_ws.TradingViewWsProducer,
    ("tradingview", "bars"):        tv.producer_api.TradingViewApiProducer,
}


def create_producers(spec: FeederSpec, bus: EventBus, services: Optional[Services] = None) -> list[Producer]:
    """
    Build Producer instances for a FeederSpec.

    - Injects 'services' so we can share singletons (e.g., backfill planner) without importing dfb.services here.
    - Only ('binance','backfill') needs special deps (planner + REST client).
    - Other producers are instantiated with a forgiving constructor pattern.
    """
    producers: list[Producer] = []
    for st in spec.streams:
        key = (spec.provider, st)
        cls = PRODUCER_REGISTRY.get(key)
        name = f"{spec.provider}:{st}:{spec.name}"
        stream_cfg = spec.extra.get(st, {})

        if key == ("binance", "backfill"):
            # Reuse (or create) a singleton planner service
            planner = services.try_get("backfill_planner")
            if planner is None:
                planner = BackfillPlanner(max_ids_per_task=spec.extra.get("max_ids_per_task", 5000))
                services.register("backfill_planner", planner)

            # REST client (API key can come from spec.extra or env)
            rest = bn.rest_client.BinanceRest(api_key=spec.extra.get("api_key"))
            producers.append(cls(name=name, bus=bus, planner=planner, rest=rest))
        else:
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


def ensure_runtime_services(
        specs: Iterable,
        services: Services,
        *,
        event_store: EventStore,
        journal: JournalStore,
        start_monitors: bool = True,
) -> None:
    """Ensure shared control-plane services exist for the configured specs."""
    # Backfill planner (shared)
    planner = services.try_get("backfill_planner")
    if planner is None:
        planner = BackfillPlanner(max_ids_per_task=5000)
        services.register("backfill_planner", planner)

    # If any Binance backfill stream is present, ensure single gap monitor
    needs_binance_gap = any(
        (getattr(s, "provider", None) == "binance") and ("backfill" in getattr(s, "streams", []))
        for s in specs
    )
    if needs_binance_gap and services.try_get("gap_monitor:binance") is None:
        bm = BinanceGapMonitor(event_store=event_store, journal=journal, planner=planner)
        services.register("gap_monitor:binance", bm)
        if start_monitors:
            bm.start()
