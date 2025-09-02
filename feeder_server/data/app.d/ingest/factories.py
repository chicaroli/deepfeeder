# app.d/ingest/factories.py
from __future__ import annotations
from typing import Any, Mapping
from core.contracts import EventBus, Producer
from runtime.feeder_specs import FeederSpec
from ingest.producers.binance_ws import BinanceWsProducer


def make_binance_ws(spec: FeederSpec, bus: EventBus) -> Producer:
    cfg: Mapping[str, Any] = spec.extra
    batch_size = int(cfg.get("batch_size", 64))
    flush_s    = float(cfg.get("flush_interval_s", 0.25))
    return BinanceWsProducer(
        spec.name,
        spec.symbols,
        bus,
        batch_size=batch_size,
        flush_interval_s=flush_s
    )
