# ingest/feeder_specs.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, List
import json

@dataclass(frozen=True)
class FeederSpec:
    provider: str               # "binance", "tradingview", ...
    name: str                   # service/instance name
    streams: List[str]          # e.g. ["quotes","ohlcv_1m"]
    symbols: List[str]
    autostart: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def key(self) -> str:       # service-level key (not per-producer)
        return f"{self.provider}:{self.name}"

def _normalize_symbols(provider: str, symbols: List[str]) -> List[str]:
    seen, out = set(), []
    for s in symbols or []:
        s2 = s.upper() if provider == "binance" else s
        if s2 not in seen:
            seen.add(s2); out.append(s2)
    return out

def load_feeder_specs(path: str) -> List[FeederSpec]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    specs: List[FeederSpec] = []
    for idx, item in enumerate(data):
        provider = str(item.get("provider","")).strip().lower()
        name     = str(item.get("name","")).strip()
        streams  = item.get("streams")
        if not provider or not name:
            raise ValueError(f"spec[{idx}] missing 'provider' or 'name'")
        if not streams or not isinstance(streams, list):
            raise ValueError(f"spec[{idx}] must include non-empty 'streams' list")

        streams = [str(s).strip().lower() for s in streams]
        symbols = _normalize_symbols(provider, item.get("symbols", []))
        autostart = bool(item.get("autostart", False))
        extra     = dict(item.get("extra", {}))

        # NOTE: Here we keep multi-stream at the spec level.
        specs.append(FeederSpec(provider, name, streams, symbols, autostart, extra))
    return specs
