# app.d/runtime/feeder_specs.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import json

@dataclass
class FeederSpec:
    provider: str
    name: str
    symbols: list[str]
    autostart: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

def load_feeder_specs(path: str) -> list[FeederSpec]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    specs = []
    for item in data:
        provider = item["provider"].strip().lower()
        # normalize symbols per provider (Binance wants UPPER; TV needs exact TV symbols)
        if provider == "binance":
            syms = [s.upper() for s in item["symbols"]]
        else:
            syms = list(dict.fromkeys(item["symbols"]))  # keep order, de-dup (TV list had repeats)
        specs.append(FeederSpec(provider, item["name"], syms, bool(item.get("autostart", False))))
    return specs
