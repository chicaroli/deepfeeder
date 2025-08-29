# app.d/runtime/feeder_specs.py
from dataclasses import dataclass
from typing import List
import json, os

@dataclass
class FeederSpec:
    provider: str
    name: str
    symbols: List[str]
    autostart: bool = False

def load_feeder_specs(path: str) -> List[FeederSpec]:
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
