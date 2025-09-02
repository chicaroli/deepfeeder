# app.d/sinks/registry.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple, Optional, List
from core.contracts import Tick
import os
DEBUG_IO = os.getenv("DF_DEBUG_IO", "0") not in ("0", "false", "False")

WriterKey = Tuple[str, str]  # (provider, stream)
Flattener = Callable[[List[Tick]], Dict[str, List]]

@dataclass
class WriterSpec:
    writer: Any            # Deephaven DynamicTableWriter (or adapter with write_rows)
    flatten: Flattener     # how to turn List[Tick] -> column lists for THIS writer

class WriterRegistry:
    def __init__(self) -> None:
        self._by_pair: Dict[WriterKey, WriterSpec] = {}

    def add(self, *, stream: str, flatten: Flattener, writer: Any, provider: Optional[str]=None):
        spec = WriterSpec(writer=writer, flatten=flatten)
        key = (provider or "default", stream)
        self._by_pair[key] = spec
        if DEBUG_IO:
            print(f"[DF DEBUG] WriterRegistry.add provider={key[0]} stream={stream} writer={type(writer).__name__}")

    def resolve(self, provider: Optional[str], stream: str) -> Optional[WriterSpec]:
        key = (provider or "default", stream)
        spec = self._by_pair.get(key)
        if DEBUG_IO:
            print(f"[DF DEBUG] WriterRegistry.resolve provider={key[0]} stream={stream} found={spec is not None}")
        return spec
