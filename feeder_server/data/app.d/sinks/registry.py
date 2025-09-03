# app.d/sinks/registry.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple, Optional, List
from core.contracts import Tick

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

    def resolve(self, provider: Optional[str], stream: str) -> Optional[WriterSpec]:
        key = (provider or "default", stream)
        spec = self._by_pair.get(key)
        return spec
