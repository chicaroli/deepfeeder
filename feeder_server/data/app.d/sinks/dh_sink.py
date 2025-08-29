# app.d/sinks/dh_sink.py
from __future__ import annotations
from typing import Dict, List, Tuple
from core.contracts import DhSink, Envelope, Tick
from .registry import WriterRegistry

class DhSinkDynamic(DhSink):
    def __init__(self, registry: WriterRegistry):
        self.registry = registry

    def write_envelope(self, env: Envelope) -> None:
        buckets: Dict[Tuple[str,str], List[Tick]] = {}
        for t in env.rows:
            buckets.setdefault((t.provider, t.stream), []).append(t)

        for (provider, stream), ticks in buckets.items():
            spec = self.registry.resolve(provider, stream)
            if not spec:  # no writer registered; you may log/raise instead
                continue
            cols = spec.flatten(ticks)      # provider-specific flatten
            if hasattr(spec.writer, "write_rows"):
                spec.writer.write_rows(cols)
            else:
                # fall back to per-row
                for row in zip(*[cols[c] for c in cols]):
                    spec.writer.write_row(*row)
