# app.d/sinks/dh_sink.py
from __future__ import annotations
from typing import Dict, List, Tuple
from core.contracts import DhSink, Envelope, Tick
from .registry import WriterRegistry
import os

DEBUG_IO = os.getenv("DF_DEBUG_IO", "0") not in ("0", "false", "False")

class DhSinkDynamic(DhSink):
    def __init__(self, registry: WriterRegistry):
        self.registry = registry

    def write_envelope(self, env: Envelope) -> None:
        if DEBUG_IO:
            print(f"[DF DEBUG] DhSink.write_envelope batch_id={env.batch_id} rows={len(env.rows)}")
        buckets: Dict[Tuple[str,str], List[Tick]] = {}
        for t in env.rows:
            buckets.setdefault((t.provider, t.stream), []).append(t)

        for (provider, stream), ticks in buckets.items():
            if DEBUG_IO:
                print(f"[DF DEBUG] DhSink bucket provider={provider} stream={stream} ticks={len(ticks)}")
            spec = self.registry.resolve(provider, stream)
            if not spec:  # no writer registered; you may log/raise instead
                if DEBUG_IO:
                    print(f"[DF DEBUG] DhSink MISSING writer provider={provider} stream={stream}")
                continue
            cols = spec.flatten(ticks)      # provider-specific flatten
            if DEBUG_IO:
                col_lens = {k: len(v) for k,v in cols.items()}
                print(f"[DF DEBUG] DhSink flattened columns lens={col_lens}")
            if hasattr(spec.writer, "write_rows"):
                spec.writer.write_rows(cols)
                if DEBUG_IO:
                    print(f"[DF DEBUG] DhSink wrote rows via write_rows provider={provider} stream={stream}")
            else:
                # fall back to per-row
                for row in zip(*[cols[c] for c in cols], strict=False):
                    spec.writer.write_row(*row)
                if DEBUG_IO:
                    print(f"[DF DEBUG] DhSink wrote rows via write_row loop provider={provider} stream={stream}")
