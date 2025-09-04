# tests/core/binance_event_bus_smoke.py
from __future__ import annotations
from typing import List, Dict, Optional

# --- import the real pieces we want to test ---
from core.event_bus import EventBus
from core.contracts import EventStore, JournalStore, Envelope, Tick
from sinks.registry import WriterRegistry
from sinks.dh_sink import DhSinkDynamic
from providers.binance.adapter import trade_json_to_tick
from providers.binance.adapter import flatten_trades as binance_flatten_trades

# -----------------------------
# Mocks / test doubles
# -----------------------------
class TinyEventStore(EventStore):
    def __init__(self):
        self._envs: Dict[int, Envelope] = {}
        self._last_id = 0
        self._committed = 0

    def append(self, env: Envelope) -> None:
        self._envs[env.batch_id] = env
        self._last_id = max(self._last_id, env.batch_id)

    def next_from(self, next_batch_id: int, max_n: int) -> List[Envelope]:
        keys = [k for k in sorted(self._envs) if k >= next_batch_id]
        out = [self._envs[k] for k in keys[:max_n]]
        return out

    def last_committed(self) -> int:
        return self._committed

    def mark_committed(self, batch_id: int) -> None:
        self._committed = max(self._committed, int(batch_id))

class TinyJournal(JournalStore):
    def __init__(self):
        self.rows: List[Tick] = []
        self._wm: Dict[str, int] = {"global": 0}

    def append_batch(self, ticks: List[Tick]) -> int:
        # pretend idempotent append; for smoke we just extend
        self.rows.extend(ticks)
        return len(ticks)

    def load_recent(self, since_ts_ns: int, columns: Optional[List[str]] = None) -> List[Tick]:
        return [t for t in self.rows if t.ts_ns >= since_ts_ns]

    def get_watermark(self, scope: str = "global") -> int:
        return self._wm.get(scope, 0)

    def set_watermark(self, value: int, scope: str = "global") -> None:
        self._wm[scope] = int(value)

class CaptureWriter:
    """Pretend DH writer that just records what would be written."""
    def __init__(self):
        self.batches: List[Dict[str, List]] = []

    # DhSinkDynamic prefers write_rows if available
    def write_rows(self, cols: Dict[str, List]) -> None:
        self.batches.append({k: list(v) for k, v in cols.items()})

# -----------------------------
# One-shot drain helpers (no infinite loops)
# -----------------------------
def drain_once_dh(bus: EventBus, sink: DhSinkDynamic) -> int:
    envs = bus.drain_for_dh(max_n=1024, timeout=0.01)
    if not envs:
        return 0
    last = 0
    for env in envs:
        sink.write_envelope(env)
        last = env.batch_id
    bus.ack_dh(last)
    return len(envs)

def drain_once_journal(event_store: TinyEventStore, journal: TinyJournal, bus: EventBus) -> int:
    next_id = event_store.last_committed() + 1
    envs = event_store.next_from(next_id, 1024) or bus.drain_for_journal(1024, 0.01)
    if not envs:
        return 0
    for env in envs:
        journal.append_batch(env.rows)
        event_store.mark_committed(env.batch_id)
    bus.ack_journal(envs[-1].batch_id)
    return len(envs)

# -----------------------------
# Sample data (your three trades)
# -----------------------------
_RAW = [
    {'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1756493800154, 's': 'BTCUSDT', 't': 5202022819, 'p': '108158.12000000', 'q': '0.00252000', 'T': 1756493800154, 'm': True, 'M': True}},
    {'stream': 'btcusdt@trade', 'data': {'e': 'trade', 'E': 1756493800155, 's': 'BTCUSDT', 't': 5202022820, 'p': '108158.12000000', 'q': '0.00183000', 'T': 1756493800155, 'm': True, 'M': True}},
    {'stream': 'ethusdt@trade', 'data': {'e': 'trade', 'E': 1756493800193, 's': 'ETHUSDT', 't': 2798535000, 'p': '4311.63000000', 'q': '0.00120000', 'T': 1756493800193, 'm': True, 'M': True}},
]

# -----------------------------
# Public test entrypoint
# -----------------------------
def run() -> Dict[str, object]:
    """
    Smoke test:
      - Build EventBus with TinyEventStore/TinyJournal
      - Publish 3 Binance trades as Ticks
      - Drain once into DH sink (CaptureWriter) and once into Journal
      - Return what was written for inspection
    """
    event_store = TinyEventStore()
    bus = EventBus(event_store, max_envelopes=10)
    journal = TinyJournal()

    # build sink w/ registry routing ("_binance","trades")
    cap = CaptureWriter()
    reg = WriterRegistry()
    reg.add(provider="binance", stream="trades", writer=cap, flatten=binance_flatten_trades)
    sink = DhSinkDynamic(reg)

    # publish the three samples as Ticks via the real adapter
    ticks = [trade_json_to_tick(m["data"]) for m in _RAW]
    batch_id = bus.publish(ticks)

    # drain once to DH, once to Journal
    n_dh  = drain_once_dh(bus, sink)
    n_jrn = drain_once_journal(event_store, journal, bus)

    # prepare a compact view of what the DH sink received
    written = cap.batches[-1] if cap.batches else {}
    # also return a few columns for quick visual diff
    compact = {}
    for k in ("Provider","Stream","Symbol","TsNanos","Seq","IsFinal","Price","Qty","Side"):
        if k in written:
            compact[k] = written[k]

    return {
        "batch_id": batch_id,
        "dh_env_count": n_dh,
        "journal_env_count": n_jrn,
        "dh_last_batch_compact": compact,
        "journal_rows": len(journal.rows),
    }

if __name__ == '__main__':
    result = run()
    import pprint
    pprint.pprint(result)