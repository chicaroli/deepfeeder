# runtime/warmup.py
from __future__ import annotations
from typing import Optional, Set, Tuple
from core.contracts import Envelope, JournalStore, DhSink

def hydrate_dh_from_journal(
    journal: JournalStore,
    dh_sink: DhSink,
    *,
    since_ts_ns: int,
    provider_filter: Optional[Set[str]] = None,
    stream_filter: Optional[Set[str]] = None,
    page_rows: int = 20_000,
) -> Tuple[int, int]:
    """
    Load committed rows from Journal into DH as Envelopes with a synthetic batch_id.
    Returns (total_rows_written, last_ts_ns_written).
    """
    total = 0
    last_ts_written = 0

    for page in journal.stream_ticks_since(
        since_ts_ns,
        provider_filter=provider_filter,
        stream_filter=stream_filter,
        page_rows=page_rows,
    ):
        if not page:
            continue
        # Wrap the page as an Envelope; batch_id is synthetic (not used for cursors)
        env = Envelope(
            batch_id=0,
            produced_at_ns=int(page[-1].ts_ns),
            first_offset=None,
            last_offset=None,
            rows=page,
        )
        dh_sink.write_envelope(env)
        total += len(page)
        last_ts_written = page[-1].ts_ns

    return total, last_ts_written
