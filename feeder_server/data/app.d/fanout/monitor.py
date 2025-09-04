# fanout/monitor.py
"""fanout.monitor

Stats table materializer (renamed from control) for MarketFeeder.

Use get_fanout_stats_table(period='PT5S', history=False) to obtain either a
latest snapshot (one row per provider/schema/symbol) or an append-only history.

Internals:
- Filled OHLCV schemas emit placeholders internally; listener suppresses them.
- This table reflects only real listener state transitions (metrics only).
"""
from __future__ import annotations
from typing import Optional
from deephaven import time_table, DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.table_listener import listen, TableListener, TableUpdate
from datetime import datetime, timezone
from .core import market_feeder

_STATS_DTW: Optional[DynamicTableWriter] = None
_STATS_HISTORY_TABLE = None
_STATS_LATEST_TABLE = None
_STATS_PERIOD = None
_TICK_HANDLE = None

_STATS_COLS = {
    "snapshot_ts": dht.Instant,
    "provider": dht.string,
    "schema": dht.string,
    "symbol": dht.string,
    "ref_count": dht.int32,
    "subscriber_count": dht.int32,
    "buffer_len": dht.int32,
    "last_added_rows": dht.int32,
    "last_updated_rows": dht.int32,
    "last_completed_rows": dht.int32,
    "total_handles": dht.int32,
}
_DEF_PERIOD_ISO = "PT5S"


def _emit_snapshot():
    global _STATS_DTW
    if _STATS_DTW is None:
        return
    try:
        s = market_feeder.stats()
        total = s.get("total_handles", 0)
        now_ts = datetime.now(timezone.utc)
        for l in s.get("listeners", []):
            _STATS_DTW.write_row(
                now_ts,
                l.get("provider"),
                l.get("schema"),
                l.get("symbol"),
                int(l.get("ref_count", 0)),
                int(l.get("subscriber_count", 0)),
                int(l.get("buffer_len", 0)),
                int(l.get("last_added_rows", 0)),
                int(l.get("last_updated_rows", 0)),
                int(l.get("last_completed_rows", 0)),
                int(total),
            )
    except Exception:
        pass


class _TickListener(TableListener):
    def on_update(self, update: TableUpdate, is_replay: bool):  # noqa: D401
        _emit_snapshot()
    def on_error(self, e: Exception):  # pragma: no cover
        pass


def get_fanout_stats_table(period: str = _DEF_PERIOD_ISO, history: bool = False):
    """Return a Deephaven stats table for MarketFeeder listeners.

    Args:
        period: ISO-8601 duration string for sampling interval (e.g. 'PT5S').
        history: True => append-only history; False => latest snapshot view.
    """
    global _STATS_DTW, _STATS_HISTORY_TABLE, _STATS_LATEST_TABLE, _STATS_PERIOD, _TICK_HANDLE
    if _STATS_DTW is not None and _STATS_PERIOD == period:
        return _STATS_HISTORY_TABLE if history else _STATS_LATEST_TABLE
    # If changing period, stop prior listener
    if _TICK_HANDLE is not None:
        try:
            _TICK_HANDLE.stop()
        except Exception:
            pass
        _TICK_HANDLE = None
    _STATS_DTW = DynamicTableWriter(_STATS_COLS)
    _STATS_HISTORY_TABLE = _STATS_DTW.table
    _STATS_PERIOD = period
    ticks = time_table(period)
    _TICK_HANDLE = listen(ticks, _TickListener())
    # Some Deephaven versions auto-start the listener; defensively attempt to start but ignore 'already started'
    try:  # pragma: no cover - environment specific
        _TICK_HANDLE.start()
    except RuntimeError as e:
        if "already started" not in str(e).lower():
            raise
    # produce first snapshot immediately so table not empty until first tick
    _emit_snapshot()
    _STATS_LATEST_TABLE = _STATS_HISTORY_TABLE.last_by(["provider", "schema", "symbol"])
    return _STATS_HISTORY_TABLE if history else _STATS_LATEST_TABLE

__all__ = ["get_fanout_stats_table"]
