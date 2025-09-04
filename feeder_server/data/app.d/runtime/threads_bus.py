# runtime/threads_bus.py
"""Threads / services control-plane DynamicTableWriter and accessors.

Each heartbeat or lifecycle event appends a row keyed by (service, name, role).
A *live* snapshot view is exposed via last_by([...]) for convenience.
"""
from __future__ import annotations
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

_THREADS_WRITER = DynamicTableWriter({
    "service":        dht.string,
    "name":           dht.string,
    "role":           dht.string,
    "state":          dht.string,
    "started_at":     dht.Instant,
    "last_heartbeat": dht.Instant,
    "uptime_s":       dht.long,
    "last_error":     dht.string,
    "meta":           dht.string,
})

def get_threads_writer():
    """Return the singleton DynamicTableWriter for thread/service heartbeats."""
    return _THREADS_WRITER

def get_threads_table():
    """Return a *live* snapshot table (one row per service/name/role)."""
    return _THREADS_WRITER.table.last_by(["service", "name", "role"])
