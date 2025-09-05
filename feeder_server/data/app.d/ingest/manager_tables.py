# ingest/manager_tables.py
"""Table writers & accessors used by the FeederManager.

Previously this lived in bus.py; renamed for clarity to reflect its
association with the manager (lifecycle + status/config surfacing).
"""
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

# --- live configs snapshot (for UI) ---
_CONFIGS_WRITER = DynamicTableWriter({
    "provider":  dht.string,
    "name":      dht.string,
    "symbols":   dht.string,   # CSV string for display
    "autostart": dht.bool_,
    "deleted":   dht.bool_,    # model deletions since DTW can't delete rows
    "updated_at": dht.Instant,
})

def get_configs_writer():
    return _CONFIGS_WRITER

def get_configs_table():
    # latest row per (provider,name), hide deleted
    return _CONFIGS_WRITER.table.last_by(["provider", "name"]).where("deleted==false")

# Feeder status (provider-agnostic)
_STATUS_WRITER = DynamicTableWriter({
    "provider": dht.string,
    "feeder": dht.string,
    "alive": dht.bool_,
    "symbols": dht.string,
    "msg_count": dht.long,
    "last_msg_ts": dht.Instant,
    "uptime_s": dht.long,
    "last_error": dht.string,
})

def get_status_writer():
    return _STATUS_WRITER

def get_status_table():
    # last row per (provider, feeder)
    return _STATUS_WRITER.table.last_by(["provider", "feeder"])

# Backfill gaps and tasks (for UI + producers)
_BACKFILL_GAPS_WRITER = DynamicTableWriter({
    "provider": dht.string,
    "symbol": dht.string,
    "start_id": dht.long,
    "end_id": dht.long,
    "discovered_at_ns": dht.long,
    "status": dht.string,      # pending, planned, running, done, failed
    "priority": dht.long,
})

_BACKFILL_TASKS_WRITER = DynamicTableWriter({
    "task_id": dht.string,
    "provider": dht.string,
    "symbol": dht.string,
    "start_id": dht.long,
    "end_id": dht.long,
    "created_at_ns": dht.long,
    "status": dht.string,      # pending, running, done, failed
    "try_count": dht.long,
    "last_error": dht.string,
})

def get_backfill_gaps_writer():
    return _BACKFILL_GAPS_WRITER

def get_backfill_tasks_writer():
    return _BACKFILL_TASKS_WRITER

def get_backfill_gaps_table():
    return _BACKFILL_GAPS_WRITER.table

def get_backfill_tasks_table():
    return _BACKFILL_TASKS_WRITER.table

