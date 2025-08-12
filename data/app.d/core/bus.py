# app.d/core/bus.py
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


# Trade ticks (provider-agnostic)
_TRADES_WRITER = DynamicTableWriter({
    "ts": dht.Instant,
    "provider": dht.string,
    "symbol": dht.string,
    "price": dht.double,
    "qty": dht.double,
    "raw": dht.string,
})

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

def get_trades_writer():  # writer only (not a Table)
    return _TRADES_WRITER

def get_status_writer():
    return _STATUS_WRITER

def get_trades_table():
    return _TRADES_WRITER.table  # accessed only via bindings

def get_status_table():
    # last row per (provider, feeder)
    return _STATUS_WRITER.table.last_by(["provider", "feeder"])
