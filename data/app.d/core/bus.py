# app.d/core/bus.py
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

# Trade ticks (provider-agnostic)
_TRADES_WRITER = DynamicTableWriter({
    "ts": dht.Instant,
    "symbol": dht.string,
    "price": dht.double,
    "qty": dht.double,
    "raw": dht.string,
    "provider": dht.string,
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
