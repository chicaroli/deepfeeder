from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

# Append-only event stream for all services/threads.
_EVENTLOG_WRITER = DynamicTableWriter({
	"ts":            dht.Instant,   # event time UTC
	"service":       dht.string,    # logical service: feeder, fanout, etc.
	"name":          dht.string,    # instance identifier (e.g. binance:spot)
	"role":          dht.string,    # thread/role (ws_loop, worker-1, etc.)
	"level":         dht.string,    # INFO,WARN,ERROR,DEBUG
	"code":          dht.string,    # machine tag (START, WS_ERR, ...)
	"message":       dht.string,    # human readable message
	"meta":          dht.string,    # JSON payload (compact)
	"corr_id":       dht.string,    # correlation id (optional)
	"parent_corr_id":dht.string,    # parent correlation id (optional)
})

def get_eventlog_writer():
	return _EVENTLOG_WRITER

def get_eventlog_table():
	# Raw append-only table; consumers apply filtering/windowing.
	return _EVENTLOG_WRITER.table
