# filepath: feeder_server/data/storage/notebooks/tests/duckdb/test_event_store.py
# --- EventStore smoke test (run in Deephaven UI) --------------------------
import time, duckdb
import deepfeeder as dfb
from config.paths import PATHS
from storage.eventstore_duckdb import DuckDbEventStore
from core.contracts import Tick, Envelope

# Use the singleton store managed by services (recommended)
store = dfb.get_service("event_store")
# Or: reopen a fresh handle on the same DB path
# store = DuckDbEventStore(str(PATHS.event_store_db))

def mk_tick(symbol, ts_ns, seq, price, qty, side):
    return Tick(
        provider="binance", stream="trades", symbol=symbol,
        ts_ns=int(ts_ns), seq=int(seq),
        payload={"price": float(price), "qty": float(qty), "side": side},
        is_final=True,
    )

# 1) Append an envelope (DB must assign batch_id)
now_ns = time.time_ns()
ticks1 = [
    mk_tick("BTCUSDT", now_ns,       1, 100.0, 0.10, "buy"),
    mk_tick("BTCUSDT", now_ns + 500, 2, 101.0, 0.20, "sell"),
]
env1 = Envelope(batch_id=-1, produced_at_ns=now_ns,
                first_offset=None, last_offset=None, rows=ticks1)
bid1 = store.append(env1)
print(f"[event_store] append#1 -> batch_id={bid1}")

# 2) Read it back and verify round-trip
envs = store.next_from(bid1, 10)
assert len(envs) >= 1 and envs[0].batch_id == bid1, "next_from failed to return the appended envelope"
rt = envs[0].rows
assert len(rt) == len(ticks1), "tick count mismatch after round-trip"
assert rt[0].symbol == ticks1[0].symbol, "symbol mismatch on round-trip"
assert float(rt[0].payload["price"]) == 100.0, "payload(price) mismatch"

# 3) Ack and verify
store.mark_committed(bid1)
lc = store.last_committed()
print(f"[event_store] last_committed={lc}")
assert lc == bid1, "last_committed did not advance to bid1"

# 4) Second append: ensure monotonic batch_ids
ticks2 = [mk_tick("ETHUSDT", now_ns + 2000, 3, 2000.0, 1.0, "buy")]
env2 = Envelope(batch_id=-1, produced_at_ns=now_ns + 2000,
                first_offset=None, last_offset=None, rows=ticks2)
bid2 = store.append(env2)
print(f"[event_store] append#2 -> batch_id={bid2}")
assert bid2 > bid1, "batch_id did not increase on second append"

# 5) Quick peek at raw DuckDB rows
con = duckdb.connect(str(PATHS.event_store_db))
rows = con.execute(
    "SELECT batch_id, row_count, strftime(produced_at, '%Y-%m-%d %H:%M:%S') "
    "FROM event_store ORDER BY batch_id DESC LIMIT 5"
).fetchall()
print("[event_store] tail:", rows)

print("[event_store] OK: append/read/ack/sequence smoke test passed ✅")
