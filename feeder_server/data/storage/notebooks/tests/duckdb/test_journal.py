# feeder_server/data/storage/notebooks/tests/duckdb/test_journal.py
# --- Journal smoke test (matches journal_hot schema) -----------------------
import time, duckdb
import deepfeeder as dfb
from config.paths import PATHS
from core.contracts import Tick

bus   = dfb.get_service("event_bus")
store = dfb.get_service("event_store")

def mk_tick(sym, seq, price, qty, side, ts_ns=None):
    if ts_ns is None:
        ts_ns = time.time_ns()
    return Tick(
        provider="binance", stream="trades", symbol=sym,
        ts_ns=int(ts_ns), seq=int(seq),
        payload={"price": float(price), "qty": float(qty), "side": side},
        is_final=True,
    )

# 1) Publish a tiny batch with a unique symbol tag
stamp = int(time.time() * 1000) % 1_000_000
sym   = f"TESTUSDT_{stamp}"
base  = stamp * 10
ticks = [mk_tick(sym, base+i, 100.0+i*0.01, 0.1+i*0.01, "buy" if i%2==0 else "sell") for i in range(5)]
bid   = bus.publish(ticks)
print(f"[journal_smoke] published batch_id={bid} rows={len(ticks)} symbol={sym}")

# 2) Wait for commit
deadline = time.time() + 8
while time.time() < deadline and store.last_committed() < bid:
    time.sleep(0.05)
lc = store.last_committed()
print(f"[journal_smoke] last_committed={lc}")
assert lc >= bid, "Journal consumer did not commit in time (is it running?)"

# 3) Verify the 5 rows in journal_hot using symbol + seq range
con = duckdb.connect(str(PATHS.journal_db))

lo, hi = base, base + len(ticks) - 1
count = con.execute(
    "SELECT COUNT(*) FROM journal_hot "
    "WHERE provider='binance' AND stream='trades' AND symbol=? AND seq BETWEEN ? AND ?",
    [sym, lo, hi],
).fetchone()[0]
print(f"[journal_smoke] journal rows matched: {count}")
assert count == len(ticks), f"Expected {len(ticks)} journal rows, found {count}"

# 4) Inspect a sample with price/qty extracted from JSON
df = con.execute(
    """
    SELECT
      provider, stream, symbol, seq, ts_ns,
      CAST(json_extract(payload, '$.price') AS DOUBLE)  AS price,
      CAST(json_extract(payload, '$.qty')   AS DOUBLE)  AS qty,
      json_extract_string(payload, '$.side')            AS side
    FROM journal_hot
    WHERE symbol = ? AND seq BETWEEN ? AND ?
    ORDER BY seq
    """,
    [sym, lo, hi],
).fetchdf()
print(df)

# 5) Sanity-check values round-tripped
assert float(df["price"].iloc[0]) == 100.0
assert float(df["qty"].iloc[0])   == 0.1
assert df["side"].iloc[0]         == "buy"

print("[journal_smoke] OK ✅")
