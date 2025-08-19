"""Simple MarketFeeder stats smoke test.

Subscribes to a single symbol and prints:
 - initial stats()
 - counters for added / updated / completed batches after a wait
 - final stats()

Adjust ONLY_COMPLETED to see either all batches or only completed bars.
"""
import time
import deepfeeder as df

mf = df.fanout.market_feeder

PROVIDER = "binance"
SCHEMA = "ohlcv_1m"  # change to "trades" for faster activity
SYMBOL = "BTCUSDT"
WAIT_SECONDS = 75
ONLY_COMPLETED = False  # set True to see only completed bars (may stay 0 until rollover)

# IMPORTANT: counts must be at module scope so the callback can update it (emit_completed swallows exceptions)
counts = {"added_batches": 0, "updated_batches": 0, "completed_batches": 0, "completed_rows": 0}
_printed_first = False


def _cb(msg: dict):
    global _printed_first
    try:
        a = msg.get("added")
        u = msg.get("updated")
        c = msg.get("completed")
        if a is not None and getattr(a, "num_rows", 0) > 0:
            counts["added_batches"] += 1
        if u is not None and getattr(u, "num_rows", 0) > 0:
            counts["updated_batches"] += 1
        if c is not None and getattr(c, "num_rows", 0) > 0:
            counts["completed_batches"] += 1
            counts["completed_rows"] += c.num_rows
        if not _printed_first:
            print("First batch received -> counts:", counts)
            _printed_first = True
    except Exception as e:
        # Expose callback errors since core silently suppresses them
        print("Callback error:", e)


def main():
    print("Initial stats:")
    print(mf.stats())

    handle = mf.subscribe(PROVIDER, SCHEMA, SYMBOL, _cb, only_completed=ONLY_COMPLETED)
    print(f"Subscribed {SYMBOL} handle={handle} ONLY_COMPLETED={ONLY_COMPLETED}")
    print("Post-subscribe stats:")
    print(mf.stats())

    print(f"Waiting {WAIT_SECONDS}s...")
    for _ in range(WAIT_SECONDS):
        time.sleep(1)

    print("Counts:", counts)
    print("Final stats():")
    print(mf.stats())

    mf.unsubscribe(handle)
    print("After unsubscribe stats():")
    print(mf.stats())
    print("Done.")


if __name__ == "__main__":
    main()
