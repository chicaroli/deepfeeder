"""Minimal test of MarketFeeder subscription for completed bars.

Run inside Deephaven app container Python console or as a script:
    python test_market_feeder.py
Stops only when you manually unsubscribe:
    mf.unsubscribe(handle)
"""
import deepfeeder as df

PROVIDER = "binance"
SCHEMA = "ohlcv_1m"
SYMBOL = "BTCUSDT"
MAX_BARS = 2  # set an integer to auto-unsubscribe after that many completed bars

_count = 0
handle = None


def on_msg(msg: dict):
    global _count, handle
    completed = msg.get("completed")
    if not completed or getattr(completed, "num_rows", 0) == 0:
        return
    try:
        dfc = completed.to_pandas()
    except Exception:
        # Fallback manual extraction (unlikely needed)
        rows = []
        for i in range(completed.num_rows):
            rows.append({name: completed.column(j)[i].as_py() for j, name in enumerate(completed.schema.names)})
        import pandas as pd
        dfc = pd.DataFrame(rows)
    for _, r in dfc.iterrows():
        # Print a concise line per completed bar
        print(
            f"COMPLETED {r.Timestamp} {r.Symbol} id={r.BarId} "
            f"O={r.Open} H={r.High} L={r.Low} C={r.Close} V={r.Volume}"
        )
        _count += 1
        if MAX_BARS is not None and _count >= MAX_BARS:
            mf = df.fanout.get_market_feeder()
            mf.unsubscribe(handle)
            print(f"Auto-unsubscribed after {MAX_BARS} bars.")


def main():  # simple entry point
    global handle
    mf = df.fanout.get_market_feeder()
    handle = mf.subscribe(PROVIDER, SCHEMA, SYMBOL, on_msg, only_completed=True)
    print(f"Subscribed (completed bars) handle={handle}. To stop: mf.unsubscribe(handle)")


if __name__ == "__main__":
    main()
