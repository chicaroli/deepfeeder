import deepfeeder as dfb
import pandas as pd

mf = dfb.fanout.market_feeder

snp = mf.snapshot_range(
    provider='tradingview',
    data_schema='ohlcv_5m',
    symbol='INDV2025',
)
pd_snp = snp.to_pandas()

# Subscribe first to start the listener and fill the buffer
handle = mf.subscribe(
    provider="tradingview",
    data_schema="ohlcv_1m",
    symbol="INDV2025",
    callback=lambda _: None  # No-op callback
)

rows = []

def emit_msg(env: dict):
    phase = env.get("phase")
    part = env.get("part")
    for r in env.get("rows", []) or []:
        rr = dict(r)
        rr["_phase"] = phase
        rr["_part"] = part
        rows.append(rr)

# Example: catch up all buffered events (since_ns=None)
emitted = mf.catchup_since_ns(
    provider="tradingview",
    data_schema="ohlcv_1m",
    symbol="INDV2025",
    since_ns=None,
    emit=emit_msg,
    fields=None,  # or specify a list of fields
)
# Convert to DataFrame for inspection
pd_replay = pd.DataFrame(rows)
print(f"Total envelopes emitted: {emitted}")
print(pd_replay.head())