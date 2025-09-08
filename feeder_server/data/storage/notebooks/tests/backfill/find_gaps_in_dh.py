# feeder_server/data/storage/notebooks/find_gaps_in_dh.py
"""Find gaps in current Deephaven tables (binance trades and 1m bars).

Run inside the Deephaven app container Python console or as a script:
    python /data/storage/notebooks/find_gaps_in_dh.py

It inspects live tables exposed by deepfeeder and reports:
  - trade id gaps per symbol in binance_trades
  - empty/missing 1m bars in binance_ohlcv_1m_filled

Notes:
- The script snapshots dynamic tables into static pandas DataFrames for analysis.
- If your tables are very large, consider increasing available memory or
  using a sampled timeframe (the script can be modified to filter by time).
"""
from __future__ import annotations
import time
from typing import List, Tuple

import pandas as pd

import deepfeeder as df


def _to_pandas(tbl) -> pd.DataFrame:
    """Snapshot a Deephaven dynamic table to a pandas DataFrame (best-effort)."""
    try:
        snap = tbl.snapshot()
        return snap.to_pandas()
    except Exception:
        try:
            return tbl.to_pandas()
        except Exception as e:
            raise RuntimeError("Failed to convert table to pandas: " + repr(e))


def find_trade_gaps(trades_table) -> List[Tuple[str, int, int, int]]:
    """Find gaps in TradeID sequences.

    Returns list of tuples: (symbol, gap_start, gap_end, discovered_at_ns)
    """
    df_trades = _to_pandas(trades_table)
    if df_trades.empty:
        print("No trades rows available in table.")
        return []

    # Keep only relevant columns and ensure numeric TradeID
    cols = [c for c in ("Symbol", "TradeID", "Timestamp") if c in df_trades.columns]
    df_trades = df_trades[cols].copy()
    df_trades = df_trades.dropna(subset=["TradeID"])  # remove rows without TradeID
    df_trades["TradeID"] = df_trades["TradeID"].astype("int64")

    gaps = []
    now_ns = time.time_ns()
    for sym, g in df_trades.groupby("Symbol"):
        # sort and drop duplicates by TradeID (keep first/last doesn't matter)
        g2 = g.sort_values("TradeID").drop_duplicates(subset=["TradeID"])  # asc
        prev = None
        for tid in g2["TradeID"].values:
            if prev is None:
                prev = int(tid)
                continue
            if int(tid) > prev + 1:
                gap_start = prev + 1
                gap_end = int(tid) - 1
                gaps.append((sym, gap_start, gap_end, now_ns))
            prev = int(tid)
    return gaps


def find_1m_bar_gaps(filled_bars_table) -> List[Tuple[str, pd.Timestamp]]:
    """Find empty/missing 1m bars from the _filled table.

    Returns list of (symbol, timestamp) where IsEmpty is True.
    """
    df_bars = _to_pandas(filled_bars_table)
    if df_bars.empty:
        print("No bars available in filled table.")
        return []

    if "IsEmpty" not in df_bars.columns:
        print("Filled bars table missing 'IsEmpty' column; cannot detect empties.")
        return []

    empties = df_bars[df_bars["IsEmpty"] == True]
    rows = []
    if not empties.empty:
        for _, r in empties.iterrows():
            rows.append((r.get("Symbol"), r.get("Timestamp")))
    return rows


def main():
    tables = df.tables()

    print("Available tables:")
    for k in sorted(tables.keys()):
        print("  ", k)

    # 1) Trades gaps
    trades_tbl = tables.get("binance_trades")
    if trades_tbl is None:
        print("binance_trades table not found in deepfeeder.tables(); skipping trade gap check.")
    else:
        print("Scanning binance_trades for TradeID gaps...")
        try:
            trade_gaps = find_trade_gaps(trades_tbl)
        except Exception as e:
            print("Error while scanning trades table:", e)
            trade_gaps = []

        if not trade_gaps:
            print("No trade gaps detected.")
        else:
            print(f"Detected {len(trade_gaps)} trade gap(s):")
            for sym, a, b, at in trade_gaps:
                print(f"  {sym}: [{a},{b}] discovered_at_ns={at}")

    # 2) 1m bars empties
    bars_tbl = tables.get("binance_ohlcv_1m_filled")
    if bars_tbl is None:
        print("binance_ohlcv_1m_filled table not found; skipping 1m filled check.")
    else:
        print("Scanning binance_ohlcv_1m_filled for IsEmpty bars...")
        try:
            empty_bars = find_1m_bar_gaps(bars_tbl)
        except Exception as e:
            print("Error while scanning filled bars table:", e)
            empty_bars = []

        if not empty_bars:
            print("No empty 1m bars detected in recent window.")
        else:
            print(f"Detected {len(empty_bars)} empty 1m bar(s):")
            for sym, ts in empty_bars:
                print(f"  {sym} @ {ts}")

    print("Scan complete.")


if __name__ == "__main__":
    main()

