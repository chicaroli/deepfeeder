"""Smoke tester for BinanceGapFiller._request_gaps

Place this file on the Deephaven server (it already lives under app.d/feeders/binance) and run
it from the Deephaven UI or a server-side Python REPL. Modify `SYMBOL` and `GAP` below.

Notes:
- The `/api/v3/historicalTrades` endpoint requires an API key for `fromId` pagination in many cases.
- If you don't have an API key, try a small recent gap where the public aggTrades could be used instead.
"""
from __future__ import annotations

from typing import List, Tuple
from feeders.binance.backfill import BinanceGapFiller


def test_request_gaps(symbol: str, gap: Tuple[int, int], api_key: str | None = None, sample: int = 10) -> List[dict]:
    """Call the BinanceGapFiller._request_gaps helper for a single gap and print results.

    Returns the raw rows list for further inspection.
    """
    print(f"Creating BinanceGapFiller for {symbol} (api_key set: {'yes' if api_key else 'no'})")
    filler = BinanceGapFiller(symbol=symbol, api_key=api_key)

    print(f"Requesting gap {gap[0]}..{gap[1]} (this may take a few seconds)")
    rows = filler._request_gaps([gap])

    print(f"Total rows fetched: {len(rows)}")
    if rows:
        print("Sample rows:")
        for i, r in enumerate(rows[:sample]):
            print(i, r)
    else:
        print("No rows returned for this gap.")

    return rows


if __name__ == '__main__':
    # Edit these values as needed and run from the Deephaven server-side REPL or UI script runner.
    SYMBOL = 'BTCUSDT'
    # Example gap: replace with a real trade id range you want to test
    GAP = (65000000, 65000100)
    API_KEY = None  # set to your Binance API key string if required

    test_request_gaps(SYMBOL, GAP, api_key=API_KEY)
