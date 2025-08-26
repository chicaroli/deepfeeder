import os
import time
import pandas as pd
from threading import Lock

from tvDatafeed import TvDatafeed, Interval


tv_lock = Lock()


def get_tv_datafeed(username: str | None = None, password: str | None = None) -> TvDatafeed:
    if username and password:
        return TvDatafeed(username=username, password=password)
    return TvDatafeed()


def fetch_tv(symbol: str, exchange: str, interval: Interval, n_bars: int = 5000, retries: int = 5, delay: float = 5.0):
    """
    A rate-limited wrapper for TradingView data fetch with retry on 429 errors.
    """
    for attempt in range(1, retries + 1):
        with tv_lock:
            try:
                tv = get_tv_datafeed()
                df = tv.get_hist(symbol, exchange, interval=interval, n_bars=n_bars)
            except Exception as e:
                if "429" in str(e):
                    print(f"[{symbol}] ⚠️ Rate limited (429). Retry {attempt}/{retries}...")
                    time.sleep(delay * attempt)
                    continue
                print(f"[{symbol}] ❌ Failed with: {e}")
                return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        df["datetime"] = pd.to_datetime(df["datetime"], utc=False)

        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("UTC").dt.tz_localize(None)
        else:
            df["datetime"] = df["datetime"].dt.tz_localize("America/Sao_Paulo").dt.tz_convert("UTC").dt.tz_localize(None)

        df["symbol"] = symbol
        return df[["symbol", "datetime", "open", "high", "low", "close", "volume"]]

    print(f"[{symbol}] ❌ Failed after {retries} retries.")
    return pd.DataFrame()
