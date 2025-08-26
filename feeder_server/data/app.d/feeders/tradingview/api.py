
import time
from threading import Lock
from typing import Optional

import pandas as pd
from tvDatafeed import TvDatafeed, Interval

__all__ = ["get_tv_datafeed", "fetch_tv_data"]

_tv_lock = Lock()

def get_tv_datafeed(username: Optional[str] = None, password: Optional[str] = None) -> TvDatafeed:
    """
    Create a TvDatafeed client for TradingView.
    Args:
        username: TradingView username (optional).
        password: TradingView password (optional).
    Returns:
        TvDatafeed client instance.
    """
    if username and password:
        return TvDatafeed(username=username, password=password)
    return TvDatafeed()

def fetch_tv_data(
    symbol: str,
    exchange: str,
    interval: Interval,
    n_bars: int = 5000,
    retries: int = 5,
    delay: float = 5.0,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch historical data from TradingView with retry and rate-limit handling.
    Args:
        symbol: Trading symbol.
        exchange: Exchange name.
        interval: Data interval (tvDatafeed.Interval).
        n_bars: Number of bars to fetch.
        retries: Number of retry attempts on rate limit.
        delay: Delay between retries (seconds).
        username: TradingView username (optional).
        password: TradingView password (optional).
    Returns:
        DataFrame with columns: symbol, datetime, open, high, low, close, volume.
    """
    for attempt in range(1, retries + 1):
        with _tv_lock:
            try:
                tv = get_tv_datafeed(username, password)
                df = tv.get_hist(symbol, exchange, interval=interval, n_bars=n_bars)
            except Exception as e:
                if "429" in str(e):
                    print(f"[{symbol}] Rate limited (429). Retry {attempt}/{retries}...")
                    time.sleep(delay * attempt)
                    continue
                print(f"[{symbol}] Failed with: {e}")
                return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.reset_index()
        df["datetime"] = pd.to_datetime(df["datetime"], utc=False)

        if df["datetime"].dt.tz is not None:
            df["datetime"] = df["datetime"].dt.tz_convert("UTC").dt.tz_localize(None)
        else:
            df["datetime"] = df["datetime"].dt.tz_localize("America/Sao_Paulo").dt.tz_convert("UTC").dt.tz_localize(None)

        # Always set symbol and exchange columns from parameters for consistency
        df["symbol"] = symbol
        df["exchange"] = exchange

        return df[["exchange", "symbol", "datetime", "open", "high", "low", "close", "volume"]]

    print(f"[{symbol}] Failed after {retries} retries.")
    return pd.DataFrame()
