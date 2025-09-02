import time
import random
import socket
import websocket
from typing import Dict
from threading import Lock
from contextlib import contextmanager
import pandas as pd
from tvDatafeed import TvDatafeedLive, Interval

__all__ = ["fetch_tv_data"]

# ---------- Scoped websocket hardening (applies only during TV call) ----------
@contextmanager
def _tv_ws_hardened(ipv4_only: bool, timeout: float, ping_interval: int, ping_timeout: int):
    orig_getaddrinfo = socket.getaddrinfo
    orig_ws_connect = websocket.WebSocket.connect
    orig_create_conn = websocket.create_connection
    orig_run_forever = websocket.WebSocketApp.run_forever
    orig_default_to = getattr(websocket, "_default_timeout", None)

    try:
        if ipv4_only:
            def _v4_only(host, port, family=0, type=0, proto=0, flags=0):
                res = orig_getaddrinfo(host, port, family, type, proto, flags)
                v4 = [r for r in res if r[0] == socket.AF_INET]
                return v4 or res
            socket.getaddrinfo = _v4_only

        websocket.setdefaulttimeout(timeout)

        def _connect_with_timeout(self, *a, **k):
            k.setdefault("timeout", timeout)
            return orig_ws_connect(self, *a, **k)
        websocket.WebSocket.connect = _connect_with_timeout

        def _create_with_timeout(url, *a, **k):
            k.setdefault("timeout", timeout)
            return orig_create_conn(url, *a, **k)
        websocket.create_connection = _create_with_timeout

        def _run_forever_patched(self, *a, **k):
            k.setdefault("ping_interval", ping_interval)
            k.setdefault("ping_timeout", ping_timeout)
            return orig_run_forever(self, *a, **k)
        websocket.WebSocketApp.run_forever = _run_forever_patched

        yield
    finally:
        socket.getaddrinfo = orig_getaddrinfo
        websocket.WebSocket.connect = orig_ws_connect
        websocket.create_connection = orig_create_conn
        websocket.WebSocketApp.run_forever = orig_run_forever
        websocket.setdefaulttimeout(orig_default_to)

# ---------- Client + concurrency ----------
_call_lock = Lock()           # serialize get_hist (lib isn't thread-safe)

_MIN_INTERVAL = 1.5           # seconds between calls
_next_allowed_ts = 0.0

EXCHANGE_TZ: Dict[str, str] = {
    "BMFBOVESPA": "America/Sao_Paulo",
}

def _respect_rate_limit_locked():
    global _next_allowed_ts
    now = time.time()
    wait = _next_allowed_ts - now
    if wait > 0:
        time.sleep(wait)
    _next_allowed_ts = time.time() + _MIN_INTERVAL

def _normalize_datetime(df: pd.DataFrame, exchange: str) -> pd.DataFrame:
    if "datetime" not in df.columns:
        df = df.reset_index()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    if pd.api.types.is_datetime64tz_dtype(df["datetime"]):
        df["datetime"] = df["datetime"].dt.tz_convert("UTC").dt.tz_localize(None)
    else:
        tz = EXCHANGE_TZ.get(exchange.upper(), "UTC")
        df["datetime"] = (
            df["datetime"]
            .dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
            .dt.tz_convert("UTC")
            .dt.tz_localize(None)
        )
    return df

def _is_rate_or_conn_error(msg: str) -> bool:
    m = msg.lower()
    return any(s in m for s in [
        "429", "too many request", "too many requests", "rate limit", "cloudflare",
        "connection to remote host was lost", "connection reset", "badmessage", "timed out"
    ])

# ---------- Public API ----------
def fetch_tv_data(
    symbol: str,
    exchange: str,
    interval: Interval,
    n_bars: int = 5000,
    retries: int = 1,
    base_delay: float = 1.0,
    ws_timeout: float = 10.0,
    force_ipv4: bool = False,           # keep False to avoid impacting other WS clients
    ping_interval: int = 20,
    ping_timeout: int = 10,
) -> pd.DataFrame:

    for attempt in range(1, retries + 1):
        try:
            client = TvDatafeedLive()

            with _call_lock:
                _respect_rate_limit_locked()

                # Apply WS hardening
                with _tv_ws_hardened(ipv4_only=force_ipv4, timeout=ws_timeout,
                                     ping_interval=ping_interval, ping_timeout=ping_timeout):
                    try:
                        df = client.get_hist(symbol, exchange, interval=interval, n_bars=n_bars, timeout=ws_timeout)
                    except TypeError:
                        # for versions that don't accept timeout kwarg
                        df = client.get_hist(symbol, exchange, interval=interval, n_bars=n_bars)

        except Exception as e:
            msg = str(e)
            if _is_rate_or_conn_error(msg):
                if attempt < retries:
                    sleep_s = base_delay * (2 ** (attempt - 1)) + random.random()
                    print(f"[{exchange}:{symbol}] conn/rate issue: {e!r} — retry {attempt}/{retries} in {sleep_s:.1f}s")
                    time.sleep(sleep_s)
                    continue
            print(f"[{exchange}:{symbol}] fetch failed: {e!r}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        df = _normalize_datetime(df, exchange)
        if "volume" not in df.columns and "value" in df.columns:
            df = df.rename(columns={"value": "volume"})
        df["symbol"] = symbol
        df["exchange"] = exchange

        cols = ["exchange", "symbol", "datetime", "open", "high", "low", "close", "volume"]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            print(f"[{exchange}:{symbol}] missing columns: {missing}")
            return df

        return df[cols].sort_values("datetime").reset_index(drop=True)

    print(f"[{exchange}:{symbol}] failed after {retries} retries.")
    return pd.DataFrame()
