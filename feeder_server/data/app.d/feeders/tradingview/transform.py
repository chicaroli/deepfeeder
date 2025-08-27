"""
transform.py

Transformation utilities for TradingView feeder.

Functions in this module convert TradingView API/WS/parquet data to Deephaven row tables and vice versa.
Add new transformation functions here as needed.
"""
import hashlib
from typing import Any, Dict
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from deephaven.time import to_j_instant


def split_exchange_ticker(s: str) -> tuple[str | None, str]:
    """
    Split a TradingView symbol string into exchange and ticker.
    """
    s = (s or "").strip()
    if ":" in s:
        exch, tick = s.split(":", 1)
        return (exch.strip().upper() or None), tick.strip().upper()
    return None, s.upper()

def to_instant_from_epoch_s(x: float | None):
    """
    Convert epoch seconds to Deephaven Instant.
    """
    if x is None:
        return None
    try:
        return to_j_instant(datetime.fromtimestamp(int(x), tz=timezone.utc))
    except Exception:
        return None

def ws_row_to_dh_row(row: tuple) -> dict:
    """
    Convert TradingView WebSocket tuple to Deephaven row dict.
    """
    return {
        'exchange': row[0] or '',
        'symbol': row[1],
        'datetime': row[2],
        'last_price': row[3],
        'bid': row[4],
        'ask': row[5],
        'volume': row[6],
        'change': row[7],
        'change_pct': row[8],
        'vol_delta': row[9],
    }

def df_row_to_dh_row(exchange: str, symbol: str, row: pd.Series) -> Dict[str, Any]:
    """
    Convert a TradingView DataFrame row to Deephaven row format.
    Args:
        exchange: Exchange name.
        symbol: Trading symbol.
        row: Pandas Series representing a row from TradingView data.
    Returns:
        Dict with keys matching Deephaven table columns.
    """
    dt = row['datetime']
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    j_dt = to_j_instant(dt)
    return {
        'exchange': exchange,
        'symbol': symbol,
        'datetime': j_dt,
        'open': row['open'],
        'high': row['high'],
        'low': row['low'],
        'close': row['close'],
        'volume': row['volume']
    }

def normalize_journal_record(rec: dict) -> dict:
    """Normalize a journal record for writing.

    Ensures timestamp is a pandas Timestamp with UTC tz, lowercases exchange/symbol
    and produces a dt (ISO date) string for partitioning.
    """
    ts = rec.get('timestamp')
    try:
        if isinstance(ts, datetime):
            ts_ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            ts_pd = pd.to_datetime(ts_ts)
        else:
            ts_pd = pd.to_datetime(str(ts), utc=True, errors='coerce')
    except Exception:
        ts_pd = pd.NaT

    try:
        dt = ts_pd.date().isoformat() if not pd.isna(ts_pd) else datetime.utcnow().date().isoformat()
    except Exception:
        dt = datetime.utcnow().date().isoformat()

    return {
        'dt': dt,
        'timestamp': ts_pd,
        'exchange': (rec.get('exchange') or '').lower() if rec.get('exchange') else 'unknown',
        'symbol': (rec.get('symbol') or '').lower() if rec.get('symbol') else 'unknown',
        'open': rec.get('open'),
        'high': rec.get('high'),
        'low': rec.get('low'),
        'close': rec.get('close'),
        'volume': rec.get('volume'),
    }

def records_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Convert a list of raw journal records into a typed pandas DataFrame.

    The resulting DataFrame includes columns: dt, timestamp (datetime[ns, UTC]),
    exchange, symbol, open, high, low, close, volume.
    """
    norm = [normalize_journal_record(r) for r in rows]
    df = pd.DataFrame(norm)
    # enforce types
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    for c in ('open', 'high', 'low', 'close', 'volume'):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def to_arrow_table(df: pd.DataFrame) -> pa.Table:
    """Convert a pandas DataFrame to a pyarrow Table with sensible schema."""
    return pa.Table.from_pandas(df, preserve_index=False)
