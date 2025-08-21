from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone

from persistence.paths import BINANCE_HOT_DIR
from feeders.binance import binance_trades_writer, binance_trades_table
from feeders.binance.schema import register_binance_trades_tap
import pyarrow as pa

name = 'binance'
base_dir = BINANCE_HOT_DIR


def _to_utc(x):
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x)
    if s.endswith('Z'):
        s = s.replace('Z', '+00:00')
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def to_record(args: tuple) -> Dict[str, Any]:
    # args order per schema DTW
    ts_r = args[8] if len(args) > 8 and args[8] is not None else (args[1] if len(args) > 1 else None)
    ts = _to_utc(ts_r)
    sym_u = (args[2] or '')
    return {
        'ts': ts,
        'dt': ts.date().isoformat(),
        'symbol': sym_u.lower(),
        'EventType': args[0] if len(args) > 0 else None,
        'EventTime': _to_utc(args[1]) if len(args) > 1 else None,
        'Symbol': sym_u,
        'TradeID': args[3] if len(args) > 3 else None,
        'Price': args[4] if len(args) > 4 else None,
        'Quantity': args[5] if len(args) > 5 else None,
        'BuyerID': args[6] if len(args) > 6 else None,
        'SellerID': args[7] if len(args) > 7 else None,
        'Timestamp': _to_utc(args[8]) if len(args) > 8 else None,
        'IsBuyerMaker': args[9] if len(args) > 9 else None,
    }


def to_writer_args(r: Dict[str, Any]) -> tuple:
    return (
        r.get('EventType'), r.get('EventTime'), r.get('Symbol'), r.get('TradeID'),
        r.get('Price'), r.get('Quantity'), r.get('BuyerID'), r.get('SellerID'),
        r.get('Timestamp'), r.get('IsBuyerMaker'),
    )


def get_writer():
    return binance_trades_writer()


def register_tap(fn):
    register_binance_trades_tap(fn)


def build_seen(symbol: str, t0: datetime, t1: datetime):
    seen: set[Tuple[str, Optional[int]]] = set()
    try:
        t = binance_trades_table()
        df = t.where(f"Symbol == '{symbol.upper()}'") \
             .where(f"Timestamp >= `{t0.isoformat()}` && Timestamp < `{t1.isoformat()}`") \
             .select_distinct('Symbol', 'TradeID') \
             .to_pandas()
        for _, row in df.iterrows():
            s = str(row['Symbol']).lower()
            tid = int(row['TradeID']) if row['TradeID'] is not None else None
            seen.add((s, tid))
    except Exception:
        pass
    return seen


def make_key(record: Dict[str, Any]) -> Tuple[str, Optional[int]]:
    """Hashable dedupe key: (symbol.lower(), TradeID or None)."""
    try:
        return (str(record.get('Symbol', '')).lower(), int(record.get('TradeID')) if record.get('TradeID') is not None else None)
    except Exception:
        return (str(record.get('symbol', '')).lower(), None)


def to_arrow_table(batch: List[Dict[str, Any]]) -> pa.Table:
    """Convert a batch of Binance trade records to a typed Arrow table.

    Columns:
    - ts: timestamp[us, tz=UTC]
    - dt: string
    - symbol: string (lowercase)
    - EventType: string
    - EventTime: timestamp[us, tz=UTC]
    - Symbol: string (original case)
    - TradeID: int64
    - Price: float64
    - Quantity: float64
    - BuyerID: int64
    - SellerID: int64
    - Timestamp: timestamp[us, tz=UTC]
    - IsBuyerMaker: bool
    """
    def col(name):
        return [rec.get(name) for rec in batch]

    ts_type = pa.timestamp('us', tz='UTC')
    schema = pa.schema([
        ('ts', ts_type),
        ('dt', pa.string()),
        ('symbol', pa.string()),
        ('EventType', pa.string()),
        ('EventTime', ts_type),
        ('Symbol', pa.string()),
        ('TradeID', pa.int64()),
        ('Price', pa.float64()),
        ('Quantity', pa.float64()),
        ('BuyerID', pa.int64()),
        ('SellerID', pa.int64()),
        ('Timestamp', ts_type),
        ('IsBuyerMaker', pa.bool_()),
    ])

    arrays = [
        pa.array(col('ts'), type=ts_type),
        pa.array(col('dt'), type=pa.string()),
        pa.array(col('symbol'), type=pa.string()),
        pa.array(col('EventType'), type=pa.string()),
        pa.array(col('EventTime'), type=ts_type),
        pa.array(col('Symbol'), type=pa.string()),
        pa.array(col('TradeID'), type=pa.int64()),
        pa.array(col('Price'), type=pa.float64()),
        pa.array(col('Quantity'), type=pa.float64()),
        pa.array(col('BuyerID'), type=pa.int64()),
        pa.array(col('SellerID'), type=pa.int64()),
        pa.array(col('Timestamp'), type=ts_type),
        pa.array(col('IsBuyerMaker'), type=pa.bool_()),
    ]
    return pa.Table.from_arrays(arrays, schema=schema)
