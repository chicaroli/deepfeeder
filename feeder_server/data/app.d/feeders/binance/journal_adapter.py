from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone

from persistence.paths import BINANCE_HOT_DIR
from feeders.binance import binance_trades_writer, binance_trades_table
from feeders.binance.schema import register_binance_trades_tap
from runtime.eventlog import emit_event
from deephaven.time import to_j_instant
import pyarrow as pa

name = 'binance'
base_dir = BINANCE_HOT_DIR


def _to_utc(x):
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x)
    # Truncate fractional seconds to 6 digits for Python compatibility
    if '.' in s:
        pre, post = s.split('.', 1)
        if '+' in post or 'Z' in post:
            if '+' in post:
                frac, rest = post.split('+', 1)
                frac = frac[:6]
                s = f"{pre}.{frac}+{rest}"
            else:
                frac, rest = post.split('Z', 1)
                frac = frac[:6]
                s = f"{pre}.{frac}Z"
        else:
            s = f"{pre}.{post[:6]}"
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
    """Map a persisted record back to writer args, converting times to Instant."""
    trade_id = r.get('TradeID') if 'TradeID' in r else r.get('TradeId')
    # Convert datetime/iso strings to Deephaven Instant
    et = r.get('EventTime')
    ts = r.get('Timestamp')
    try:
        et_i = to_j_instant(_to_utc(et)) if et is not None else None
    except Exception:
        et_i = None
    try:
        ts_i = to_j_instant(_to_utc(ts)) if ts is not None else None
    except Exception:
        ts_i = None
    return (
        r.get('EventType'), et_i, r.get('Symbol'), trade_id,
        r.get('Price'), r.get('Quantity'), r.get('BuyerID'), r.get('SellerID'),
        ts_i, r.get('IsBuyerMaker'),
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
        sym = record.get('Symbol')
        if sym is None:
            sym = record.get('symbol')
        tid = record.get('TradeID') if 'TradeID' in record else record.get('TradeId')
        return (str(sym or '').lower(), int(tid) if tid is not None else None)
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

# Provider-specific replay logic for binance
def replay(symbol: str, t0_iso: str, t1_iso: str, iter_parquet_fn) -> str:
    from datetime import datetime, timezone
    t0 = datetime.fromisoformat(t0_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    t1 = datetime.fromisoformat(t1_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    writer = get_writer()
    seen: set[Any] = set()
    try:
        seen = build_seen(symbol, t0, t1)
    except Exception:
        pass
    n = 0
    try:
        for r in iter_parquet_fn(base_dir, symbol, t0, t1):
            if not isinstance(r, dict):
                continue
            try:
                key = make_key(r)
            except Exception:
                key = None
            if key is not None and key in seen:
                continue
            try:
                args = to_writer_args(r)
                getattr(writer, "write_row_direct", writer.write_row)(*args)
                if key is not None:
                    seen.add(key)
                n += 1
            except Exception:
                pass
        emit_event("journal", name, "replay", "INFO", "REPLAY", f"[replay] binance {symbol} +{n}")
        return f"[replay] binance {symbol} +{n}"
    except Exception as exc:
        try:
            emit_event("journal", name, "replay", "ERROR", "REPLAY_ERR", f"Replay error: {exc}")
        except Exception:
            pass
        return f"[replay] binance {symbol} ERROR: {exc}"