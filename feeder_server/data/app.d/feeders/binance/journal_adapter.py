from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone, timedelta

from persistence.paths import BINANCE_HOT_DIR
from feeders.binance.schema import (
    binance_trades_writer,
    binance_trades_table,
    register_binance_trades_tap,
)
from runtime.eventlog import emit_event
from deephaven.time import to_j_instant
# from deephaven.arrow import to_arrow
import pyarrow as pa

from typing import Iterable

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


def build_seen(symbol: str, t0: datetime, t1: datetime) -> set[Tuple[str, Optional[int]]]:
    """
    Build a set of (symbol.lower(), TradeID) tuples for deduplication within a timestamp window.

    Uses Deephaven's Arrow conversion for efficiency.
    """
    seen: set[Tuple[str, Optional[int]]] = set()
    try:
        t = binance_trades_table()
        filtered = t.where(f"Symbol == '{symbol.upper()}'") \
                    .where(f"Timestamp >= `{t0.isoformat()}` && Timestamp < `{t1.isoformat()}`") \
                    .select_distinct('Symbol', 'TradeID')
        
        arrow_tbl = pa.arrow.to_arrow(filtered)
        symbol_arr = arrow_tbl.column('Symbol')
        tradeid_arr = arrow_tbl.column('TradeID')
        for s, tid in zip(symbol_arr, tradeid_arr):
            sym = str(s).lower()
            trade_id = int(tid) if tid is not None else None
            seen.add((sym, trade_id))
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


def _ms_to_dt(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def rest_row_to_record(symbol: str, r: Dict[str, Any]) -> Dict[str, Any]:
    """Map a single Binance REST trade JSON row to the journal record dict expected by the writer.

    Expected input fields (historicalTrades):
      - id (int)
      - price (str|float)
      - qty (str|float)
      - time (int ms)
      - buyerOrderId, sellerOrderId (ints)
      - isBuyerMaker (bool)
    """
    ts_dt = _ms_to_dt(r.get('time') or r.get('T') or r.get('timestamp'))
    trade_id = r.get('id') or r.get('a') or r.get('tradeId')
    try:
        trade_id = int(trade_id) if trade_id is not None else None
    except Exception:
        trade_id = None

    def ffloat(x):
        try:
            return float(x) if x is not None else None
        except Exception:
            return None

    return {
        'EventType': 'trade',
        'EventTime': ts_dt,
        'Symbol': symbol.upper(),
        'TradeID': trade_id,
        'Price': ffloat(r.get('price') or r.get('p')),
        'Quantity': ffloat(r.get('qty') or r.get('q')),
        'BuyerID': int(r.get('buyerOrderId')) if r.get('buyerOrderId') is not None else None,
        'SellerID': int(r.get('sellerOrderId')) if r.get('sellerOrderId') is not None else None,
        'Timestamp': ts_dt,
        'IsBuyerMaker': bool(r.get('isBuyerMaker')) if 'isBuyerMaker' in r else None,
    }


def ingest_rest_rows(rows: Iterable[Dict[str, Any]], symbol: str, writer=None, batch_size: int = 500) -> Tuple[int, int]:
    """Map REST rows and write them using the existing writer.

    Returns (written_count, failed_count).
    """
    writer = writer or get_writer()
    written = 0
    failed = 0

    # First map rows to internal records so we can compute a timestamp window for dedupe
    records: List[Dict[str, Any]] = []
    for r in rows:
        try:
            records.append(rest_row_to_record(symbol, r))
        except Exception:
            # skip malformed rows
            continue

    # Build timestamp window for dedupe query (if timestamps present)
    timestamps = [rec['Timestamp'] for rec in records if rec.get('Timestamp') is not None]
    if timestamps:
        t0 = min(timestamps) - timedelta(seconds=1)
        t1 = max(timestamps) + timedelta(seconds=1)
        try:
            seen = build_seen(symbol, t0, t1)
        except Exception:
            seen = set()
    else:
        seen = set()

    # Write records skipping keys already seen
    for rec in records:
        try:
            key = make_key(rec)
            if key in seen:
                continue
            args = to_writer_args(rec)
            # Use normal writer so taps run
            writer.write_row(*args)
            written += 1
            # mark as seen to avoid duplicates in same batch
            seen.add(key)
            # no id watermarking here; gap detection + dedupe handle duplicates
        except Exception as ex:
            failed += 1
            emit_event('feeder', 'binance', 'backfill', 'ERROR', 'WRITE_ERR', f'Write error: {ex}', {'error': str(ex)})

    return written, failed

# Partitioning schema for Binance: dt, symbol
partition_schema = pa.schema([
    ("dt", pa.string()),
    ("symbol", pa.string()),
])

# Provider-specific replay logic for binance
def replay(symbol: str, t0_iso: str, t1_iso: str, iter_parquet_fn, **kwargs) -> str:
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
        emit_event("journal", name, "replay", "ERROR", "REPLAY_ERR", f"Replay error: {exc}")
        return f"[replay] binance {symbol} ERROR: {exc}"