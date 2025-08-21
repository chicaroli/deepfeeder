from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime, timezone
import hashlib

from persistence.paths import TV_HOT_DIR
from feeders.tradingview import tv_quotes_writer, tv_quotes_table
from feeders.tradingview.schema import register_tv_quotes_tap
import pyarrow as pa

name = 'tv'
base_dir = TV_HOT_DIR


def _to_utc(x):
    if isinstance(x, datetime):
        return x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
    s = str(x)
    if s.endswith('Z'):
        s = s.replace('Z', '+00:00')
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def to_record(args: tuple) -> Dict[str, Any]:
    ts = _to_utc(args[2])
    sym_u = (args[1] or '')
    return {
        'ts': ts,
        'dt': ts.date().isoformat(),
        'symbol': sym_u.lower(),
        'Exchange': args[0] if len(args) > 0 else None,
        'Symbol': sym_u,
        'LpTime': _to_utc(args[2]) if len(args) > 2 else None,
        'LastPrice': args[3] if len(args) > 3 else None,
        'Bid': args[4] if len(args) > 4 else None,
        'Ask': args[5] if len(args) > 5 else None,
        'Volume': args[6] if len(args) > 6 else None,
        'Change': args[7] if len(args) > 7 else None,
        'ChangePct': args[8] if len(args) > 8 else None,
        'VolDelta': args[9] if len(args) > 9 else None,
    }


def to_writer_args(r: Dict[str, Any]) -> tuple:
    return (
        r.get('Exchange'), r.get('Symbol'), r.get('LpTime'), r.get('LastPrice'),
        r.get('Bid'), r.get('Ask'), r.get('Volume'), r.get('Change'), r.get('ChangePct'), r.get('VolDelta'),
    )


def get_writer():
    return tv_quotes_writer()


def register_tap(fn):
    register_tv_quotes_tap(fn)


def build_seen(symbol: str, t0: datetime, t1: datetime):
    seen: set[str] = set()
    try:
        t = tv_quotes_table()
        df = t.where(f"Symbol == '{symbol.upper()}'") \
             .where(f"LpTime >= `{t0.isoformat()}` && LpTime < `{t1.isoformat()}`") \
             .select('LpTime', 'Symbol', 'Bid', 'Ask', 'LastPrice', 'Volume') \
             .to_pandas()
        for _, r in df.iterrows():
            key_raw = (str(r['Symbol']).lower(), str(r['LpTime']), r['Bid'], r['Ask'], r['LastPrice'], r['Volume'])
            m = hashlib.md5('|'.join(map(str, key_raw)).encode('utf-8')).hexdigest()
            seen.add(m)
    except Exception:
        pass
    return seen


def make_key(record: Dict[str, Any]) -> str:
    key_raw = (
        str(record.get('Symbol', '')).lower(),
        str(record.get('LpTime')),
        record.get('Bid'), record.get('Ask'), record.get('LastPrice'), record.get('Volume'),
    )
    return hashlib.md5('|'.join(map(str, key_raw)).encode('utf-8')).hexdigest()


def to_arrow_table(batch: List[Dict[str, Any]]) -> pa.Table:
    """Convert a batch of TV quote records to a typed Arrow table."""
    def col(name):
        return [rec.get(name) for rec in batch]

    ts_type = pa.timestamp('us', tz='UTC')
    schema = pa.schema([
        ('ts', ts_type),
        ('dt', pa.string()),
        ('symbol', pa.string()),
        ('Exchange', pa.string()),
        ('Symbol', pa.string()),
        ('LpTime', ts_type),
        ('LastPrice', pa.float64()),
        ('Bid', pa.float64()),
        ('Ask', pa.float64()),
        ('Volume', pa.float64()),
        ('Change', pa.float64()),
        ('ChangePct', pa.float64()),
        ('VolDelta', pa.float64()),
    ])

    arrays = [
        pa.array(col('ts'), type=ts_type),
        pa.array(col('dt'), type=pa.string()),
        pa.array(col('symbol'), type=pa.string()),
        pa.array(col('Exchange'), type=pa.string()),
        pa.array(col('Symbol'), type=pa.string()),
        pa.array(col('LpTime'), type=ts_type),
        pa.array(col('LastPrice'), type=pa.float64()),
        pa.array(col('Bid'), type=pa.float64()),
        pa.array(col('Ask'), type=pa.float64()),
        pa.array(col('Volume'), type=pa.float64()),
        pa.array(col('Change'), type=pa.float64()),
        pa.array(col('ChangePct'), type=pa.float64()),
        pa.array(col('VolDelta'), type=pa.float64()),
    ]
    return pa.Table.from_arrays(arrays, schema=schema)
