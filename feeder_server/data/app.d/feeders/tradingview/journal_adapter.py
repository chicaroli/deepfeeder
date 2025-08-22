from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime, timezone
import hashlib

from persistence.paths import TV_HOT_DIR
from feeders.tradingview import tv_quotes_writer, tv_quotes_table
from feeders.tradingview.schema import register_tv_quotes_tap
from deephaven.time import to_j_instant
import pyarrow as pa

name = 'tv'
base_dir = TV_HOT_DIR


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
    """Map a persisted record back to writer args, converting LpTime to Instant."""
    lpt = r.get('LpTime') or r.get('lptime')
    try:
        lpt_i = to_j_instant(_to_utc(lpt)) if lpt is not None else None
    except Exception:
        lpt_i = None
    return (
        r.get('Exchange') or r.get('exchange'),
        r.get('Symbol') or r.get('symbol'),
        lpt_i,
        r.get('LastPrice') or r.get('lastprice'),
        r.get('Bid') or r.get('bid'),
        r.get('Ask') or r.get('ask'),
        r.get('Volume') or r.get('volume'),
        r.get('Change') or r.get('change'),
        r.get('ChangePct') or r.get('changepct'),
        r.get('VolDelta') or r.get('voldelta'),
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
        str((record.get('Symbol') or record.get('symbol') or '')).lower(),
        str(record.get('LpTime') or record.get('lptime')),
        record.get('Bid') or record.get('bid'),
        record.get('Ask') or record.get('ask'),
        record.get('LastPrice') or record.get('lastprice'),
        record.get('Volume') or record.get('volume'),
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

# Provider-specific replay logic for tradingview
def replay(symbol: str, t0_iso: str, t1_iso: str, iter_parquet_fn) -> str:
    from datetime import datetime, timezone
    t0 = datetime.fromisoformat(t0_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    t1 = datetime.fromisoformat(t1_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    try:
        writer = get_writer()
        seen: set[str] = set()
        try:
            seen = build_seen(symbol, t0, t1)
        except Exception:
            pass
        n = 0
        for r in iter_parquet_fn(base_dir, symbol, t0, t1):
            # Skip legacy JSON-row files (typed rows have these keys)
            if not isinstance(r, dict) or "Symbol" not in r or "LpTime" not in r:
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
        from runtime.eventlog import emit_event
        emit_event("journal", name, "replay", "INFO", "REPLAY", f"[replay] tv {symbol} +{n}")
        return f"[replay] tv {symbol} +{n}"
    except Exception as exc:
        try:
            from runtime.eventlog import emit_event
            emit_event("journal", name, "replay", "ERROR", "REPLAY_ERR", f"Replay error: {exc}")
        except Exception:
            pass
        return f"[replay] tv {symbol} ERROR: {exc}"