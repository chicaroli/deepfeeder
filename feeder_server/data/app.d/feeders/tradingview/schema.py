# ingest.feeders.providers.tradingview.schema
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht
from feeders.bins import bins_recent
from functools import lru_cache

# --- Schema metadata (exported) ---
TV_QUOTES_TIME_COL = "LpTime"
TV_QUOTES_SYMBOL_COL = "Symbol"
TV_QUOTES_SCHEMA_COLS = ("LpTime", "Symbol", "LastPrice", "Bid", "Ask", "Volume", "Change", "ChangePct", "VolDelta")

# Unified OHLCV schema (common to all minute aggregations)
TV_OHLCV_TIME_COL = "Timestamp"
TV_OHLCV_SYMBOL_COL = "Symbol"
TV_OHLCV_SCHEMA_COLS = ("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap")
TV_OHLCV_FILLED_SCHEMA_COLS = TV_OHLCV_SCHEMA_COLS + ("IsEmpty",)

__all__ = [
    'tv_quotes_writer', 'tv_quotes_table', 'tv_ohlcv_1m_from_quotes', 'tv_ohlcv_5m_from_quotes',
    'tv_ohlcv_1m_filled', 'tv_ohlcv_5m_filled', 'tv_synthetic_trades_view',
    'TV_QUOTES_TIME_COL', 'TV_QUOTES_SYMBOL_COL', 'TV_QUOTES_SCHEMA_COLS',
    'TV_OHLCV_TIME_COL', 'TV_OHLCV_SYMBOL_COL', 'TV_OHLCV_SCHEMA_COLS', 'TV_OHLCV_FILLED_SCHEMA_COLS'
]

_TV_QUOTES_DTW = DynamicTableWriter({
    'Exchange': dht.string,
    'Symbol': dht.string,
    'LpTime': dht.Instant,
    'LastPrice': dht.double,
    'Bid': dht.double,
    'Ask': dht.double,
    'Volume': dht.double,
    'Change': dht.double,
    'ChangePct': dht.double,
    'VolDelta': dht.double,
})

_TV_BARS_DTW = DynamicTableWriter({
    'Exchange': dht.string,
    'Symbol': dht.string,
    'Timestamp': dht.Instant,
    'Open': dht.double,
    'High': dht.double,
    'Low': dht.double,
    'Close': dht.double,
    'Volume': dht.double,
})


# --- mirrored writer with taps ---------------------------------------------
_TV_TAPS = []


class _MirroredWriter:
    def __init__(self, dtw: DynamicTableWriter):
        self._dtw = dtw

    def write_row(self, *args):
        self._dtw.write_row(*args)
        for fn in list(_TV_TAPS):
            try:
                fn(*args)
            except Exception:
                pass

    def write_row_direct(self, *args):
        self._dtw.write_row(*args)

    @property
    def table(self):
        return self._dtw.table


_TV_QUOTES_MIRROR = _MirroredWriter(_TV_QUOTES_DTW)


def register_tv_quotes_tap(fn):
    _TV_TAPS.append(fn)


@lru_cache(maxsize=1)
def tv_quotes_writer():
    return _TV_QUOTES_MIRROR

@lru_cache(maxsize=1)
def tv_quotes_table():
    return _TV_QUOTES_DTW.table

# TV bars writer and table accessors
def tv_bars_writer():
    return _TV_BARS_DTW

def tv_bars_table():
    return _TV_BARS_DTW.table

# TV OHLCV from quotes
@lru_cache(maxsize=1)
def tv_ohlcv_1m_from_quotes():
    t = _TV_QUOTES_DTW.table.update([
        'Timestamp = lowerBin(LpTime, MINUTE)',
        'PriceQty = LastPrice * VolDelta',
    ])
    bars = t.agg_by(
        aggs=[
            agg.first('Open=LastPrice'),
            agg.max_('High=LastPrice'),
            agg.min_('Low=LastPrice'),
            agg.last('Close=LastPrice'),
            agg.sum_('Volume=VolDelta'),
            agg.sum_('PriceQty=PriceQty'),
        ],
        by=['Exchange', 'Symbol', 'Timestamp'],
    ).update_view([
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / MINUTE)',
        'Vwap = Volume == 0 ? null : PriceQty / Volume',
    ]).drop_columns(['PriceQty'])
    return bars.view(list(TV_OHLCV_SCHEMA_COLS))

@lru_cache(maxsize=1)
def tv_ohlcv_5m_from_quotes():
    t = _TV_QUOTES_DTW.table.update([
        'Timestamp = lowerBin(LpTime, 5 * MINUTE)',
        'PriceQty = LastPrice * VolDelta',
    ])
    bars = t.agg_by(
        aggs=[
            agg.first('Open=LastPrice'),
            agg.max_('High=LastPrice'),
            agg.min_('Low=LastPrice'),
            agg.last('Close=LastPrice'),
            agg.sum_('Volume=VolDelta'),
            agg.sum_('PriceQty=PriceQty'),
        ],
        by=['Exchange', 'Symbol', 'Timestamp'],
    ).update_view([
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / (5 * MINUTE))',
        'Vwap = Volume == 0 ? null : PriceQty / Volume',
    ]).drop_columns(['PriceQty'])
    return bars.view(list(TV_OHLCV_SCHEMA_COLS))

@lru_cache(maxsize=1)
def tv_ohlcv_1m_filled():
    """Lightweight recent 1m OHLCV (current + previous) with IsEmpty flag.

    Switched from full-day ``bins_today`` to minimal rolling window ``bins_recent``.
    """
    sparse = tv_ohlcv_1m_from_quotes()
    symbols = sparse.where("Timestamp >= lowerBin(now(), DAY)").select_distinct("Symbol")
    bins = bins_recent(1, 2)
    grid = symbols.join(bins)
    filled = grid.natural_join(sparse, on=["Symbol", "Timestamp"]).update_view([
        "IsEmpty = isNull(Volume)"
    ])
    return filled.view(list(TV_OHLCV_FILLED_SCHEMA_COLS))

@lru_cache(maxsize=1)
def tv_ohlcv_5m_filled():
    """Lightweight recent 5m OHLCV (current + previous) with IsEmpty flag.

    Switched from full-day enumeration to 2-bin rolling window using ``bins_recent``.
    """
    sparse = tv_ohlcv_5m_from_quotes()
    symbols = sparse.where("Timestamp >= lowerBin(now(), DAY)").select_distinct("Symbol")
    bins5 = bins_recent(5, 2)
    grid = symbols.join(bins5)
    filled = grid.natural_join(sparse, on=["Symbol","Timestamp"]).update_view([
        'IsEmpty = isNull(Volume)'
    ])
    return filled.view(list(TV_OHLCV_FILLED_SCHEMA_COLS))

@lru_cache(maxsize=1)
def tv_synthetic_trades_view():
    t = _TV_QUOTES_DTW.table.update([
        'ts = LpTime',
        'provider = "tradingview"',
        'exchange = Exchange',
        'symbol = Symbol',
        'price = LastPrice',
        'qty = VolDelta',
        'raw = (String) null',
        'quality = "synthetic_quote"',
    ])
    return t.view(['ts','provider','exchange','symbol','price','qty','raw','quality'])
