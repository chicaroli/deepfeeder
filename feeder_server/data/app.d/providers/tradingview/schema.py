# providers.tradingview.schema
import deephaven.dtypes as dht
from deephaven import DynamicTableWriter, agg, merge

from providers.bins import bins_recent

# --- Schema metadata (exported) ---
TV_QUOTES_TIME_COL = "LpTime"
TV_QUOTES_SYMBOL_COL = "Symbol"
TV_QUOTES_SCHEMA_COLS = ("LpTime", "Symbol", "LastPrice", "Bid", "Ask", "Volume", "Change", "ChangePct", "VolDelta")

# Unified OHLCV schema (common to all minute aggregations)
TV_OHLCV_TIME_COL = "Timestamp"
TV_OHLCV_SYMBOL_COL = "Symbol"
TV_OHLCV_SCHEMA_COLS = ("Timestamp", "Exchange", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Vwap")
TV_OHLCV_FILLED_SCHEMA_COLS = TV_OHLCV_SCHEMA_COLS + ("IsEmpty",)

__all__ = [
    'tv_quotes_writer',
    'tv_quotes_table',
    'tv_bars_writer',
    'tv_bars_table',
    'tv_bars_table_deduped',
    'tv_ohlcv_1m_from_quotes',
    'tv_ohlcv_1m_filled',
    'tv_ohlcv_1m',
    'tv_ohlcv_5m',

    'TV_QUOTES_TIME_COL',
    'TV_QUOTES_SYMBOL_COL',
    'TV_QUOTES_SCHEMA_COLS',
    'TV_OHLCV_TIME_COL',
    'TV_OHLCV_SYMBOL_COL',
    'TV_OHLCV_SCHEMA_COLS',
    'TV_OHLCV_FILLED_SCHEMA_COLS',
    ]

# ---------- Primary Tables Writer Definitions ----------------------------------------
_QUOTES_DTW = DynamicTableWriter({
    'Exchange':     dht.string,
    'Symbol':       dht.string,
    'LpTime':       dht.Instant,
    'LastPrice':    dht.double,
    'Bid':          dht.double,
    'Ask':          dht.double,
    'Volume':       dht.double,
    'Change':       dht.double,
    'ChangePct':    dht.double,
    'VolDelta':     dht.double,
})

_BARS_DTW = DynamicTableWriter({
    'Exchange':     dht.string,
    'Symbol':       dht.string,
    'Timestamp':    dht.Instant,
    'Open':         dht.double,
    'High':         dht.double,
    'Low':          dht.double,
    'Close':        dht.double,
    'Volume':       dht.double,
})


# ----------- Tables Writers / Tables Getters -------------------------------
def tv_quotes_writer():
    return _QUOTES_DTW

def tv_quotes_table():
    return _QUOTES_DTW.table

def tv_bars_writer():
    return _BARS_DTW


# ----------- Derived Tables Contructors ------------------------------------
_BARS_TABLE = _BARS_DTW.table.update([
        "Exchange = Exchange.toUpperCase()",
        "Symbol = Symbol.toUpperCase()",
    ])

def tv_bars_table():
    return _BARS_TABLE

_BARS_DEDUPED_TABLE = (
    _BARS_TABLE
    .sort(['Exchange', 'Symbol', 'Timestamp'])
    .last_by(['Exchange', 'Symbol', 'Timestamp'])
)

def tv_bars_table_deduped():
    return _BARS_DEDUPED_TABLE


# ----------- Bars From Quotes ---------------------------------------------
_BARS_FROM_QUOTES = (
    _QUOTES_DTW.table
    .update([
        'Timestamp = lowerBin(LpTime, MINUTE)',
        'PriceQty = LastPrice * VolDelta',
    ])
    .agg_by(aggs=[
        agg.first('Open=LastPrice'),
        agg.max_('High=LastPrice'),
        agg.min_('Low=LastPrice'),
        agg.last('Close=LastPrice'),
        agg.sum_('Volume=VolDelta'),
        agg.sum_('PriceQty=PriceQty'),
    ],
    by=['Exchange', 'Symbol', 'Timestamp'])
    .update_view([
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / MINUTE)',
        'Vwap = Volume == 0 ? null : PriceQty / Volume',
    ])
    .drop_columns(['PriceQty'])
    .view(list(TV_OHLCV_SCHEMA_COLS))
    )

def tv_ohlcv_1m_from_quotes():
    return _BARS_FROM_QUOTES


def _build_tv_bars_from_quotes_filled():
    """Lightweight recent 1m OHLCV (current + previous) with IsEmpty flag.
    Switched from full-day ``bins_today`` to minimal rolling window ``bins_recent``.
    """
    sparse = _BARS_FROM_QUOTES
    symbols = sparse.where("Timestamp >= lowerBin(now(), DAY)").select_distinct(["Exchange", "Symbol"])
    bins = bins_recent(1, 2)
    grid = symbols.join(bins)
    filled = grid.natural_join(sparse, on=["Exchange", "Symbol", "Timestamp"]).update_view([
        "IsEmpty = isNull(Volume)"
    ])
    return filled.view(list(TV_OHLCV_FILLED_SCHEMA_COLS))

_BARS_FROM_QUOTES_FILLED = _build_tv_bars_from_quotes_filled()

def tv_ohlcv_1m_filled():
    return _BARS_FROM_QUOTES_FILLED



# ---------- Derived Tables Definitions ----------------------------------------
# TV OHLCV from bars + quotes
def _build_tv_ohlcv_1m():
    quotes_1m = (
        # _TV_BARS_FROM_QUOTES_FILLED
        _BARS_FROM_QUOTES
        .view(['Exchange', 'Symbol', 'Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        )
    bars_last = _BARS_DEDUPED_TABLE.agg_by(aggs=[agg.max_("LastBarTs=Timestamp")], by=["Exchange", "Symbol"])
    quotes_after_cutoff = (
        quotes_1m
            .natural_join(bars_last, on=["Exchange", "Symbol"], joins=["LastBarTs"])
            .where("isNull(LastBarTs) || Timestamp > LastBarTs")
            .drop_columns("LastBarTs")
    )
    stitched_bars = (
        merge([_BARS_DEDUPED_TABLE, quotes_after_cutoff])
        .update_view([
            'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / MINUTE)',
            ])
        )
    return stitched_bars

_OHLCV_1M = _build_tv_ohlcv_1m()

def tv_ohlcv_1m():
    return _OHLCV_1M

# RT OHLCV 5M
_OHLCV_5M = (
    _OHLCV_1M
    .update([
        'Timestamp = lowerBin(Timestamp, (5 * MINUTE))',
    ])
    .agg_by(aggs=[
        agg.first('Open=Open'),
        agg.max_('High=High'),
        agg.min_('Low=Low'),
        agg.last('Close=Close'),
        agg.sum_('Volume=Volume'),
        ],
        by=['Exchange', 'Symbol', 'Timestamp'])
    .update_view([
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / (5 * MINUTE))',
        ])
)

def tv_ohlcv_5m():
    return _OHLCV_5M
