# feeders/binance/schema.py
from __future__ import annotations
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht
from providers.bins import bins_recent


# --- Unified Schema metadata (exported) ---
BINANCE_OHLCV_TIME_COL = "Timestamp"
BINANCE_OHLCV_SYMBOL_COL = "Symbol"
BINANCE_OHLCV_SCHEMA_COLS = ("Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Trades", "Vwap", "BuyerMakerCount")
BINANCE_OHLCV_FILLED_SCHEMA_COLS = BINANCE_OHLCV_SCHEMA_COLS + ("IsEmpty",)

BINANCE_TRADES_TIME_COL = "Timestamp"
BINANCE_TRADES_SYMBOL_COL = "Symbol"
BINANCE_TRADES_SCHEMA_COLS = ("Timestamp", "Symbol", "TradeId", "Price", "Quantity", "BuyerID", "SellerID", "IsBuyerMaker")

__all__ = [
    'binance_trades_writer', 'binance_trades_table',
    'binance_ohlcv_1m', 'binance_ohlcv_1m_filled', 'binance_ohlcv_5m', 'binance_ohlcv_5m_filled',
    'BINANCE_OHLCV_TIME_COL', 'BINANCE_OHLCV_SYMBOL_COL', 'BINANCE_OHLCV_SCHEMA_COLS', 'BINANCE_OHLCV_FILLED_SCHEMA_COLS',
    'BINANCE_TRADES_TIME_COL', 'BINANCE_TRADES_SYMBOL_COL', 'BINANCE_TRADES_SCHEMA_COLS'
]

_TRADES_DTW = DynamicTableWriter({
    'EventType':    dht.string,
    'EventTime':    dht.Instant,
    'Symbol':       dht.string,
    'TradeID':      dht.long,
    'Price':        dht.double,
    'Quantity':     dht.double,
    'BuyerID':      dht.long,
    'SellerID':     dht.long,
    'Timestamp':    dht.Instant,
    'IsBuyerMaker': dht.bool_,
})

def binance_trades_writer():
    return _TRADES_DTW

def binance_trades_table():
    return _TRADES_DTW.table


# --- Deduped trades table ---------------------------------------------------
def binance_trades_table_deduped():
    """
    Returns a deduplicated version of the Binance trades table,
    removing duplicate TradeID entries per Symbol.
    """
    t = _TRADES_DTW.table
    # Deduplicate by Symbol and TradeID, keeping the latest row (by Timestamp)
    deduped = t.sort_descending("Timestamp").drop_duplicates(by=["Symbol", "TradeID"])
    return deduped

def binance_ohlcv_1m():
    t = _TRADES_DTW.table.update([
        'Timestamp = lowerBin(Timestamp, MINUTE)',
        'BuyerMakerCount = IsBuyerMaker ? 1 : 0',
        'PriceQty = Price * Quantity',
    ])
    bars = t.agg_by(
        aggs=[
            agg.first('Open=Price'),
            agg.max_('High=Price'),
            agg.min_('Low=Price'),
            agg.last('Close=Price'),
            agg.sum_('Volume=Quantity'),
            agg.count_('Trades'),
            agg.sum_('BuyerMakerCount=BuyerMakerCount'),
            agg.sum_('PriceQty=PriceQty'),
        ],
        by=['Symbol', 'Timestamp'],
    ).update_view([
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / MINUTE)',
        'Vwap = Volume == 0 ? null : PriceQty / Volume',
    ]).drop_columns(['PriceQty'])
    return bars.view(list(BINANCE_OHLCV_SCHEMA_COLS))

def binance_ohlcv_1m_filled():
    """Lightweight recent 1m bars (current + previous) with IsEmpty flag.

    Previously this enumerated the full UTC day using ``bins_today``; now we only
    materialize a minimal rolling window (2 bins) via ``bins_recent`` to support
    forced completion / UI freshness with less overhead.
    """
    sparse = binance_ohlcv_1m()
    symbols = sparse.where("Timestamp >= lowerBin(now(), DAY)").select_distinct("Symbol")
    bins = bins_recent(1, 2)  # current + previous minute
    grid = symbols.join(bins)
    filled = grid.natural_join(sparse, on=["Symbol", "Timestamp"]).update_view(["IsEmpty = isNull(Volume)"])
    return filled.view(list(BINANCE_OHLCV_FILLED_SCHEMA_COLS))

def binance_ohlcv_5m():
    one_m = binance_ohlcv_1m()
    t = one_m.update([
        'T5 = lowerBin(Timestamp, 5 * MINUTE)',
        'PriceQty = (Volume == 0 ? 0 : Vwap * Volume)',
    ])
    bars = t.agg_by(
        aggs=[
            agg.first('Open=Open'),
            agg.max_('High=High'),
            agg.min_('Low=Low'),
            agg.last('Close=Close'),
            agg.sum_('Volume=Volume'),
            agg.sum_('Trades=Trades'),
            agg.sum_('BuyerMakerCount=BuyerMakerCount'),
            agg.sum_('PriceQty=PriceQty'),
        ],
        by=['Symbol', 'T5'],
    ).update_view([
        'Timestamp = T5',
        'BarId = (long) ((Timestamp - lowerBin(Timestamp, DAY)) / (5 * MINUTE))',
        'Vwap = Volume == 0 ? null : PriceQty / Volume',
    ]).drop_columns(['T5','PriceQty'])
    return bars.view(list(BINANCE_OHLCV_SCHEMA_COLS))

def binance_ohlcv_5m_filled():
    """Lightweight recent 5m bars (current + previous) with IsEmpty flag.

    Reduced from full-day enumeration to 2-bin rolling window using ``bins_recent``.
    """
    sparse = binance_ohlcv_5m()
    symbols = sparse.where("Timestamp >= lowerBin(now(), DAY)").select_distinct("Symbol")
    bins5 = bins_recent(5, 2)
    grid = symbols.join(bins5)
    filled = grid.natural_join(sparse, on=["Symbol","Timestamp"]).update_view(['IsEmpty = isNull(Volume)'])
    return filled.view(list(BINANCE_OHLCV_FILLED_SCHEMA_COLS))
