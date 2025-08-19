# feeders/binance/schema.py
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht

# --- Schema metadata (exported) ---
BINANCE_OHLCV_1M_TIME_COL = "Timestamp"
BINANCE_OHLCV_1M_SYMBOL_COL = "Symbol"
BINANCE_OHLCV_1M_SCHEMA_COLS = (
    "Timestamp", "Symbol", "BarId", "Open", "High", "Low", "Close", "Volume", "Trades", "Vwap",
    "BuyerMakerCount"
)
BINANCE_TRADES_TIME_COL = "Timestamp"
BINANCE_TRADES_SYMBOL_COL = "Symbol"
BINANCE_TRADES_SCHEMA_COLS = (
    "Timestamp", "Symbol", "TradeId", "Price", "Quantity", "BuyerID", "SellerID", "IsBuyerMaker"
)

__all__ = [
    'binance_trades_writer', 'binance_trades_table', 'binance_ohlcv_1m',
    'BINANCE_OHLCV_1M_TIME_COL', 'BINANCE_OHLCV_1M_SYMBOL_COL', 'BINANCE_OHLCV_1M_SCHEMA_COLS',
    'BINANCE_TRADES_TIME_COL', 'BINANCE_TRADES_SYMBOL_COL', 'BINANCE_TRADES_SCHEMA_COLS'
]

_BINANCE_TRADES_DTW = DynamicTableWriter({
    'EventType': dht.string,
    'EventTime': dht.Instant,
    'Symbol': dht.string,
    'TradeID': dht.long,
    'Price': dht.double,
    'Quantity': dht.double,
    'BuyerID': dht.long,
    'SellerID': dht.long,
    'Timestamp': dht.Instant,
    'IsBuyerMaker': dht.bool_,
})

def binance_trades_writer():
    return _BINANCE_TRADES_DTW

def binance_trades_table():
    return _BINANCE_TRADES_DTW.table

def binance_ohlcv_1m():
    t = _BINANCE_TRADES_DTW.table.update([
        'Timestamp = lowerBin(Timestamp, MINUTE)',
        'BuyerMakerCount = IsBuyerMaker ? 1 : 0',
        'PriceQty = Price * Quantity',
    ])
    ohlc = t.agg_by(
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
    return ohlc
