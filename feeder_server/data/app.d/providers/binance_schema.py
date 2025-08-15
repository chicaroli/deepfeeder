# app.d/providers/binance_schema.py
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht

# --- Detailed Binance trades (provider-specific) ---
# NOTE: not exposed as a top-level var in Panels. Access via getters only.
_BINANCE_TRADES_DTW = DynamicTableWriter({
    "EventType": dht.string,
    "EventTime": dht.Instant,
    "Symbol": dht.string,
    "TradeID": dht.long,
    "Price": dht.double,
    "Quantity": dht.double,
    "BuyerOrderID": dht.long,
    "SellerOrderID": dht.long,
    "Timestamp": dht.Instant,
    "IsBuyerMaker": dht.bool_,
})

def binance_trades_writer():
    """Writer for detailed Binance trades."""
    return _BINANCE_TRADES_DTW

def binance_trades_table():
    """Live detailed trades table."""
    return _BINANCE_TRADES_DTW.table

def binance_ohlcv_1m():
    """1-minute OHLCV derived from detailed trades."""
    t = _BINANCE_TRADES_DTW.table.update([
        "MinuteBin = lowerBin(Timestamp, 60 * 1_000_000_000L)",
        "PriceQty = Price * Quantity",
        "BuyerMakerCount = IsBuyerMaker ? 1 : 0",
    ])
    ohlc = t.agg_by(
        aggs=[
            agg.first("Open=Price"),
            agg.max_("High=Price"),
            agg.min_("Low=Price"),
            agg.last("Close=Price"),
            agg.sum_("Volume=Quantity"),
            agg.sum_("PriceQty=PriceQty"),
            agg.sum_("BuyerMakerCount=BuyerMakerCount"),
            agg.count_("TotalTrades"),
        ],
        by=["Symbol", "MinuteBin"],
    ).update([
        "VWAP = PriceQty / Volume",
        "BuyerMakerPct = (BuyerMakerCount / TotalTrades) * 100",
    ])
    return ohlc
