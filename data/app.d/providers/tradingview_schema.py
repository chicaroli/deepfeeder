# app.d/providers/tradingview_schema.py
from deephaven import DynamicTableWriter, agg, updateby as ub
import deephaven.dtypes as dht

# Provider-specific quotes table (stateful snapshots)
_TV_QUOTES_DTW = DynamicTableWriter({
    "Exchange":   dht.string,   # "BMFBOVESPA"
    "Ticker":     dht.string,   # "WIN1!"
    "LpTime":     dht.Instant,  # from lp_time (seconds) or last seen
    "LastPrice":  dht.double,   # lp
    "Bid":        dht.double,
    "Ask":        dht.double,
    "Volume":     dht.double,   # session cumulative
    "Change":     dht.double,   # ch
    "ChangePct":  dht.double,   # chp
    "VolDelta":   dht.double,   # computed per-symbol
})

def tv_quotes_writer():
    return _TV_QUOTES_DTW

def tv_quotes_table():
    return _TV_QUOTES_DTW.table

# Optional: 1m OHLCV derived from quotes using VolDelta
def tv_ohlcv_1m_from_quotes():
    t = _TV_QUOTES_DTW.table.update([
        "MinuteBin = lowerBin(LpTime, 60 * 1_000_000_000L)",
        "PriceQty = LastPrice * VolDelta"
    ])
    bars = t.agg_by(
        aggs=[
            agg.first("Open=LastPrice"),
            agg.max_("High=LastPrice"),
            agg.min_("Low=LastPrice"),
            agg.last("Close=LastPrice"),
            agg.sum_("Volume=VolDelta"),
            agg.sum_("PriceQty=PriceQty"),
        ],
        by=["Exchange", "Ticker", "MinuteBin"],
    ).update(["VWAP = Volume == 0 ? null : PriceQty / Volume"])
    return bars

# Optional: synthetic trades (clearly labeled as derived)
def tv_synthetic_trades_view():
    t = _TV_QUOTES_DTW.table.update([
        'ts = LpTime',
        'provider = "tradingview"',
        'exchange = Exchange',
        'ticker = Ticker',
        'price = LastPrice',
        'qty = VolDelta',
        'raw = (String) null',              # not storing raw; keep schema aligned
        'quality = "synthetic_quote"',
    ])
    return t.view(["ts","provider","exchange","ticker","price","qty","raw","quality"])
