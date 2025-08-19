# ingest.feeders.providers.tradingview.schema
from deephaven import DynamicTableWriter, agg
import deephaven.dtypes as dht

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

def tv_quotes_writer():
    return _TV_QUOTES_DTW

def tv_quotes_table():
    return _TV_QUOTES_DTW.table

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
    return bars

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
    return bars

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

