import deepfeeder as dfb
mf = dfb.fanout.market_feeder


def cb(msg: dict):
    completed = msg.get('completed')
    if completed is not None and getattr(completed, 'num_rows', 0) > 0:
        df = completed.to_pandas()
        print(df)

my_handler = mf.subscribe("binance", "ohlcv_1m", "BTCUSDT", cb, ['Timestamp', 'Symbol', 'BarId', 'Close', 'Volume'], only_completed=True)

# print(mf.stats())

# mf.unsubscribe(my_handler)

# mf.unsubscribe_all()