import deepfeeder as dfb
mf = dfb.fanout.market_feeder
print(mf.stats())

def cb(msg: dict):
    meta = msg['meta']
    print(f"Callback: {meta}")

my_handler = mf.subscribe("binance", "ohlcv_1m", "BTCUSDT", cb, ['Timestamp', 'Symbol', 'BarId', 'Close', 'Volume'], True)

mf.unsubscribe(my_handler)