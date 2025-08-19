import deepfeeder as mfb

# Callback: print only completed bars as a DataFrame
def print_batch(batch):
    completed = batch.get('completed')
    if not completed or completed.num_rows == 0:
        return
    df = completed.to_pandas()
    print("COMPLETED BARS:\n", df)

# Choose provider/schema/symbol for test
provider = "binance"
data_schema = "ohlcv_1m"
symbol = "BTCUSDT"
cols = ['Timestamp', 'Symbol', 'BarId', 'Open', 'High', 'Low', 'Close', 'Volume']
spec = mfb.fanout.get_schemas()[(provider, data_schema)]
view = spec.table_fn().where(f"{spec.symbol_col}=='{symbol}'").view(cols)
SymListener = mfb.fanout.get_sym_listener()
listener = SymListener(provider, data_schema, symbol, spec, view, print_batch, debug=False)

# listener.stop()  # keep running to observe completed bars; stop manually when done

