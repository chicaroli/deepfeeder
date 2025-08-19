import marketfeeder as mfb

# Dummy callback to print batches
def print_batch(batch):
    meta = batch.get('meta', {})
    timestamp = meta.get('timestamp', '')
    added = batch.get('added')
    updated = batch.get('updated')
    completed = batch.get('completed')
    print(f"meta {meta} | {timestamp} | adds: {added.num_rows if added else 0} | updates: {updated.num_rows if updated else 0} | completes: {completed.num_rows if completed else 0}")


# Choose provider/schema/symbol for test
provider = "binance"
data_schema = "ohlcv_1m"
symbol = "BTCUSDT"
spec = mfb.get_schemas()[(provider, data_schema)]
view = spec.table_fn().where(f"{spec.symbol_col}=='{symbol}'").view(list(spec.cols))
SymListener = mfb.get_sym_listener()
listener = SymListener(provider, data_schema, symbol, spec, view, print_batch)
listener.stop()
