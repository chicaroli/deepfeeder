from pydeephaven import Session


sess = Session()
t = sess.open_table("tb_binance_trades")
sym_t = t.where("Symbol=='BTCUSDT'")  # filter runs on server

last_ts = None
last_tid = -1

def fetch_new():
    q = sym_t
    if last_ts is not None:
        # fetch only rows strictly after the watermark (server-side)
        str_time = 'parseInstant("' + last_ts.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'+'")'
        q = q.where(f'(EventTime > {str_time}) || (EventTime == {str_time} && TradeID > {last_tid})')
    # materialize a small snapshot of just the new rows
    return q.to_arrow().to_pandas()

# loop with a short sleep; update your watermark from the newest rows you processed
if __name__ == '__main__':
    while True:
        df = fetch_new()
        if not df.empty:
            print(df)
            last_ts = df['Timestamp'].max()
            last_tid = df['TradeID'].max()
        else:
            print("No new data available.")
        # sleep for a while before fetching again
        import time
        time.sleep(1)  # adjust the sleep duration as needed