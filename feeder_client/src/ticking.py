# filepath: /app/src/test_ticking.py
import os
import time
from dotenv import load_dotenv
from connectors.deephaven_connector import DeephavenConnector
from pydeephaven import TableListener, TableUpdate, listen

load_dotenv()  # Load environment variables from .env

class MyListener(TableListener):
    def on_update(self, update: TableUpdate):
        self._show_deltas("removes", update.removed())
        self._show_deltas("adds", update.added())
        self._show_deltas("modified-prev", update.modified_prev())
        self._show_deltas("modified", update.modified())

    def on_error(self, error: Exception):
        print(f"Error happened: {error}")

    def _show_deltas(self, what: str, dict: dict):
        if len(dict) == 0:
            return

        print(f"*** {what} ***")
        for name, data in dict.items():
            print(f"name={name}, data={data}")

def main():
    dh_conn = DeephavenConnector()
    dh_conn.establish_session()

    # Open table and server-filter down to one symbol to minimize bandwidth
    table = dh_conn.session.open_table("tb_binance_ohlcv_1m").where('Symbol=="BTCUSDT"')

    listen_handle = listen(table, MyListener())
    # Start processing data in another thread
    listen_handle.start()
    time.sleep(15)  # simulate doing other work for 15 seconds
    listen_handle.stop()

if __name__ == "__main__":
    print("Start DH ticking test")
    main()
    print("Done")
