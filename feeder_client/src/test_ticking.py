import pyarrow as pa
import time
from pydeephaven import Session, TableListener, TableUpdate, listen


class MyListener(TableListener):
    def on_update(self, update: TableUpdate) -> None:
        self._show_deltas("removes", update.removed())
        self._show_deltas("adds", update.added())
        self._show_deltas("modified-prev", update.modified_prev())
        self._show_deltas("modified", update.modified())

    def on_error(self, error: Exception):
        print(f"Error happened: {error}")

    def _show_deltas(self, what: str, dict: dict[str, pa.Array]):
        if len(dict) == 0:
            return

        print(f"*** {what} ***")
        for name, data in dict.items():
            print(f"name={name}, data={data}")


def main():
    sess = Session(host="deephaven")

    # Open table and server-filter down to one symbol to minimize bandwidth
    table = sess.open_table("tb_binance_trades").where('Symbol=="BTCUSDT"')

    listen_handle = listen(table, MyListener())
    # Start processing data in another thread
    listen_handle.start()
    time.sleep(15)  # simulate doing other work for 15 seconds
    listen_handle.stop()

if __name__ == "__main__":
    print(f"Start DH ticking test")
    main()
    print("Done")
