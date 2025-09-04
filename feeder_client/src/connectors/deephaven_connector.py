# filepath: feeder_client/src/connectors/deephaven_connector.py

from dotenv import load_dotenv
import os
import pandas as pd
from pydeephaven import Session, TableListener, TableUpdate

load_dotenv()

class DeephavenConnector:
    def __init__(self, host: str = None, port: int = None):
        self.host = host or os.getenv("DEEPHAVEN_HOST", "localhost")
        self.port = port or int(os.getenv("DEEPHAVEN_PORT", "10000"))
        self.session = None

    def establish_session(self):
        self.session = Session(host=self.host, port=self.port)

    def open_table(self, table_name: str):
        if self.session is None:
            raise RuntimeError("Session not established. Call establish_session() first.")
        return self.session.open_table(table_name)

    def close_session(self):
        if self.session is not None:
            self.session.close()
            self.session = None

    def __enter__(self):
        self.establish_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_session()


class FeedListener(TableListener):
    def __init__(self, callback=None):
        """
        Generic listener for Deephaven table updates.
        Args:
            callback (Optional[Callable[[str, dict], None]]): Function called for each delta type and row.
        """
        super().__init__()
        self.callback = callback

    def on_update(self, update: TableUpdate):
        self._show_deltas("removed", update.removed())
        self._show_deltas("added", update.added())
        self._show_deltas("modified_prev", update.modified_prev())
        self._show_deltas("modified", update.modified())
        # Optionally call callback for each added row
        if self.callback:
            df = pd.DataFrame({name: data.to_pylist() for name, data in update.added().items()})
            self.callback("adds", df)

    def on_error(self, error: Exception):
        print(f"Error happened: {error}")

    def _show_deltas(self, what: str, dict_: dict):
        if not dict_:
            return

        # Convert Arrow Arrays to lists for DataFrame construction
        df = pd.DataFrame({name: data.to_pylist() for name, data in dict_.items()})
        print(f"*** {what} ***\n{df}")
