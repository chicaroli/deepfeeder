import os
from dotenv import load_dotenv
load_dotenv()

class DeephavenConnector:
    def __init__(self, host: str = None, port: int = None):
        self.host = host or os.getenv("DEEPHAVEN_HOST", "localhost")
        self.port = port or int(os.getenv("DEEPHAVEN_PORT", "10000"))
        self.session = None

    def establish_session(self):
        from pydeephaven import Session
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
