"""
TradingBot using DeephavenConnector for table subscription and event handling.
"""
import time
from connectors.deephaven_connector import DeephavenConnector, FeedListener
from pydeephaven import listen


class TradingBot:
    """
    A trading bot that subscribes to a Deephaven table using DeephavenConnector and exposes on_price_update for pricing updates.
    """
    def __init__(self, connector: DeephavenConnector, table_name: str):
        """
        Initialize the TradingBot.
        Args:
            connector (DeephavenConnector): Deephaven connector instance.
            table_name (str): Name of the table to subscribe to.
        """
        self.connector = connector
        self.table_name = table_name
        self.table = None
        self._subscribe()

    def _subscribe(self):
        """
        Establish session and subscribe to the Deephaven table using BotListener and listen.
        """
        self.connector.establish_session()
        self.table = self.connector.open_table(self.table_name)
        self.listen_handle = listen(self.table, FeedListener(self.on_price_update))
        self.listen_handle.start()

    def on_price_update(self, delta_type: str, row: dict):
        """
        Called on each pricing update from the table.
        Args:
            row (dict): The updated row data from the table.
        """
        # Default: print the row. Override in subclass for custom logic.
        print(f"Pricing update ({delta_type}):", row)

    def close(self):
        """
        Stop listening and close the Deephaven session.
        """
        if hasattr(self, "listen_handle"):
            self.listen_handle.stop()
        self.connector.close_session()


if __name__ == "__main__":
    connector = DeephavenConnector()
    table_name = "tb_tv_ohlcv_1m"  # Replace with your actual table name
    bot = TradingBot(connector, table_name)
    try:
        print("TradingBot is running. Press Ctrl+C to exit.")
        time.sleep(15)  # Listen for 15 seconds
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        bot.close()
