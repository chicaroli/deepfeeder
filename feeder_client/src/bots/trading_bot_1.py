# filepath: feeder_client/src/bots/trading_bot_1.py
"""
TradingBot using DeephavenConnector for table subscription and event handling.
"""
import time
from typing import Optional
from feeder_client import DeepFeederClient, Envelope, Bar


class TradingBot:
    """
    A trading bot that subscribes to a Deepfeeder stream usingDeepFeederClient and exposes on_price_update for pricing updates.
    """
    def __init__(
            self,
            deepfeeder_cli: DeepFeederClient,
            provider: str,
            schema: str,
            symbol: str,
            *,
            exchange: Optional[str] = None,
            fields: Optional[str] = None,
            only_completed: Optional[bool] = None,
    ):
        """
        Initialize the TradingBot.
        Args:
            deepfeeder_cli (DeephavenConnector): Deephaven connector instance.
            provider (str): Data provider name.
            schema (str): Data schema name.
            symbol (str): Trading symbol.
            exchange (Optional[str]): Exchange name.
            fields (Optional[str]): Comma-separated list of fields to subscribe to.
            only_completed (Optional[bool]): Whether to receive only completed bars.
        """
        self.deepfeeder_cli = deepfeeder_cli
        self.provider = provider
        self.schema = schema
        self.symbol = symbol
        self.exchange = exchange
        self.fields = fields
        self.only_completed = only_completed
        self._subscribe()

    def _subscribe(self):
        """
        Establish session and subscribe to the Deephaven table using BotListener and listen.
        """
        self.stream = self.deepfeeder_cli.subscribe(
            provider=self.provider,
            schema=self.schema,
            symbol=self.symbol,
            exchange=self.exchange,
            fields=self.fields,
            only_completed=self.only_completed,
            on_data=self.on_price_update,
        )
        print(f"Subscribed to {self.provider} {self.schema} {self.symbol}")

    @staticmethod
    def on_price_update(env: Envelope) -> None:
        """
        Called on each pricing update from the stream.
        Args:
        env (Envelope): The data envelope containing pricing updates.
        """
        print(f"Pricing update ({env}):")

    def close(self):
        """
        Stop listening and close the Deephaven session.
        """
        self.deepfeeder_cli.unsubscribe(self.stream)
        self.deepfeeder_cli.close()
        print("TradingBot closed.")


if __name__ == "__main__":
    cli = DeepFeederClient()

    bot = TradingBot(
        deepfeeder_cli=cli,
        provider="tradingview",
        schema="ohlcv_1m",
        symbol="INDV2025",
        exchange="BMFBOVESPA",
        fields="Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange,BarId",
        only_completed=True,
    )
    try:
        print("TradingBot is running. Press Ctrl+C to exit.")
        time.sleep(5*60)  # Listen for a while
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        bot.close()
