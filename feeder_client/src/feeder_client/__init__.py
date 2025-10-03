"""
feeder_client: Python client for DeepFeeder real-time market data streaming.

Exports:
    - DeepFeederClient: Main client for connecting and subscribing to data streams.
    - DeepFeederStream: Per-stream worker for managing subscriptions.
    - Envelope, Bar, Trade: Data models for market data.
    - adapt_envelope: Utility for adapting raw envelopes.
    - Stream: Stream configuration helper.
    - Strategy, TradingBot: Trading bot framework.
"""

from .client import DeepFeederClient, DeepFeederStream
from .models import Envelope, Bar, Trade, adapt_envelope
from .stream import Stream
from .trading_bot import Strategy, TradingBot

__all__ = [
    "DeepFeederClient",
    "DeepFeederStream",
    "Envelope",
    "Bar",
    "Trade",
    "adapt_envelope",
    "Stream",
    "Strategy",
    "TradingBot",
]

__version__ = "0.1.0"