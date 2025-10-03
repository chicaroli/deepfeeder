"""
feeder_client.trading_bot: Trading bot framework for DeepFeeder streams.

Contains:
    - Strategy: Protocol for trading strategies.
    - TradingBot: Bot lifecycle and event dispatch for DeepFeeder streams.
"""

from __future__ import annotations

import threading
import time
from queue import Queue
from typing import Optional, Protocol

from feeder_client import DeepFeederClient, Envelope, Bar, Trade
from feeder_client.stream import Stream


# ---- Strategy protocol (optional but handy) ---------------------------------

class Strategy(Protocol):
    """
    Minimal interface a strategy can implement; all methods are optional.

    Attributes:
        name: Optional name for the strategy.
    Methods:
        on_start(bot): Called when the bot starts.
        on_stop(bot): Called when the bot stops.
        on_snapshot(bot, env): Called on snapshot envelope.
        on_snapshot_boundary(bot): Called on snapshot boundary.
        on_bar(bot, bar): Called on new bar.
        on_trade(bot, trade): Called on new trade.
        on_event(bot, env): Called on any envelope event.
    """
    name: Optional[str] = None
    def on_start(self, bot: TradingBot) -> None: ...
    def on_stop(self, bot: TradingBot) -> None: ...
    def on_snapshot(self, bot: TradingBot, env: Envelope) -> None: ...
    def on_snapshot_boundary(self, bot: TradingBot) -> None: ...
    def on_bar(self, bot: TradingBot, bar: Bar) -> None: ...
    def on_trade(self, bot: TradingBot, trade: Trade) -> None: ...
    def on_event(self, bot: TradingBot, env: Envelope) -> None: ...


# ---- TradingBot -------------------------------------------------------------

class TradingBot:
    """
    A trading bot that subscribes to DeepFeeder streams and invokes strategy hooks.

    Key features:
      - Dedicated supervisor thread per bot (lifecycle managed via start/stop).
      - Internal Queue decouples client callback from strategy execution.
      - Simple reconnect with exponential backoff.
      - Pause / resume processing without dropping the subscription.
      - Does NOT auto-close the shared DeepFeederClient unless own_client=True.

    Args:
        deepfeeder_cli: DeepFeederClient instance.
        stream: Stream configuration.
        strategy: Optional strategy implementing the Strategy protocol.
    """

    def __init__(
        self,
        deepfeeder_cli: DeepFeederClient,
        stream: Stream,
        strategy: Optional[Strategy] = None,
        *,
        queue_maxsize: int = 10_000,
        auto_start: bool = True,
    ):
        self.stream = stream
        self.client = deepfeeder_cli
        self.strategy = strategy
        self.name = strategy.name if strategy and strategy.name else f"GenericBot#{int(time.time())}"

        # lifecycle / concurrency
        self._q: Queue[Envelope|None] = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stream_handle = None
        self._lock = threading.RLock()

        if auto_start:
            self.start()

    # --- Public API ----------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._paused.clear()
            self._thread = threading.Thread(
                target=self._run, name=self.name, daemon=True
            )
            self._thread.start()

    def stop(self, join: bool = True, timeout: Optional[float] = 10.0) -> None:
        with self._lock:
            self._stop.set()
            self._paused.clear()
            self._unsubscribe_silent()
            try:
                self._q.put_nowait(None)    # wake consumer via sentinel
            except Exception:
                pass
        if join and self._thread:
            self._thread.join(timeout=timeout)
        if self.strategy:
            try:
                self.strategy.on_stop(self)
            except Exception as e:
                print(f"[{self.name}] strategy.on_stop error: {e}")

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and not self._stop.is_set()

    def is_paused(self) -> bool:
        return self._paused.is_set()

    def __enter__(self) -> TradingBot:
        if not self.is_running():
            self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop(join=True)

    # --- Internal: supervisor runner ----------------------------------------

    def _run(self) -> None:
        if self.strategy:
            try:
                self.strategy.on_start(self)
            except Exception as e:
                print(f"[{self.name}] strategy.on_start error: {e}")

        backoff = 0.5
        while not self._stop.is_set():
            try:
                # ensure stream is subscribed
                if self._stream_handle is None:
                    self._subscribe()
                backoff = 0.5           # reset backoff after a successful subscribe

                env = self._q.get()     # blocks until callback enqueues -> immediate wakeup
                if env is None:         # sentinel for shutdown
                    break

                # Pause gate (blocks here until resume)
                if self._paused.is_set():
                    self._paused.wait()

                self._dispatch(env)

            except Exception as e:
                print(f"[{self.name}] run-loop error: {e}")
                # if anything blows up, drop the handle and retry after backoff
                self._unsubscribe_silent()
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)

        self._unsubscribe_silent()

    # --- Subscription and callback ------------------------------------------

    def _subscribe(self) -> None:
        self._stream_handle = self.client.subscribe(
            **self.stream.to_subscribe_kwargs(),
            on_data=self._on_data,
        )
        print(f"[{self.name}] Subscribed {self.stream.key} fields={self.stream.fields or 'default'}")

    def _unsubscribe_silent(self) -> None:
        if self._stream_handle is not None:
            try:
                self.client.unsubscribe(self._stream_handle)
            except Exception as e:
                print(f"[{self.name}] Unsubscribe error: {e}")
            finally:
                self._stream_handle = None

    def _on_data(self, env: Envelope) -> None:
        # Handoff from client thread -> consumer thread (immediate wake)
        try:
            self._q.put_nowait(env)
        except Exception:
            # Drop-oldest policy to avoid blocking the client's I/O thread
            try:
                _ = self._q.get_nowait()
                self._q.put_nowait(env)
            except Exception:
                pass

    # --- Dispatch to strategy ------------------------------------------------

    def _dispatch(self, env: Envelope) -> None:
        if not self.strategy:
            # Default behavior: print something useful and return
            print(f"[{self.name}] Event: {env}")
            return

        try:
            # Common hook for phases if present
            if env.is_snapshot:
                self.strategy.on_snapshot(self, env)
                return

            if env.is_boundary:
                self.strategy.on_snapshot_boundary(self)

            if env.is_bar and env.rows:
                for row in env.rows:
                    # If you have a Bar dataclass, cast or validate here
                    self.strategy.on_bar(self, row)  # type: ignore[arg-type]
            elif env.is_trade and env.rows:
                for row in env.rows:
                    self.strategy.on_trade(self, row)
            elif env.rows:
                # Fallback catch-all
                self.strategy.on_event(self, env)

        except AttributeError:
            # Strategy may not implement a method; ignore gracefully
            pass
        except Exception as e:
            print(f"[{self.name}] dispatch error: {e}")


# ---- Example usage ----------------------------------------------------------

if __name__ == "__main__":
    class PrintStrategy(Strategy):
        name = "PrintStrategy"

        def on_start(self, bot: TradingBot) -> None:
            print(f"[on_start] {self.name} started.")

        def on_stop(self, bot: TradingBot) -> None:
            print(f"[on_stop] {self.name} stopped.")

        def on_snapshot(self, bot: TradingBot, env: Envelope) -> None:
            if env.rows:
                print(f"[on_shapshot]: rows={len(env.rows)}")
                for row in env.rows[:min(5, len(env.rows) - 5)]:
                    print(f"... {row}")
                    # print(env.row_type, env.phase, env.part, row.symbol, row.timestamp, row.close)

        def on_snapshot_boundary(self, bot: TradingBot) -> None:
            print(f"[on_snapshot_boundary] {self.name} snapshot boundary reached.")

        def on_bar(self, bot: TradingBot, bar: Bar) -> None:
            # Replace with real logic
            print(f"[on_bar][{self.name}] {bar}")


    cli = DeepFeederClient()
    tbot = TradingBot(
        deepfeeder_cli=cli,
        stream=Stream.bars(
            provider="tradingview",
            schema="ohlcv_1m",
            symbol="INDV2025",
            exchange="BMFBOVESPA",
            fields=["Timestamp", "Open", "High", "Low", "Close", "Volume", "Symbol", "Exchange", "BarId"],
            only_completed=True,
        ),
        strategy=PrintStrategy(),
    )

    try:
        print("TradingBot is running. Press Ctrl+C to exit.")
        time.sleep(5 * 60)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        tbot.stop(join=True)
