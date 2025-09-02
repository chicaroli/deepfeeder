from __future__ import annotations
from typing import List, Optional
import json
import time
import random
import websocket  # websocket-client
from threading import Event
import os

from core.contracts import Producer, EventBus, Tick
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
from providers.binance.adapter import trade_json_to_tick  # dict -> Tick

DEBUG_IO = os.getenv("DF_DEBUG_IO", "0") not in ("0", "false", "False")

class BinanceWsProducer(Producer):
    """
    Binance trade stream producer:
      - Opens WS for symbols -> parses messages -> batches Ticks -> bus.publish(batch)
    No Deephaven/table writes here. Fan-out is handled by DH consumer.
    """
    provider = "binance"
    stream = "trades"

    def __init__(self, name: str, symbols: List[str], bus: EventBus,
                 *, batch_size: int = 64, flush_interval_s: float = 0.25):
        self.name = f"{self.provider}:{name}"
        self.symbols = list(symbols)
        self.bus = bus
        self.batch_size = int(batch_size)
        self.flush_interval_s = float(flush_interval_s)

        self._ws: Optional[websocket.WebSocketApp] = None
        self._worker = None
        self._batch: List[Tick] = []
        self._last_flush = time.time()
        self._msg_count = 0
        self._last_error: Optional[str] = None

    # ---- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self.is_alive():
            emit_event("feeder", self.name, "listener", "INFO", "ALREADY_RUNNING", "Producer already running")
            return
        self._worker = spawn("feeder", self.name, "listener", self._run)

    def stop(self) -> None:
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass
        try:
            if self._worker is not None:
                self._worker.stop()
                self._worker.join(timeout=3)
        except Exception:
            pass
        emit_event("feeder", self.name, "listener", "INFO", "STOP", "Producer stopped")

    def is_alive(self) -> bool:
        return self._worker is not None and self._worker.is_alive()

    def join(self, timeout: Optional[float] = None) -> None:
        if self._worker:
            self._worker.join(timeout=timeout)

    # ---- ws / batching -----------------------------------------------------
    def _flush_if_needed(self, force: bool = False) -> None:
        if not self._batch:
            return
        now = time.time()
        if force or len(self._batch) >= self.batch_size or (now - self._last_flush) >= self.flush_interval_s:
            try:
                self.bus.publish(self._batch)
            finally:
                self._batch.clear()
                self._last_flush = now

    def _on_message(self, _ws, message: str) -> None:
        try:
            m = json.loads(message)
            d = m.get("data", m)  # accept wrapper or plain dict
            tick = trade_json_to_tick(d)
            self._batch.append(tick)
            self._msg_count += 1
            self._flush_if_needed(force=False)
            if (self._msg_count % 500) == 0:
                emit_event("feeder", self.name, "listener", "INFO", "PROGRESS",
                           f"Received {self._msg_count} messages")
        except Exception as ex:
            self._last_error = str(ex)
            emit_event("feeder", self.name, "listener", "ERROR", "MSG_ERR", f"Message error: {ex!r}")
            if DEBUG_IO:
                print(f"[DF DEBUG] BinanceWsProducer message error name={self.name} err={ex!r}")

    def _run(self, stop_event: Event) -> None:
        url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s.lower()}@trade" for s in self.symbols)
        backoff = 1.0
        while not stop_event.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    url,
                    on_message=self._on_message,
                    on_error=lambda _ws, e: emit_event("feeder", self.name, "listener", "ERROR", "WS_ERR", f"{e!r}"),
                    on_close=lambda *_: emit_event("feeder", self.name, "listener", "WARN", "WS_CLOSED", "WebSocket closed"),
                    on_open=lambda *_: emit_event("feeder", self.name, "listener", "INFO", "WS_OPEN", "WebSocket open"),
                )
                emit_event("feeder", self.name, "listener", "INFO", "WS_CONNECT", "Connecting to Binance WS",
                           {"symbols": self.symbols})
                self._ws.run_forever(ping_interval=25, ping_timeout=15)
                backoff = 1.0
            except Exception as e:
                self._last_error = str(e)
                emit_event("feeder", self.name, "listener", "ERROR", "WS_LOOP_ERR",
                           f"WebSocket loop error: {e!r}", {"backoff_s": backoff})
                if DEBUG_IO:
                    print(f"[DF DEBUG] BinanceWsProducer loop error name={self.name} err={e!r} backoff={backoff}")
            finally:
                self._ws = None
                self._flush_if_needed(force=True)
                if not stop_event.is_set():
                    time.sleep(backoff + random.uniform(0, 0.5))
                    backoff = min(backoff * 2.0, 15.0)
