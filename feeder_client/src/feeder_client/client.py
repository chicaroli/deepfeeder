from __future__ import annotations
import json
import threading
import time
import random
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any, Sequence, Union
from urllib.parse import urlencode

from websocket import create_connection, WebSocket
from websocket._exceptions import WebSocketTimeoutException, WebSocketConnectionClosedException

from .models import Envelope, adapt_envelope
from .protocol import ENVELOPE_VERSION

DEFAULT_BASE_WS = "ws://localhost:8083/v1"

OnData = Callable[[Envelope], None]


def _stream_id(provider: str, schema: str, symbol: str, exchange: Optional[str] = None) -> str:
    # Include exchange in the id when present to avoid collisions across venues
    return f"{provider}|{schema}|{symbol}" + (f"@{exchange.upper()}" if exchange else "")


@dataclass
class DeepFeederStream:
    """
    Public per-stream worker with a simple lifecycle (start/stop) and resume helpers.
    Users normally create this via DeepFeederClient.subscribe(...).
    """
    provider: str
    schema: str
    symbol: str
    base_ws: str
    fields: Optional[str] = None
    only_completed: Optional[bool] = None
    since_ns: Optional[int] = None
    exchange: Optional[str] = None
    on_data: Optional[OnData] = None
    timeout_s: float = 60.0
    idle_timeout_s: float = 45.0

    # runtime
    ws: Optional[WebSocket] = None
    thread: Optional[threading.Thread] = None
    stop_evt: threading.Event = threading.Event()
    last_watermark_ns: Optional[int] = None
    last_seq: Optional[int] = None
    last_frame_ts: float = 0.0
    reconnects: int = 0

    def __post_init__(self) -> None:
        self.stop_evt = threading.Event()

    @property
    def id(self) -> str:
        return _stream_id(self.provider, self.schema, self.symbol, self.exchange)

    def is_running(self) -> bool:
        return bool(self.thread and self.thread.is_alive())

    def start(self) -> None:
        if self.is_running():
            return
        self.stop_evt.clear()
        self.thread = threading.Thread(
            target=self._run,
            name=f"dfc-{self.provider}-{self.schema}-{self.symbol}" + (f"-{self.exchange}" if self.exchange else ""),
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_evt.set()
        try:
            if self.ws is not None:
                self.ws.close()
        except Exception:
            pass
        if self.thread:
            self.thread.join(timeout=5)
            self.thread = None

    # --------- internals ---------

    def _build_url(self) -> str:
        base = self.base_ws.rstrip("/")
        path = f"/ws/{self.provider}/{self.schema}/{self.symbol}"
        params: Dict[str, Any] = {}
        if self.since_ns is not None:
            params["since_ns"] = str(int(self.since_ns))
        if self.fields:
            params["fields"] = self.fields
        if self.only_completed is not None:
            params["only_completed"] = "true" if self.only_completed else "false"
        if self.exchange:
            params["exchange"] = self.exchange
        qs = urlencode(params)
        return f"{base}{path}?{qs}" if qs else f"{base}{path}"

    def _handle_envelope(self, raw_env: Dict[str, Any]) -> None:
        # Track resume hints
        try:
            if "watermark_ns" in raw_env and raw_env["watermark_ns"] is not None:
                self.last_watermark_ns = int(raw_env["watermark_ns"])
            meta = raw_env.get("meta") or {}
            if "seq" in meta:
                self.last_seq = int(meta["seq"])
        except Exception:
            pass
        self.last_frame_ts = time.time()

        if self.on_data:
            try:
                env = adapt_envelope(raw_env)
                self.on_data(env)
            except Exception:
                # swallow callback errors; keep the stream alive
                pass

    def _run(self) -> None:
        url = self._build_url()
        backoff = 1.0
        while not self.stop_evt.is_set():
            try:
                ws = create_connection(url, timeout=self.timeout_s)
            except Exception as e:
                # deliver an error-shaped envelope to on_data for observability
                if self.on_data:
                    try:
                        self.on_data(adapt_envelope({
                            "version": ENVELOPE_VERSION,
                            "phase": "error",
                            "part": None,
                            "provider": self.provider,
                            "data_schema": self.schema,
                            "symbol": self.symbol,
                            "rows": [],
                            "meta": {"message": f"connect failed: {e}"},
                        }))
                    except Exception:
                        pass
                sleep_s = min(backoff, 30.0) * (0.8 + 0.4 * random.random())
                time.sleep(sleep_s)
                backoff = min(backoff * 2.0, 30.0)
                continue

            self.ws = ws
            self.reconnects += 1
            backoff = 1.0
            self.last_frame_ts = time.time()

            try:
                while not self.stop_evt.is_set():
                    if self.idle_timeout_s and (time.time() - self.last_frame_ts) > float(self.idle_timeout_s):
                        break
                    try:
                        raw = ws.recv()
                    except WebSocketTimeoutException:
                        continue
                    except WebSocketConnectionClosedException:
                        break
                    except Exception:
                        break

                    if raw is None or raw == b"" or raw == "":
                        time.sleep(0.02)
                        continue
                    if isinstance(raw, (bytes, bytearray)):
                        try:
                            raw = raw.decode("utf-8")
                        except Exception:
                            continue
                    try:
                        raw_env = json.loads(raw)
                    except Exception:
                        continue
                    self._handle_envelope(raw_env)
            finally:
                try:
                    ws.close()
                except Exception:
                    pass
                self.ws = None
                # loop to reconnect


# ======================= Public façade =======================

class DeepFeederClient:
    """
    Facade that manages multiple DeepFeederStream instances.
    """

    def __init__(self, base_ws_url: str = DEFAULT_BASE_WS, *, default_timeout_s: float = 60.0, default_idle_timeout_s: float = 45.0) -> None:
        self._base_ws = base_ws_url.rstrip("/")
        self._default_timeout_s = float(default_timeout_s)
        self._default_idle_timeout_s = float(default_idle_timeout_s)
        self._streams: Dict[str, DeepFeederStream] = {}
        self._lock = threading.RLock()

    def subscribe(
        self,
        provider: str,
        schema: str,
        symbol: str,
        *,
        fields: Optional[str] = None,
        only_completed: Optional[bool] = None,
        since_ns: Optional[int] = None,
        exchange: Optional[str] = None,
        on_data: Optional[OnData] = None,
        timeout_s: Optional[float] = None,
        idle_timeout_s: Optional[float] = None,
    ) -> DeepFeederStream:
        """
        Start (or replace) a stream and return a DeepFeederStream handle.
        """
        stream = DeepFeederStream(
            provider=provider,
            schema=schema,
            symbol=symbol,
            base_ws=self._base_ws,
            fields=fields,
            only_completed=only_completed,
            since_ns=since_ns,
            exchange=exchange,
            on_data=on_data,
            timeout_s=float(timeout_s) if timeout_s is not None else self._default_timeout_s,
            idle_timeout_s=float(idle_timeout_s) if idle_timeout_s is not None else self._default_idle_timeout_s,
        )
        sid = stream.id
        with self._lock:
            old = self._streams.pop(sid, None)
            if old:
                try: old.stop()
                except Exception: pass
            self._streams[sid] = stream
        stream.start()
        return stream

    def unsubscribe(self, stream: Union[str, DeepFeederStream]) -> bool:
        """
        Stop a stream by id or by DeepFeederStream instance. Returns True if stopped.
        """
        sid = stream.id if isinstance(stream, DeepFeederStream) else str(stream)
        with self._lock:
            obj = self._streams.pop(sid, None)
        if not obj:
            return False
        try:
            obj.stop()
        except Exception:
            pass
        return True

    def unsubscribe_key(self, provider: str, schema: str, symbol: str, exchange: Optional[str] = None) -> bool:
        return self.unsubscribe(_stream_id(provider, schema, symbol, exchange))

    def close(self) -> int:
        """Stop all streams. Returns the number closed."""
        with self._lock:
            items = list(self._streams.items())
            self._streams.clear()
        n = 0
        for _, s in items:
            try:
                s.stop()
                n += 1
            except Exception:
                pass
        return n

    # ---- introspection ----

    def streams(self) -> Sequence[DeepFeederStream]:
        """List active streams."""
        with self._lock:
            return list(self._streams.values())

    def last_watermark(self, stream: Union[str, DeepFeederStream]) -> Optional[int]:
        sid = stream.id if isinstance(stream, DeepFeederStream) else str(stream)
        with self._lock:
            s = self._streams.get(sid)
            return s.last_watermark_ns if s else None

    def last_seq(self, stream: Union[str, DeepFeederStream]) -> Optional[int]:
        sid = stream.id if isinstance(stream, DeepFeederStream) else str(stream)
        with self._lock:
            s = self._streams.get(sid)
            return s.last_seq if s else None
