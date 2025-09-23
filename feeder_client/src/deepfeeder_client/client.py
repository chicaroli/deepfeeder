from __future__ import annotations
import json
import threading
import time
import inspect
from dataclasses import dataclass
from typing import Callable, Optional, Dict, Any, Iterable
from urllib.parse import urlencode

from websocket import create_connection, WebSocket
from websocket._exceptions import WebSocketTimeoutException, WebSocketConnectionClosedException

# Default WS base for local docker-compose setup
DEFAULT_BASE_WS = "ws://localhost:8083/v1"


OnEvent = Callable[[Dict[str, Any]], None]
OnNewData = Callable[[str, str | None, str, str, str, Iterable[Dict[str, Any]], Dict[str, Any]], None]
# on_new_data(phase, part, provider, schema, symbol, rows, meta)


@dataclass
class Subscription:
    """
    Represents an active WebSocket subscription to a (provider, schema, symbol) stream.

    Use .stop() to terminate the background thread and close the connection.
    """
    provider: str
    schema: str
    symbol: str
    _base_ws: str
    fields: Optional[str] = None
    only_completed: Optional[bool] = None
    since_ns: Optional[int] = None
    _on_event: Optional[OnEvent] = None
    _on_new_data: Optional[OnNewData] = None
    _thread: Optional[threading.Thread] = None
    _timeout_s: float = 60.0

    def __post_init__(self) -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional[WebSocket] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name=f"dfc-{self.provider}-{self.schema}-{self.symbol}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _build_url(self) -> str:
        base = self._base_ws.rstrip("/")
        path = f"/ws/{self.provider}/{self.schema}/{self.symbol}"
        params: Dict[str, Any] = {}
        if self.since_ns is not None:
            params["since_ns"] = str(int(self.since_ns))
        if self.fields:
            params["fields"] = self.fields
        if self.only_completed is not None:
            params["only_completed"] = "true" if self.only_completed else "false"
        qs = urlencode(params)
        return f"{base}{path}?{qs}" if qs else f"{base}{path}"

    def _emit_event(self, env: Dict[str, Any]) -> None:
        if self._on_event:
            try:
                self._on_event(env)
            except Exception:
                pass
        if self._on_new_data and env.get("phase") in {"snapshot", "replay", "live"}:
            rows = env.get("rows") or []
            if rows:
                phase = env.get("phase")
                part = env.get("part")
                provider = env.get("provider")
                schema = env.get("data_schema")
                symbol = env.get("symbol")
                meta = env.get("meta") or {}
                try:
                    # Flexible arity: support 7-arg (full), 4-arg (phase, part, rows, meta), 2-arg (rows, meta)
                    params = inspect.signature(self._on_new_data).parameters
                    n_pos = sum(1 for p in params.values() if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD))
                    if n_pos >= 7:
                        self._on_new_data(phase, part, provider, schema, symbol, rows, meta)
                    elif n_pos == 4:
                        self._on_new_data(phase, part, rows, meta)  # type: ignore[misc]
                    elif n_pos == 2:
                        self._on_new_data(rows, meta)  # type: ignore[misc]
                    else:
                        # Fallback to full signature
                        self._on_new_data(phase, part, provider, schema, symbol, rows, meta)
                except Exception:
                    pass

    def _run(self) -> None:
        url = self._build_url()
        try:
            ws = create_connection(url, timeout=self._timeout_s)
        except Exception as e:
            # Surface a synthetic error event
            self._emit_event({
                "version": 1,
                "phase": "error",
                "message": f"connect failed: {e}",
                "provider": self.provider,
                "data_schema": self.schema,
                "symbol": self.symbol,
            })
            return
        self._ws = ws
        try:
            while not self._stop.is_set():
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    # No data within timeout; continue (native ping/pong keeps connection alive)
                    continue
                except WebSocketConnectionClosedException:
                    # Closed by server
                    break
                except Exception:
                    # Unexpected recv exception; exit loop
                    break
                if raw is None or raw == b"" or raw == "":
                    time.sleep(0.05)
                    continue
                if isinstance(raw, (bytes, bytearray)):
                    try:
                        raw = raw.decode("utf-8")
                    except Exception:
                        continue
                try:
                    env = json.loads(raw)
                except Exception:
                    continue
                self._emit_event(env)
        finally:
            try:
                ws.close()
            except Exception:
                pass
            self._ws = None


class DeepFeederClient:
    """
    High-level client for DeepFeeder fanout API.

    Examples:
        >>> from deepfeeder_client.client import DeepFeederClient
        >>> client = DeepFeederClient("ws://localhost:8083/v1")
        >>> def on_rows(phase, part, provider, schema, symbol, rows, meta):
        ...     print(f"{phase}/{part} rows={len(rows)} ts_seq={meta.get('seq')}")
        >>> sub = client.subscribe("tradingview", "ohlcv_1m", "INDV2025",
        ...                        fields="Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange",
        ...                        only_completed=True,
        ...                        on_new_data=on_rows)
        >>> # ... later
        >>> sub.stop(); client.close()
    """

    def __init__(self, base_ws_url: str = DEFAULT_BASE_WS, *, default_timeout_s: float = 60.0) -> None:
        self._base_ws = base_ws_url.rstrip("/")
        self._default_timeout_s = float(default_timeout_s)
        self._subs: list[Subscription] = []

    def subscribe(
        self,
        provider: str,
        schema: str,
        symbol: str,
        *,
        fields: Optional[str] = None,
        only_completed: Optional[bool] = None,
        since_ns: Optional[int] = None,
        on_event: Optional[OnEvent] = None,
        on_new_data: Optional[OnNewData] = None,
        timeout_s: Optional[float] = None,
    ) -> Subscription:
        """
        Subscribe to a market data stream.

        Args:
            provider: Data provider name (e.g., 'tradingview', 'binance').
            schema: Data schema (e.g., 'ohlcv_1m', 'trades').
            symbol: Instrument symbol.
            fields: Optional comma-separated list of fields to project.
            only_completed: If True, deliver only completed bars (default True for bars, False for ticks if not specified).
            since_ns: Optional resume watermark (epoch ns); if provided, replay emits buffered events newer than this.
            on_event: Callback for every envelope (dict) received.
            on_new_data: Convenience callback for rows in snapshot/replay/live envelopes.
            timeout_s: Per-connection recv timeout (defaults to client's default_timeout_s).

        Returns:
            A Subscription handle; call .stop() to unsubscribe/close.
        """
        sub = Subscription(
            provider=provider,
            schema=schema,
            symbol=symbol,
            _base_ws=self._base_ws,
            fields=fields,
            only_completed=only_completed,
            since_ns=since_ns,
            _on_event=on_event,
            _on_new_data=on_new_data,
            _timeout_s=float(timeout_s) if timeout_s is not None else self._default_timeout_s,
        )
        self._subs.append(sub)
        sub.start()
        return sub

    def close(self) -> int:
        """Stop all active subscriptions and close their connections. Returns number closed."""
        n = 0
        for s in list(self._subs):
            try:
                s.stop()
                n += 1
            except Exception:
                pass
            finally:
                try:
                    self._subs.remove(s)
                except Exception:
                    pass
        return n


if __name__ == "__main__":
    # Simple demo runner using fixed params; adjust as needed
    def main() -> int:
        provider = "tradingview"
        # provider = "binance"
        schema = "ohlcv_1m"
        symbol = "INDV2025"
        # symbol = "BTCUSDT"
        fields = "Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange,BarId"
        since_ns = None  # fixed invalid variable name
        timeout = 60.0
        duration = 3 * 5 * 60.0  # 15 minutes

        only_completed = True

        print(
            f"Connecting stream={provider}/{schema}/{symbol} "
            f"only_completed={only_completed} since_ns={since_ns}"
        )

        def on_rows(
            phase: str,
            part: Optional[str],
            provider: str,
            schema: str,
            symbol: str,
            rows: Iterable[Dict[str, Any]],
            meta: Dict[str, Any],
        ) -> None:
            rows_list = list(rows)
            seq = (meta or {}).get("seq")
            print(f"{phase}/{part} rows={len(rows_list)} seq={seq}")
            for i, r in enumerate(rows_list[:3]):
                try:
                    print("  row[" + str(i) + "]: " + json.dumps(r, ensure_ascii=False))
                except Exception:
                    print(f"  row[{i}]: <unprintable>")
            if len(rows_list) > 3:
                print(f"  ... {len(rows_list) - 3} more rows omitted")

        client = DeepFeederClient(default_timeout_s=float(timeout))
        sub = None
        try:
            sub = client.subscribe(
                provider,
                schema,
                symbol,
                fields=fields,
                only_completed=only_completed,
                since_ns=since_ns,
                on_new_data='on_rows',
                timeout_s=float(timeout),
            )
            t0 = time.time()
            while time.time() - t0 < float(duration):
                time.sleep(0.25)
            return 0
        except KeyboardInterrupt:
            return 0
        finally:
            try:
                if sub is not None:
                    sub.stop()
            finally:
                client.close()

    raise SystemExit(main())
