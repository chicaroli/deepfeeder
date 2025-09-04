# providers/tradingview/producer_ws.py
from __future__ import annotations

import json
import time
import random
from typing import Dict, List, Optional, Tuple
from threading import Event

import websocket  # websocket-client

from core.contracts import Producer, EventBus, Tick
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event

from providers.tradingview.adapter import tv_quote_ws_to_tick


WS_URL = "wss://data.tradingview.com/socket.io/websocket"

def _frame(msg: dict) -> str:
    s = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(s)}~m~{s}"

def _iter_frames(payload: str):
    while payload.startswith("~m~"):
        try:
            _, rest = payload.split("~m~", 1)
            ln_str, rest = rest.split("~m~", 1)
            ln = int(ln_str)
            yield rest[:ln]
            payload = rest[ln:]
        except Exception:
            return

def _split_exchange_symbol(raw: str) -> Tuple[Optional[str], str]:
    raw = (raw or "").strip()
    if not raw:
        return None, raw
    parts = raw.split(":", 1)
    if len(parts) == 2:
        return parts[0].upper(), parts[1].upper()
    return None, raw.upper()


class _SymState:
    __slots__ = ("lp_time_ns", "last_price", "bid", "ask", "volume", "change", "change_pct", "last_volume")
    def __init__(self):
        self.lp_time_ns: Optional[int] = None
        self.last_price: Optional[float] = None
        self.bid: Optional[float] = None
        self.ask: Optional[float] = None
        self.volume: Optional[float] = None
        self.change: Optional[float] = None
        self.change_pct: Optional[float] = None
        self.last_volume: Optional[float] = None


class TradingViewWsProducer(Producer):
    """
    TradingView quotes stream producer:
      - Opens WS session, subscribes symbols
      - Parses qsd updates, keeps minimal per-symbol state (for VolDelta)
      - Converts each update to Tick (tv_quote_ws_to_tick)
      - Batches and publishes to EventBus
    No DH writes here; fan-out is handled by your DH consumer.
    """
    provider = "tradingview"
    stream = "quotes"

    def __init__(
        self,
        name: str,
        symbols: List[str],
        bus: EventBus,
        *,
        batch_size: int = 64,
        flush_interval_s: float = 0.25,
    ):
        self.name = name
        # Keep original subscription tokens (EXCH:SYMBOL or SYMBOL), but track parsed meta
        self._subs: List[str] = [s.strip() for s in symbols]
        self._sym_meta: Dict[str, Tuple[Optional[str], str]] = {}  # subscribe_key -> (exchange, symbol)
        for s in self._subs:
            exch, sym = _split_exchange_symbol(s)
            # TV expects the subscribe key in EXCH:SYMBOL if exch exists, else SYMBOL
            subscribe_key = f"{exch}:{sym}" if exch else sym
            self._sym_meta[subscribe_key] = (exch, sym)
        self.bus = bus
        self.batch_size = int(batch_size)
        self.flush_interval_s = float(flush_interval_s)

        self._ws: Optional[websocket.WebSocketApp] = None
        self._worker = None
        self._batch: List[Tick] = []
        self._last_flush = time.time()
        self._msg_count = 0
        self._last_error: Optional[str] = None

        self._sid = f"qs_{int(time.time() * 1000)}"
        self._state: Dict[str, _SymState] = {k: _SymState() for k in self._sym_meta.keys()}

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

    # ---- batching ----------------------------------------------------------
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

    # ---- tv protocol helpers ----------------------------------------------
    def _subscribe(self, ws: websocket.WebSocketApp) -> None:
        # TV "unauthorized_user_token" is fine for public quotes field set
        ws.send(_frame({"m": "set_auth_token", "p": ["unauthorized_user_token"]}))
        ws.send(_frame({"m": "quote_create_session", "p": [self._sid]}))
        # Keep the field set aligned with schema.py usage
        ws.send(_frame({"m": "quote_set_fields", "p": [self._sid, "lp", "bid", "ask", "volume", "ch", "chp", "lp_time"]}))
        subs = list(self._sym_meta.keys())
        for s in subs:
            ws.send(_frame({"m": "quote_add_symbols", "p": [self._sid, s]}))
        ws.send(_frame({"m": "quote_fast_symbols", "p": [self._sid, ",".join(subs)]}))

    def _on_message(self, _ws, raw: str) -> None:
        # TradingView server aggregates messages in "~m~len~m~{...}" frames
        for fr in _iter_frames(raw):
            if fr.startswith("~h~"):  # heartbeat
                continue
            try:
                obj = json.loads(fr)
            except Exception:
                continue
            if obj.get("m") == "qsd":
                self._handle_qsd(obj)

    def _handle_qsd(self, obj: dict) -> None:
        p = obj.get("p", [])
        if len(p) < 2:
            return
        updates = p[1]
        if isinstance(updates, dict):
            updates = [updates]

        for it in updates:
            sub_key = str(it.get("n", "")).strip()  # subscription key as sent by TV
            v = it.get("v") or {}
            if not sub_key or sub_key not in self._state:
                continue

            st = self._state[sub_key]
            exch, sym = self._sym_meta[sub_key]

            # tv lp_time is seconds epoch (float/integer); adapter will multiply to ns if we pass through
            lp_time = v.get("lp_time")
            if lp_time is not None:
                try:
                    # store as ns to be consistent with Tick.ts_ns
                    st.lp_time_ns = int(float(lp_time) * 1_000_000_000)
                except Exception:
                    st.lp_time_ns = None
            elif st.lp_time_ns is None:
                # fallback: arrival time in ns
                st.lp_time_ns = time.time_ns()

            # update state from fields
            if "lp" in v and v["lp"] is not None:
                try: st.last_price = float(v["lp"])
                except Exception: pass
            if "bid" in v and v["bid"] is not None:
                try: st.bid = float(v["bid"])
                except Exception: pass
            if "ask" in v and v["ask"] is not None:
                try: st.ask = float(v["ask"])
                except Exception: pass
            if "volume" in v and v["volume"] is not None:
                try: st.volume = float(v["volume"])
                except Exception: pass
            if "ch" in v and v["ch"] is not None:
                try: st.change = float(v["ch"])
                except Exception: pass
            if "chp" in v and v["chp"] is not None:
                try: st.change_pct = float(v["chp"])
                except Exception: pass

            # derive vol_delta
            vol_delta: Optional[float] = None
            if st.volume is not None:
                if st.last_volume is None:
                    vol_delta = 0.0
                else:
                    d = st.volume - st.last_volume
                    vol_delta = d if d > 0 else 0.0
                st.last_volume = st.volume

            # only material updates
            material = (
                (st.last_price is not None) or
                (st.bid is not None) or
                (st.ask is not None) or
                (vol_delta is not None and vol_delta > 0.0)
            )
            if not material:
                continue

            # build Tick (adapter also normalizes/guards)
            try:
                tick = tv_quote_ws_to_tick(
                    exchange=exch,
                    symbol_raw=sym,
                    v={
                        "lp_time": lp_time,         # seconds (may be None; we pass ts_ns_override)
                        "lp": st.last_price,
                        "bid": st.bid,
                        "ask": st.ask,
                        "volume": st.volume,
                        "ch": st.change,
                        "chp": st.change_pct,
                    },
                    ts_ns_override=st.lp_time_ns,  # authoritative ts in ns
                    vol_delta=vol_delta if vol_delta is not None else 0.0,
                )
                self._batch.append(tick)
                self._msg_count += 1
                self._flush_if_needed(force=False)
                if (self._msg_count % 100000) == 0:
                    emit_event("feeder", self.name, "listener", "INFO", "PROGRESS",
                               f"Received {self._msg_count} updates")
            except Exception as ex:
                self._last_error = str(ex)
                emit_event("feeder", self.name, "listener", "ERROR", "MSG_ERR", f"qsd parse error: {ex!r}")

    # ---- ws loop -----------------------------------------------------------
    def _run(self, stop_event: Event) -> None:
        backoff = 1.0
        while not stop_event.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=lambda ws: (self._subscribe(ws),
                                        emit_event("feeder", self.name, "listener", "INFO", "WS_OPEN", "WebSocket open")),
                    on_message=self._on_message,
                    on_error=lambda _ws, e: emit_event("feeder", self.name, "listener", "ERROR", "WS_ERR", f"{e!r}"),
                    on_close=lambda *_: emit_event("feeder", self.name, "listener", "WARN", "WS_CLOSED", "WebSocket closed"),
                )
                emit_event("feeder", self.name, "listener", "INFO", "WS_CONNECT", "Connecting to TradingView WS",
                           {"symbols": self._subs})
                self._ws.run_forever(ping_interval=25, ping_timeout=15)
                backoff = 1.0
            except Exception as e:
                self._last_error = str(e)
                emit_event("feeder", self.name, "listener", "ERROR", "WS_LOOP_ERR",
                           f"WebSocket loop error: {e!r}", {"backoff_s": backoff})
            finally:
                self._ws = None
                self._flush_if_needed(force=True)
                if not stop_event.is_set():
                    time.sleep(backoff + random.uniform(0, 0.5))
                    backoff = min(backoff * 2.0, 15.0)
