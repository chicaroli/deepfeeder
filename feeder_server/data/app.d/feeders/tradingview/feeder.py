# ingest.feeders.providers.tradingview.feeder
from __future__ import annotations
import json, time, collections
from typing import Dict, List, Any, Optional, Tuple, Deque
from datetime import datetime, timezone
from threading import Lock as _Lock

import websocket
from deephaven.time import to_j_instant

from feeders.base import BaseFeeder
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
from .schema import tv_quotes_writer
from .config import load_config

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

def _to_instant_from_epoch_s(x: Optional[float]):
    if x is None:
        return None
    try:
        return to_j_instant(datetime.fromtimestamp(int(x), tz=timezone.utc))
    except Exception:
        return None

def _split_exchange_ticker(s: str) -> Tuple[Optional[str], str]:
    s = (s or "").strip()
    if ":" in s:
        exch, tick = s.split(":", 1)
        return (exch.strip().upper() or None), tick.strip().upper()
    return None, s.upper()

class _SymState:
    __slots__ = ("t", "lp", "bid", "ask", "vol", "ch", "chp", "last_vol")
    def __init__(self):
        self.t = None
        self.lp = None
        self.bid = None
        self.ask = None
        self.vol = None
        self.ch = None
        self.chp = None
        self.last_vol = None

class TradingViewFeeder(BaseFeeder):
    def __init__(self, name: str, symbols: List[str]):
        super().__init__("tradingview", name, symbols)
        # Decoupled listener / writer pattern (mirrors Binance feeder)
        self.listener_worker = None
        self.writer_worker = None
        self.ws = None
        self._writer = tv_quotes_writer()
        # Queue & batching (moderate defaults; TradingView update rate is lower than raw trades)
        self._q: Deque[tuple] = collections.deque(maxlen=5000)
        self._q_lock = _Lock()
        # Config
        cfg = load_config()
        self._cfg = cfg
        self._batch_size = cfg.batch_size
        self._flush_interval_s = cfg.flush_interval_s
        # Metrics
        self._last_flush_ts = 0.0
        self._avg_handler_ms = 0.0
        self._last_metrics_emit = 0.0
        self._metrics_interval = cfg.metrics_interval
        self._metrics_enabled = cfg.metrics_enabled
        self._metrics_min_q_delta = cfg.metrics_min_q_delta
        self._last_metrics_snapshot = (0, 0.0)  # (q_len, avg_handler_ms)
        # Symbol meta/state
        self._sym_meta: Dict[str, Tuple[Optional[str], str, str]] = {}
        for raw in self.symbols:
            exch, tick = _split_exchange_ticker(raw)
            subscribe = f"{exch}:{tick}" if exch else tick
            self._sym_meta[subscribe.lower()] = (exch, tick, subscribe)
        self._state: Dict[str, _SymState] = {k: _SymState() for k in self._sym_meta.keys()}
        self._last_fingerprint: Dict[str, tuple] = {}
        self._sid = f"qs_{int(time.time() * 1000)}"

    def is_alive(self) -> bool:
        return self.listener_worker is not None and self.listener_worker.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return "already running"
        self.started_at = time.time()
        self.last_error = None
        self.writer_worker = spawn("feeder", f"{self.provider}:{self.name}", "writer", self._writer_loop)
        self.listener_worker = spawn("feeder", f"{self.provider}:{self.name}", "listener", self._run)
        try:
            emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        except Exception:
            pass
        self.emit_status(force=True)
        return "started"

    def stop(self):
        try:
            if self.listener_worker is not None:
                stop_method = getattr(self.listener_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        try:
            if self.writer_worker is not None:
                stop_method = getattr(self.writer_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive() and self.listener_worker is not None:
            self.listener_worker.join(timeout=3)
        try:
            emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "STOP", "Feeder stopping")
        except Exception:
            pass
        self.emit_status(force=True)
        return "stopped"

    def _subscribe(self, ws):
        ws.send(_frame({"m": "set_auth_token", "p": ["unauthorized_user_token"]}))
        ws.send(_frame({"m": "quote_create_session", "p": [self._sid]}))
        ws.send(_frame({"m": "quote_set_fields", "p": [self._sid, "lp", "bid", "ask", "volume", "ch", "chp", "lp_time"]}))
        subs = [meta[2] for meta in self._sym_meta.values()]
        for s in subs:
            ws.send(_frame({"m": "quote_add_symbols", "p": [self._sid, s]}))
        ws.send(_frame({"m": "quote_fast_symbols", "p": [self._sid, ",".join(subs)]}))

    def _on_message(self, raw: str):
        for fr in _iter_frames(raw):
            if fr.startswith("~h~"):
                continue
            try:
                obj = json.loads(fr)
            except Exception:
                continue
            if obj.get("m") == "qsd":
                self._handle_qsd(obj)

    def _handle_qsd(self, obj: dict):
        ps = obj.get("p", [])
        if len(ps) < 2:
            return
        updates = ps[1]
        if isinstance(updates, dict):
            updates = [updates]
        for it in updates:
            sym_raw = str(it.get("n", "")).strip()
            sym_key = sym_raw.lower()
            v = it.get("v") or {}
            if not sym_key or sym_key not in self._state:
                continue
            st = self._state[sym_key]
            exch, tick, _subscribe = self._sym_meta[sym_key]
            new_t = _to_instant_from_epoch_s(v.get("lp_time"))
            if new_t is not None:
                if st.t is not None and new_t < st.t:
                    continue
                st.t = new_t
            arrival_needed = (st.t is None) and any(k in v and v[k] is not None for k in ("lp", "bid", "ask"))
            if arrival_needed:
                st.t = to_j_instant(datetime.now(timezone.utc))
            if "lp" in v: st.lp = float(v["lp"]) if v["lp"] is not None else None
            if "bid" in v: st.bid = float(v["bid"]) if v["bid"] is not None else None
            if "ask" in v: st.ask = float(v["ask"]) if v["ask"] is not None else None
            if "volume" in v: st.vol = float(v["volume"]) if v["volume"] is not None else None
            if "ch" in v: st.ch = float(v["ch"]) if v["ch"] is not None else None
            if "chp" in v: st.chp = float(v["chp"]) if v["chp"] is not None else None
            vol_delta: Optional[float] = None
            if st.vol is not None:
                if st.last_vol is None:
                    vol_delta = 0.0
                else:
                    d = st.vol - st.last_vol
                    vol_delta = d if d > 0 else 0.0
                st.last_vol = st.vol
            material = (("lp" in v and v["lp"] is not None) or ("bid" in v and v["bid"] is not None) or ("ask" in v and v["ask"] is not None) or (vol_delta is not None and vol_delta > 0.0))
            if not material:
                continue
            # fingerprint could be used for dedup; omitted
            row = (
                exch or "",
                tick,
                st.t,
                st.lp,
                st.bid,
                st.ask,
                st.vol,
                st.ch,
                st.chp,
                vol_delta,
            )
            try:
                from threading import Lock  # local import to avoid circular issues
                with self._q_lock:
                    if len(self._q) < self._q.maxlen:
                        self._q.append(row)
                        self.msg_count += 1
                        self.last_msg_ts = st.t
            except Exception:
                pass
        if (self.msg_count % 100) == 0 and self.msg_count:
            self.emit_status()

    def _run(self, stop_event: Any):  # listener loop
        delay = 1
        while not stop_event.is_set():
            try:
                try:
                    emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "WS_CONNECT", "Connecting to TradingView WS")
                except Exception:
                    pass
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=lambda ws: (self._subscribe(ws), emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "WS_OPEN", "WebSocket open")),
                    on_message=lambda _ws, raw: self._on_message(raw),
                    # Console prints removed; structured event logging only
                    on_error=lambda _ws, e: emit_event("feeder", f"tradingview:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket error callback: {e}"),
                    on_close=lambda *_: emit_event("feeder", f"tradingview:{self.name}", "listener", "WARN", "WS_CLOSED", "WebSocket closed"),
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                try:
                    emit_event("feeder", f"tradingview:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket run error: {e}", {"backoff_s": delay})
                except Exception:
                    pass
            finally:
                self.ws = None
                if not stop_event.is_set():
                    time.sleep(min(delay, 15))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

    def _writer_loop(self, stop_event):  # writer loop
        last_flush = time.time()
        batch: list[tuple] = []
        while not stop_event.is_set():
            now = time.time()
            try:
                from threading import Lock  # silence linters
                with self._q_lock:
                    while self._q and len(batch) < self._batch_size:
                        batch.append(self._q.popleft())
            except Exception:
                pass
            if batch and (len(batch) >= self._batch_size or (now - last_flush) >= self._flush_interval_s):
                try:
                    for r in batch:
                        self._writer.write_row(*r)
                except Exception as e:
                    try:
                        emit_event("feeder", f"tradingview:{self.name}", "writer", "ERROR", "BATCH_ERR", f"Batch write error: {e}")
                    except Exception:
                        pass
                batch.clear()
                last_flush = now
            if (now - self._last_flush_ts) >= 5:
                self.emit_status(force=True)
                self._last_flush_ts = now
            time.sleep(0.02)
        # flush at stop
        try:
            with self._q_lock:
                while self._q:
                    batch.append(self._q.popleft())
        except Exception:
            pass
        for r in batch:
            try:
                self._writer.write_row(*r)
            except Exception:
                pass

    def emit_status(self, force: bool = False):  # override to add queue metrics via heartbeats
        super().emit_status(force=force)
        if not self._metrics_enabled:
            return
        now = time.time()
        q_len = len(self._q)
        avg_ms = round(self._avg_handler_ms, 3)
        prev_q, prev_avg = self._last_metrics_snapshot
        q_delta = abs(q_len - prev_q)
        avg_delta = abs(avg_ms - prev_avg)
        interval_ok = (now - self._last_metrics_emit) >= self._metrics_interval
        significant_change = q_delta >= self._metrics_min_q_delta or avg_delta >= 0.5
        if interval_ok or significant_change:
            metrics = {
                'q_len': q_len,
                'batch_size': self._batch_size,
                'avg_handler_ms': avg_ms,
                'q_delta': q_delta,
                'interval_s': round(now - self._last_metrics_emit, 1) if self._last_metrics_emit else None,
            }
            try:
                if self.writer_worker is not None:
                    hb = getattr(self.writer_worker, '_hb', None)
                    if hb is not None:
                        hb.beat('running', meta=metrics)
            except Exception:
                pass
            try:
                if self.listener_worker is not None:
                    hb_l = getattr(self.listener_worker, '_hb', None)
                    if hb_l is not None:
                        hb_l.beat('running', meta={'msg_count': int(self.msg_count)})
            except Exception:
                pass
            self._last_metrics_emit = now
            self._last_metrics_snapshot = (q_len, avg_ms)

