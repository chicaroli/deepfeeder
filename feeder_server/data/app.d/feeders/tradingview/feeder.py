# ingest.feeders.providers.tradingview.feeder
from __future__ import annotations
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from threading import Thread, Event
from datetime import datetime, timezone

import websocket
from deephaven.time import to_j_instant

from feeders.base import BaseFeeder
from .schema import tv_quotes_writer

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
        self.stop_event = Event()
        self.thread = Thread(target=self._run, daemon=True, name=f"tv:{name}")
        self.ws = None
        self._writer = tv_quotes_writer()
        self._sym_meta: Dict[str, Tuple[Optional[str], str, str]] = {}
        for raw in self.symbols:
            exch, tick = _split_exchange_ticker(raw)
            subscribe = f"{exch}:{tick}" if exch else tick
            self._sym_meta[subscribe.lower()] = (exch, tick, subscribe)
        self._state: Dict[str, _SymState] = {k: _SymState() for k in self._sym_meta.keys()}
        self._last_fingerprint: Dict[str, tuple] = {}
        self._sid = f"qs_{int(time.time() * 1000)}"

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return "already running"
        self.stop_event.clear()
        self.started_at = time.time()
        self.last_error = None
        self.thread = Thread(target=self._run, daemon=True, name=f"tv:{self.name}")
        self.thread.start()
        self.emit_status(force=True)
        return "started"

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive():
            self.thread.join(timeout=3)
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
            self._writer.write_row(
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
            self.msg_count += 1
            self.last_msg_ts = st.t
        self.emit_status()

    def _run(self):
        delay = 1
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=lambda ws: self._subscribe(ws),
                    on_message=lambda _ws, raw: self._on_message(raw),
                    on_error=lambda _ws, e: print(f"[tradingview:{self.name}] ws error: {e}"),
                    on_close=lambda *_: print(f"[tradingview:{self.name}] ws closed"),
                )
                self.ws.run_forever(ping_interval=15, ping_timeout=10)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                print(f"[tradingview:{self.name}] ws error: {e}")
            finally:
                self.ws = None
                if not self.stop_event.is_set():
                    time.sleep(min(delay, 15))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

