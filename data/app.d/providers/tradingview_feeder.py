# app.d/providers/tradingview_feeder.py
from __future__ import annotations
import json, time, re
from typing import Dict, List, Any
from threading import Thread, Event
from datetime import datetime, timezone

import websocket
from deephaven.time import to_j_instant

from core.base import BaseFeeder
from providers.tradingview_schema import tv_quotes_writer

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

def _to_instant_from_epoch_s(x) -> Any:
    if x is None:
        return None
    try:
        return to_j_instant(datetime.fromtimestamp(int(x), tz=timezone.utc))
    except Exception:
        return None

class _SymState:
    __slots__ = ("t","lp","bid","ask","vol","ch","chp","last_vol")
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
    """
    Delayed L1 quotes via TradingView qsd. Maintains per-symbol state and writes
    snapshots into a provider-specific quotes table. NOT real trade prints.
    """
    def __init__(self, name: str, symbols: List[str]):
        super().__init__("tradingview", name, symbols)
        self.stop_event = Event()
        self.thread = Thread(target=self._run, daemon=True, name=f"tv:{name}")
        self.ws = None
        self._writer = tv_quotes_writer()
        self._state: Dict[str, _SymState] = {s.lower(): _SymState() for s in self.symbols}
        self._sid = f"qs_{int(time.time()*1000)}"

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
        ws.send(_frame({"m":"set_auth_token","p":["unauthorized_user_token"]}))
        ws.send(_frame({"m":"quote_create_session","p":[self._sid]}))
        ws.send(_frame({"m":"quote_set_fields","p":[self._sid,
            "lp","bid","ask","volume","ch","chp","lp_time"
        ]}))
        for s in self.symbols:
            ws.send(_frame({"m":"quote_add_symbols","p":[self._sid, s]}))
        ws.send(_frame({"m":"quote_fast_symbols","p":[self._sid, ",".join(self.symbols)]}))

    def _handle_qsd(self, obj: dict):
        ps = obj.get("p", [])
        if len(ps) < 2:
            return
        updates = ps[1]
        if isinstance(updates, dict):
            updates = [updates]
        for it in updates:
            sym = str(it.get("n","")).lower()
            v = it.get("v") or {}
            if sym not in self._state:
                continue
            st = self._state[sym]

            # timestamp (seconds); keep last seen if absent
            if "lp_time" in v and v["lp_time"] is not None:
                st.t = _to_instant_from_epoch_s(v["lp_time"])

            # numeric fields (partial updates are common)
            if "lp" in v:  st.lp  = float(v["lp"])   if v["lp"]  is not None else None
            if "bid" in v: st.bid = float(v["bid"])  if v["bid"] is not None else None
            if "ask" in v: st.ask = float(v["ask"])  if v["ask"] is not None else None
            if "volume" in v:
                vol = float(v["volume"]) if v["volume"] is not None else None
                st.vol = vol
            if "ch" in v:   st.ch  = float(v["ch"])   if v["ch"]  is not None else None
            if "chp" in v:  st.chp = float(v["chp"])  if v["chp"] is not None else None

            # compute VolDelta with reset guard
            vol_delta = None
            if st.vol is not None:
                if st.last_vol is None:
                    vol_delta = 0.0
                else:
                    d = st.vol - st.last_vol
                    vol_delta = d if d > 0 else 0.0
                st.last_vol = st.vol

            # write one merged snapshot (9 columns, exact order)
            self._writer.write_row(
                sym,                 # Symbol
                st.t,                # LpTime (Instant or None)
                st.lp,               # LastPrice
                st.bid,              # Bid
                st.ask,              # Ask
                st.vol,              # Volume
                st.ch,               # Change
                st.chp,              # ChangePct
                vol_delta            # VolDelta
            )

            # metrics / status
            self.msg_count += 1
            self.last_msg_ts = st.t
        self.emit_status()  # throttled

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
