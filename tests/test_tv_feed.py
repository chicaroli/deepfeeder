#!/usr/bin/env python3
# tv_qsd_stateful_merger.py
from __future__ import annotations
import argparse, json, time
from datetime import datetime, timezone
import websocket  # pip install websocket-client

WS_URL = "wss://data.tradingview.com/socket.io/websocket"

def tv_frame(msg: dict) -> str:
    s = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(s)}~m~{s}"

def iter_frames(payload: str):
    while payload.startswith("~m~"):
        try:
            _, rest = payload.split("~m~", 1)
            ln_str, rest = rest.split("~m~", 1)
            ln = int(ln_str)
            yield rest[:ln]
            payload = rest[ln:]
        except Exception:
            return

def iso_from_epoch_seconds(x) -> str | None:
    if x is None:
        return None
    try:
        return datetime.fromtimestamp(int(x), tz=timezone.utc).isoformat()
    except Exception:
        return None

class QuoteState:
    __slots__ = ("symbol","lp","bid","ask","volume","ch","chp","lp_time_iso","_last_emitted","_last_volume")
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.lp = None
        self.bid = None
        self.ask = None
        self.volume = None
        self.ch = None
        self.chp = None
        self.lp_time_iso = None
        self._last_emitted = None
        self._last_volume = None

    def apply(self, v: dict) -> dict | None:
        """Merge incremental fields; return an event dict if something changed."""
        changed = False

        if "lp_time" in v and v["lp_time"] is not None:
            t_iso = iso_from_epoch_seconds(v["lp_time"])
            if t_iso and t_iso != self.lp_time_iso:
                self.lp_time_iso = t_iso
                changed = True

        for k in ("lp","bid","ask","volume","ch","chp"):
            if k in v:
                new = v[k]
                # cast numerics if present
                if new is not None:
                    try:
                        new = float(new)
                    except Exception:
                        pass
                if getattr(self, k) != new:
                    setattr(self, k, new)
                    changed = True

        if not changed:
            return None

        # compute volume delta (guards: first seen or resets)
        vol_delta = None
        if self.volume is not None:
            if self._last_volume is None:
                vol_delta = 0.0
            else:
                d = self.volume - self._last_volume
                vol_delta = d if d > 0 else 0.0
            self._last_volume = self.volume

        evt = {
            "provider": "tradingview",
            "symbol": self.symbol,
            "lp_time": self.lp_time_iso,       # last-known time
            "lp": self.lp,
            "bid": self.bid,
            "ask": self.ask,
            "volume": self.volume,
            "ch": self.ch,
            "chp": self.chp,
            "volume_delta": vol_delta,
        }

        # optional dedup guard (don’t spam identical JSON)
        if evt != self._last_emitted:
            self._last_emitted = evt
            return evt
        return None

def run(symbols: list[str]):
    sid = f"qs_{int(time.time()*1000)}"
    state = {s: QuoteState(s) for s in symbols}

    def on_open(ws):
        print("[open] connected")
        ws.send(tv_frame({"m":"set_auth_token","p":["unauthorized_user_token"]}))
        ws.send(tv_frame({"m":"quote_create_session","p":[sid]}))
        ws.send(tv_frame({"m":"quote_set_fields","p":[sid,
            "lp","bid","ask","volume","ch","chp","lp_time"  # ask for all we can
        ]}))
        for s in symbols:
            print("[sub]", s)
            ws.send(tv_frame({"m":"quote_add_symbols","p":[sid, s]}))
        ws.send(tv_frame({"m":"quote_fast_symbols","p":[sid, ",".join(symbols)]}))

    def on_message(_ws, raw):
        for fr in iter_frames(raw):
            if fr.startswith("~h~"):
                continue
            try:
                obj = json.loads(fr)
            except Exception:
                continue
            if obj.get("m") != "qsd":
                # other housekeeping messages ignored
                continue
            ps = obj.get("p", [])
            if len(ps) < 2:
                continue
            updates = ps[1]
            if isinstance(updates, dict):
                updates = [updates]
            for u in updates:
                sym = u.get("n")
                v = u.get("v") or {}
                if not sym or sym not in state:
                    continue
                evt = state[sym].apply(v)
                if evt:
                    # emit one complete, merged snapshot per change
                    print(json.dumps(evt, separators=(",", ":")))

    def on_error(_ws, e): print("[err]", e)
    def on_close(*_): print("[close] disconnected")

    ws = websocket.WebSocketApp(
        WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close
    )

    while True:
        try:
            ws.run_forever(ping_interval=15, ping_timeout=10)
            time.sleep(2.0)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print("[fatal]", e); time.sleep(3.0)

if __name__ == "__main__":
    run(["BMFBOVESPA:WIN1!"])
