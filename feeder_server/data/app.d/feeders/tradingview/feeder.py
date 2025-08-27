# ingest.feeders.providers.tradingview.feeder
from __future__ import annotations

import json, time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta

import websocket
from deephaven.time import to_j_instant

from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
from feeders.base import BaseFeeder
from feeders.common.queue_batch import QueueBatchMixin
from .schema import tv_quotes_writer, tv_bars_writer
from .config import load_config
from .backfill import TradingViewGapFiller
from .transform import split_exchange_ticker, to_instant_from_epoch_s
from .journal import TradingViewJournal


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

class TradingViewFeeder(BaseFeeder, QueueBatchMixin):
    def __init__(self, name: str, symbols: List[str]):
        super().__init__("tradingview", name, symbols)
        self.listener_worker = None
        self.writer_worker = None
        self.ws = None
        self._writer = tv_quotes_writer()
        self._bars_writer = tv_bars_writer()
        # Journal instance (managed per-feeder)
        self._journal: Optional[TradingViewJournal] = None
        cfg = load_config()
        self._cfg = cfg
        QueueBatchMixin.__init__(self, queue_maxlen=5000)
        # Symbol meta/state
        self._sym_meta: Dict[str, Tuple[Optional[str], str, str]] = {}
        for raw in self.symbols:
            exch, tick = split_exchange_ticker(raw)
            subscribe = f"{exch}:{tick}" if exch else tick
            self._sym_meta[subscribe.lower()] = (exch, tick, subscribe)
        self._state: Dict[str, _SymState] = {k: _SymState() for k in self._sym_meta.keys()}
        self._last_fingerprint: Dict[str, tuple] = {}
        self._sid = f"qs_{int(time.time() * 1000)}"
        # GapFillers for each symbol
        self.gap_fillers = []

    def is_alive(self) -> bool:
        return self.listener_worker is not None and self.listener_worker.is_alive()

    def start(self):
        if self.is_alive():
            self.emit_status(force=True)
            return "already running"
        self.started_at = time.time()
        self.last_error = None
        # Start the per-feeder TradingViewJournal instance
        try:
            self._journal = TradingViewJournal("feeder", f"{self.provider}:{self.name}", "journal")
            self._journal.start()
        except Exception:
            # If journaling can't be started, continue without failing the feeder
            self._journal = None
        # Optional warm replay before opening WS (config-gated)
        try:
            if self._cfg.warm_replay_on_start:
                secs = int(self._cfg.warm_replay_window_secs)
                now = datetime.now(timezone.utc)
                t0 = (now - timedelta(seconds=secs)).isoformat().replace("+00:00", "Z")
                t1 = now.isoformat().replace("+00:00", "Z")
                import deepfeeder as dfb  # lazy import to avoid circulars
                for raw in self.symbols:
                    # Parse exchange and symbol from TV symbol string 'EXCHANGE:SYMBOL'
                    parts = raw.split(":", 1)
                    if len(parts) == 2:
                        exchange, tick = parts
                    else:
                        exchange, tick = None, raw
                    msg = dfb.replay("tv", tick, t0, t1, exchange=exchange)
        except Exception:
            pass
        
        # Start listener worker
        self.start_writer(self.provider, self.name)
        self.listener_worker = spawn("feeder", f"{self.provider}:{self.name}", "listener", self._run)
        emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        self.emit_status(force=True)
        
        # Start GapFiller for all symbols
        self.gap_fillers = []
        for raw in self.symbols:
            parts = raw.split(":", 1)
            if len(parts) == 2:
                exchange, tick = parts
            else:
                exchange, tick = None, raw
            gap_filler = TradingViewGapFiller(symbol=tick, exchange=exchange, journal=self._journal)
            gap_filler.start(dh_table=self._bars_writer.table)
            self.gap_fillers.append(gap_filler)

        return "started"

    def stop(self):
        try:
            if self.listener_worker is not None:
                stop_method = getattr(self.listener_worker, 'stop', None)
                if callable(stop_method):
                    stop_method()
        except Exception:
            pass
        self.stop_writer()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        if self.is_alive() and self.listener_worker is not None:
            self.listener_worker.join(timeout=3)
        emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "STOP", "Feeder stopping")
        self.emit_status(force=True)
        # Stop the per-feeder journal
        try:
            if self._journal is not None:
                try:
                    self._journal.stop(timeout=2.0)
                except Exception:
                    pass
                self._journal = None
        except Exception:
            pass
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
            new_t = to_instant_from_epoch_s(v.get("lp_time"))
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
            # dh_row = ws_row_to_dh_row(row)
            try:
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
                emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "WS_CONNECT", "Connecting to TradingView WS")
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=lambda ws: (self._subscribe(ws), emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "WS_OPEN", "WebSocket open")),
                    on_message=lambda _ws, raw: self._on_message(raw),
                    # Console prints removed; structured event logging only
                    on_error=lambda _ws, e: emit_event("feeder", f"tradingview:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket error callback: {e}"),
                    on_close=lambda *_: emit_event("feeder", f"tradingview:{self.name}", "listener", "WARN", "WS_CLOSED", "WebSocket closed"),
                )
                self.ws.run_forever(ping_interval=25, ping_timeout=15)
                delay = 1
            except Exception as e:
                self.last_error = str(e)
                emit_event("feeder", f"tradingview:{self.name}", "listener", "ERROR", "WS_ERR", f"WebSocket run error: {e}", {"backoff_s": delay})
            finally:
                self.ws = None
                if not stop_event.is_set():
                    time.sleep(min(delay, 15))
                    delay = min(delay * 2, 15)
                self.emit_status(force=True)

    # writer loop provided by QueueBatchMixin

    def emit_status(self, force: bool = False):  # override to add queue metrics via heartbeats
        super().emit_status(force=force)
        self._emit_queue_metrics()
        try:
            if self.listener_worker is not None:
                hb_l = getattr(self.listener_worker, '_hb', None)
                if hb_l is not None:
                    hb_l.beat('running', meta={'msg_count': int(self.msg_count)})
        except Exception:
            pass

