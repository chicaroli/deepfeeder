# ingest.feeders.providers.tradingview.feeder
from __future__ import annotations

import json, time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta

import websocket
from deephaven.time import to_j_instant
import traceback

import deepfeeder as dfb
from runtime.dh_thread import spawn
from runtime.eventlog import emit_event
from feeders.base import BaseFeeder
from feeders.common.queue_batch import QueueBatchMixin
from .schema import tv_quotes_writer, tv_bars_writer
from .config import load_config
from .backfill import TradingViewGapFiller
from .transform import split_exchange_ticker, to_instant_from_epoch_s, df_row_to_dh_row
from .journal import TradingViewJournal
from pathlib import Path
import pyarrow.dataset as ds
import pandas as pd
from persistence.paths import TV_HOT_BARS_DIR, META_DIR


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
        self.symbols = sorted({s.upper() for s in symbols})
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
            self._sym_meta[subscribe] = (exch, tick, subscribe)
        self._state: Dict[str, _SymState] = {k: _SymState() for k in self._sym_meta.keys()}
        self._last_fingerprint: Dict[str, tuple] = {}
        self._sid = f"qs_{int(time.time() * 1000)}"
        # Single GapFiller for all symbols
        self.gap_filler = None
        # In-memory marker to avoid repeated hot-loads during this process lifetime
        self._hot_replayed = False

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
        except Exception as e:
            # If journaling can't be started, continue without failing the feeder
            tb = traceback.format_exc()
            emit_event("feeder", f"tradingview:{self.name}", "startup", "ERROR", "JOURNAL_START_ERR", f"journal start failed: {e}", {"exc": tb})
            self._journal = None
        # Optional warm replay before opening WS (config-gated)
        try:
            if self._cfg.warm_replay_on_start:
                secs = int(self._cfg.warm_replay_window_secs)
                now = datetime.now(timezone.utc)
                t0 = (now - timedelta(seconds=secs)).isoformat().replace("+00:00", "Z")
                t1 = now.isoformat().replace("+00:00", "Z")
                for raw in self.symbols:
                    # Parse exchange and symbol from TV symbol string 'EXCHANGE:SYMBOL'
                    parts = raw.split(":", 1)
                    if len(parts) == 2:
                        exchange, tick = parts
                    else:
                        exchange, tick = None, raw
                    msg = dfb.replay("tv", tick, t0, t1, exchange=exchange)
        except Exception as e:
            tb = traceback.format_exc()
            emit_event("feeder", f"tradingview:{self.name}", "startup", "ERROR", "WARM_REPLAY_ERR", f"warm replay failed: {e}", {"exc": tb})
            pass

        # Attempt one-time hot-parquet load into the bars table if applicable.
        try:
            if not getattr(self, '_hot_replayed', False):
                self._load_hot_bars()
        except Exception as e:
            # Non-fatal: log and continue startup
            emit_event("feeder", f"tradingview:{self.name}", "startup", "WARN", "HOTLOAD_ERR", f"hot load failed: {e}")
          
        # Start listener worker
        self.start_writer(self.provider, self.name)
        self.listener_worker = spawn("feeder", f"{self.provider}:{self.name}", "listener", self._run)
        emit_event("feeder", f"tradingview:{self.name}", "listener", "INFO", "START", "Feeder starting", {"symbols": self.symbols})
        self.emit_status(force=True)

        # Use self._sym_meta to extract exchange and symbol lists for gap filler
        exchanges = [meta[0] for meta in self._sym_meta.values()]
        symbols_only = [meta[1] for meta in self._sym_meta.values()]
        self.gap_filler = TradingViewGapFiller(symbols=symbols_only, exchange=exchanges, journal=self._journal, feeder_name=self.name)
        self.gap_filler.start(dh_table=self._bars_writer.table, scan_interval=self._cfg.gapfill_scan_interval)

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
            sym_key = sym_raw
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
                (exch or '').upper(),
                (tick or '').upper(),
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

    def _load_hot_bars(self) -> None:
        """Load hot parquet files into the bars table once on startup.

        Emits a HOT_LOAD event at start and end for traceability.
        """
        emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", "Starting hot file load", {})
        # Marker path to indicate replay done
        marker = Path(META_DIR) / f"tv_hot_replayed_{self.name}.flag"
        # If marker exists, skip
        if marker.exists():
            self._hot_replayed = True
            emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", "Hot file load skipped (marker exists)", {})
            return

        writer = tv_bars_writer()
        # If the target table already has rows, skip hot-load to avoid duplicates
        try:
            t = writer.table
            table_not_empty = False
            if hasattr(t, 'size'):
                table_not_empty = t.size > 0
            else:
                # Fallback: try to get one row as pandas
                try:
                    table_not_empty = len(t.head(1).to_pandas()) > 0
                except Exception:
                    table_not_empty = False
            if table_not_empty:
                emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", "Hot file load skipped (table not empty)", {})
                return
        except Exception:
            # If we can't introspect the table, proceed conservatively
            pass

        base = Path(TV_HOT_BARS_DIR)
        if not base.exists():
            emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", "Hot file load skipped (no hot storage dir)", {})
            return

        # Discover parquet files under the hot dir
        try:
            ds_obj = ds.dataset(str(base), format='parquet', partitioning='hive')
            table = ds_obj.to_table()
            df = table.to_pandas()
        except (OSError, FileNotFoundError, Exception) as e:
            # reading parquet failed; record stacktrace and skip hot-load
            tb = traceback.format_exc()
            emit_event("feeder", f"tradingview:{self.name}", "startup", "ERROR", "HOTLOAD_READ_ERR", f"reading hot parquet failed: {e}", {"exc": tb})
            return

        if df.empty:
            # nothing to load
            try:
                marker.write_text('no-data')
            except Exception as e:
                tb = traceback.format_exc()
                emit_event("feeder", f"tradingview:{self.name}", "startup", "ERROR", "HOTLOAD_MARKER_ERR", f"writing marker failed: {e}", {"exc": tb})
            self._hot_replayed = True
            emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", "Hot file load completed (no data)", {})
            return

        # Use centralized transform helper to normalize parquet rows for Deephaven
        # Ensure the pandas DF has a `datetime` column expected by df_row_to_dh_row
        try:
            if 'timestamp' in df.columns and 'datetime' not in df.columns:
                df['datetime'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            # Normalize exchange/symbol presence
            if 'exchange' in df.columns:
                df['exchange'] = df['exchange'].fillna('')
            if 'symbol' in df.columns:
                df['symbol'] = df['symbol'].fillna('')
        except Exception:
            pass

        # Deduplicate by (exchange, symbol, datetime) keeping last
        dedup_keys = [k for k in ('exchange', 'symbol', 'datetime') if k in df.columns]
        if dedup_keys:
            df = df.sort_values(by=dedup_keys).drop_duplicates(subset=dedup_keys, keep='last')

        # Write rows using the transform helper to ensure consistent mapping
        written = 0
        failed = 0
        last_tb = None
        for _, row in df.iterrows():
            try:
                exch = (row.get('exchange') or '').upper()
                sym = (row.get('symbol') or '').upper()
                dh_row = df_row_to_dh_row(exch, sym, row)
                writer.write_row(
                    dh_row['exchange'],
                    dh_row['symbol'],
                    dh_row['datetime'],
                    dh_row['open'],
                    dh_row['high'],
                    dh_row['low'],
                    dh_row['close'],
                    dh_row['volume'],
                )
                written += 1
            except Exception as e:
                failed += 1
                last_tb = traceback.format_exc()
                # continue writing remaining rows
                continue

        # Mark replay done
        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(f'loaded:{written}')
        except Exception as e:
            tb = traceback.format_exc()
            emit_event("feeder", f"tradingview:{self.name}", "startup", "ERROR", "HOTLOAD_MARKER_ERR", f"writing marker failed: {e}", {"exc": tb})
        # mark in-memory flag
        self._hot_replayed = True
        emit_event("feeder", f"tradingview:{self.name}", "startup", "INFO", "HOT_LOAD", f"Hot file load completed: {written} rows written, {failed} failed", {"written": int(written), "failed": int(failed)})

