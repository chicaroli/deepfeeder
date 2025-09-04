# providers/tradingview/producer_api.py
from __future__ import annotations

import threading
from threading import Event
from typing import Optional, Dict, Any, List, Tuple

import pandas as pd
from tvDatafeed import Interval

from core.contracts import Producer, EventBus, Tick
from runtime.eventlog import emit_event
from runtime.dh_thread import spawn
from .api import fetch_tv_data


class TradingViewApiProducer(Producer):
    """
    TradingView bars producer (API batches):
      - Runs on schedule (internal loop)
      - Fetches bars from TradingView API
      - Repaints last N bars (overlap), dedupes old ones downstream
      - Publishes Tick(stream="bars") to EventBus
    """
    provider = "tradingview"
    stream = "bars"

    def __init__(
        self,
        name: str,
        symbols: List[str],
        bus: EventBus,
        *,
        exchange: Optional[str | List[str]] = None,
        tv_bar_interval: Interval = Interval.in_1_minute,
        flush_interval_s: float = 60.0,
        batch_size: int = 5000,
        overlap_bars: int = 5,
        days_back_full: int = 10,
        event_store: Optional[Any] = None,          # optional WAL append-only store
        state: Optional[Dict[str, Any]] = None,     # optional persisted watermarks: {"watermarks": {sym: iso}}
    ):
        self.name = name
        self.bus = bus
        self.event_store = event_store

        self.tv_bar_interval = tv_bar_interval
        self.flush_interval_s = int(flush_interval_s)
        self.batch_size = int(batch_size)
        self.overlap_bars = int(overlap_bars)
        self.days_back_full = int(days_back_full)

        # Normalize symbols/exchanges: support "EXCH:SYM" or (exchange, sym)
        parsed = [s.split(":", 1) if ":" in s else (exchange, s) for s in symbols]
        self._symbols = [p[1] for p in parsed]
        if exchange is None:
            self._exchanges = [p[0] for p in parsed]
        elif isinstance(exchange, str):
            self._exchanges = [exchange] * len(self._symbols)
        elif isinstance(exchange, list) and len(exchange) == len(self._symbols):
            self._exchanges = exchange
        else:
            raise ValueError("exchange must be None/str/list[str] with same length as symbols")

        # Per-symbol watermark (tz-naive UTC)
        self._wm: Dict[str, Optional[pd.Timestamp]] = dict.fromkeys(self._symbols, None)
        if state and isinstance(state.get("watermarks"), dict):
            for k, v in state["watermarks"].items():
                if k in self._wm and v:
                    self._wm[k] = pd.to_datetime(v).tz_localize(None)
        self._state_ref = state

        self._worker = None  # worker thread returned by spawn

    # -------- Producer protocol --------
    def start(self) -> None:
        if self.is_alive():
            emit_event("feeder", self.name, "listener", "INFO", "ALREADY_RUNNING", "Producer already running")
            return
        self._worker = spawn("feeder", self.name, "listener", self._run)

    def stop(self) -> None:
        """Gracefully stop the TradingView API producer and its worker thread."""
        try:
            if self._worker is not None:
                self._worker.stop()
                self._worker.join(timeout=3)
        except Exception:
            pass
        emit_event("feeder", self.name, "listener", "INFO", "STOP", "Producer stopped")

    def join(self, timeout: Optional[float] = None) -> None:
        if self._worker:
            self._worker.join(timeout)

    def is_alive(self) -> bool:
        return bool(self._worker and self._worker.is_alive())

    # -------- internal loop --------
    def _run(self, stop_event: Event) -> None:
        emit_event("feeder", self.name, "api", "INFO", "START", f"{self.name} started", {})
        try:
            while not stop_event.is_set():
                self._run_once_batch()
                stop_event.wait(self.flush_interval_s)
        finally:
            emit_event("feeder", self.name, "api", "INFO", "STOP", f"{self.name} stopped", {})

    # -------- one scheduled run --------
    def _run_once_batch(self) -> None:
        total_emitted = 0

        for i, sym in enumerate(self._symbols):
            exch = self._exchanges[i]
            start_ts, end_ts, n_req = self._calc_window(sym)

            try:
                emit_event("feeder", self.name, "api", "INFO", "API_REQUEST",
                           f"Requesting data for {sym} {exch} {n_req} bars",
                           {"Exchange": exch, "symbol": sym, "nbars": n_req}
                           )
                df = fetch_tv_data(symbol=sym, exchange=exch, interval=self.tv_bar_interval, n_bars=n_req)
                if df is None or df.empty:
                    continue

                print(f"DEBUG: fetched {len(df)} bars for {exch}:{sym} from {start_ts} to {end_ts}")

                _start_ts = pd.Timestamp(start_ts).tz_localize(None)
                _end_ts = pd.Timestamp(end_ts).tz_localize(None)
                df = df[(df["datetime"] >= _start_ts) & (df["datetime"] <= _end_ts)]
                if df.empty:
                    emit_event("feeder", self.name, "api", "WARN", "API_EMPTY",
                        f"No data returned for {sym} {exch} {n_req} bars",
                        {"Exchange": exch, "symbol": sym, "nbars": n_req}
                    )
                    continue

                # repaint last N bars each cycle, but emit all bars on first run
                if self._wm.get(sym) is None:
                    # First run: emit all bars in window
                    recent_cutoff = df["datetime"].min()
                elif len(df) > self.overlap_bars:
                    # Subsequent runs: emit only last N bars
                    recent_cutoff = df["datetime"].sort_values().iloc[-self.overlap_bars]
                else:
                    recent_cutoff = df["datetime"].min()

                batch_ticks, latest = self._to_ticks(exch, sym, df, recent_cutoff)
                if not batch_ticks:
                    continue

                # 1) publish to in-memory bus
                self.bus.publish(batch_ticks)

                # 2) optionally append to durable store (WAL)
                if self.event_store is not None:
                    try:
                        self.event_store.append(batch_ticks)
                    except Exception:
                        # WAL hiccups shouldn't kill the cycle
                        pass

                # advance watermark
                if latest is not None:
                    self._wm[sym] = latest

                total_emitted += len(batch_ticks)

            except Exception as ex:
                emit_event("feeder", f"{self.name}:{sym}", "api", "ERROR", "BATCH_ERR",
                           f"{exch}:{sym} error: {ex}", {"error": str(ex)})

        # persist watermarks if provided
        if self._state_ref is not None:
            self._state_ref["watermarks"] = {
                k: (v.isoformat() if v is not None else None) for k, v in self._wm.items()
            }

        emit_event("feeder", self.name, "api", "INFO", "BATCH_DONE",
                   f"TV API batch done, emitted={total_emitted}", {"count": total_emitted})

    # -------- helpers --------
    def _calc_window(self, symbol: str) -> Tuple[pd.Timestamp, pd.Timestamp, int]:
        now = pd.Timestamp.utcnow().tz_localize(None)
        wm = self._wm.get(symbol)
        start = (now - pd.Timedelta(days=self.days_back_full)) if wm is None else (wm - pd.Timedelta(minutes=self.overlap_bars))
        end = now
        n_bars = int(max(1, (end - start).total_seconds() // 60))
        return start, end, min(n_bars, self.batch_size)

    def _to_ticks(
            self,
            exchange: str,
            symbol: str,
            df: pd.DataFrame,
            recent_cutoff: pd.Timestamp,
    ) -> Tuple[List[Tick], Optional[pd.Timestamp]]:
        ticks: List[Tick] = []
        latest: Optional[pd.Timestamp] = self._wm.get(symbol)

        full_symbol = f"{exchange}:{symbol}" if exchange else symbol

        for _, row in df.iterrows():
            dt = pd.to_datetime(row["datetime"]).tz_localize(None)
            if dt < recent_cutoff:
                continue

            ts_ns = int(pd.Timestamp(dt).value)
            payload = {
                "exchange": exchange,
                "symbol": symbol,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
            }
            ticks.append(
                Tick(
                    provider=self.provider,
                    stream=self.stream,
                    symbol=full_symbol,     # EXCHANGE:SYMBOL
                    ts_ns=ts_ns,
                    seq=None,               # TradingView API has no seq
                    payload=payload,
                    is_final=True,
                )
            )

            if latest is None or dt > latest:
                latest = dt

        return ticks, latest
