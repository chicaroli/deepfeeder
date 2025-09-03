"""
TradingViewGapFiller: Fills missing OHLCV bars in _TV_BARS_DTW using TradingView API.
Mirrors the BinanceGapFiller pattern for consistency.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
from tvDatafeed import Interval
from deephaven import arrow as dh_arrow

from runtime.backfill import GapFiller
from runtime.eventlog import emit_event
from .transform import df_row_to_dh_row
from .api import fetch_tv_data
from providers.tradingview.schema import tv_bars_writer



class TradingViewGapFiller(GapFiller):
    """
    TradingView gap filler: now supports multiple symbols per instance.
    Implements provider-specific gap detection and backfill using TradingView API.
    Uses a watermark (latest timestamp) for incremental backfill, mimicking Binance structure.
    """
    def __init__(self, symbols: list[str], journal: Optional[Any] = None, interval: Interval = Interval.in_1_minute, exchange: None | str | list[str] = None, feeder_name: str = None):
        """Create a TradingViewGapFiller for multiple symbols.

        symbols: list of symbol strings (e.g., ['EXCHANGE:BTCUSD', 'EXCHANGE:ETHUSD'])
        exchange: None, str, or list[str] for per-symbol assignment
        journal: optional journaling instance (e.g., TradingViewJournal). If
        provided, written rows will be forwarded to it using
        journal.journal_bar_nonblocking(...).
        feeder_name: name of the feeder for logging/alerts.
        """
        # Split symbols into exchange and symbol parts
        parsed = [s.split(":", 1) if ":" in s else (exchange, s) for s in symbols]
        exchanges_from_symbols = [p[0] for p in parsed]
        symbols_only = [p[1] for p in parsed]
        # Handle exchange as None, str, or list[str]
        if exchange is None:
            exchanges = exchanges_from_symbols
        elif isinstance(exchange, str):
            exchanges = [exchange] * len(symbols_only)
        elif isinstance(exchange, list):
            if len(exchange) != len(symbols_only):
                raise ValueError("Length of exchange list must match symbols list")
            exchanges = exchange
        else:
            raise TypeError("exchange must be None, str, or list[str]")
        if len(symbols_only) != len(exchanges):
            raise ValueError("Length of symbols and exchanges must match for TradingViewGapFiller.")
        self.exchanges = exchanges
        self.symbols = symbols_only
        # For backward compatibility, set self.exchange to first exchange
        self.exchange = self.exchanges[0] if self.exchanges else None
        super().__init__(provider="tradingview", symbols=symbols_only, key_column="Timestamp", exchange=self.exchanges, feeder_name=feeder_name)
        self.interval = interval
        self.writer = tv_bars_writer()
        # Watermark per symbol
        self.watermarks = dict.fromkeys(symbols_only)
        self._journal: Optional[Any] = journal

    def _get_existing_timestamps(self, exch_str: str, sym_str: str):
        """
        Retrieve existing Timestamp values for exchange+symbol from the underlying
        Deephaven table using deephaven.arrow.to_arrow() and return a (set, max)
        tuple where the set contains tz-naive datetimes and max is the latest datetime
        (or None).
        This avoids converting the full table to pandas.
        """
        existing_ts_set = set()
        existing_max = None
        try:
            tbl = self.writer.table
            existing = tbl.where(f'Exchange == "{exch_str}" && Symbol == "{sym_str}"').select('Timestamp')
            a_table = dh_arrow.to_arrow(existing)
            chunked = a_table.column('Timestamp')
            py_ts = []
            for chunk in getattr(chunked, 'chunks', [chunked]):
                for v in chunk.to_pylist():
                    if v is None:
                        continue
                    if getattr(v, 'tzinfo', None) is not None:
                        v = v.astimezone(timezone.utc).replace(tzinfo=None)
                    py_ts.append(v)
            if py_ts:
                ts_series = pd.to_datetime(py_ts, utc=True, errors='coerce')
                ts_series = ts_series.dt.tz_convert(None)
                existing_ts_set = set(ts_series.tolist())
                if len(ts_series):
                    existing_max = ts_series.max()
        except Exception:
            # best-effort: if table/arrow operations fail, proceed without dedupe
            existing_ts_set = set()
            existing_max = None
        return existing_ts_set, existing_max

    def provider_gap_detection(self, dh_table, start: datetime = None, end: datetime = None):
        """
        Provider-specific gap detection for TradingView bars table.
        Returns a dict mapping symbol to list of (start_ts, end_ts) tuples for missing intervals.
        """
        now = datetime.now(timezone.utc)
        overlap_minutes = 5  # Re-fetch last N minutes to refresh open bars
        def ensure_utc(dt):
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        gaps = {}
        for symbol in self.symbols:
            wm = self.watermarks.get(symbol)
            if wm is None:
                s = now - timedelta(days=30)
                e = now
            else:
                s = wm - timedelta(minutes=overlap_minutes)
                e = now
            s = ensure_utc(s)
            e = ensure_utc(e)
            gaps[symbol] = [(s, e)]
        return gaps

    def _coalesce_gaps(self, gaps, max_ids, merge_distance):
        """
        TradingView does not need gap coalescing; just return the input gaps unchanged.
        """
        return gaps

    def provider_backfill_gaps(self, gaps_dict):
        """
        Provider-specific backfill logic for TradingView.
        Loops through all symbols, fills gaps sequentially by fetching bars from TradingView API and writing to bars table.
        Updates watermark per symbol after each fetch. Emits events for start, ingest, and errors.
        """
        if not gaps_dict:
            return
        for idx, symbol in enumerate(self.symbols):
            gaps = gaps_dict.get(symbol)
            exchange = self.exchanges[idx]
            if not gaps:
                continue
            start, end = gaps[0]
            n_bars = int((end - start).total_seconds() // 60)
            n_bars = min(n_bars, 5000)
            emit_event(
                "feeder", f"tradingview:{symbol}", "backfill", "INFO", "BACKFILL_INIT",
                f"Initializing TradingView backfill for {symbol} {exchange} {n_bars} bars",
                {"Exchange": exchange, "symbol": symbol, "nbars": n_bars}
                )
            try:
                df = fetch_tv_data(
                    symbol=symbol,
                    exchange=exchange,
                    interval=self.interval,
                    n_bars=n_bars
                )
                if df.empty:
                    emit_event(
                        "feeder", f"tradingview:{symbol}", "backfill", "WARN", "API_EMPTY",
                        f"No data returned for {symbol} {exchange} {n_bars} bars",
                        {"Exchange": exchange, "symbol": symbol, "nbars": n_bars}
                    )
                    continue
                start_ts = pd.Timestamp(start).tz_localize(None)
                end_ts = pd.Timestamp(end).tz_localize(None)
                df = df[(df['datetime'] >= start_ts) & (df['datetime'] <= end_ts)]

                exch_str = (exchange or "").upper()
                sym_str = (symbol or "")
                existing_ts_set, existing_max = self._get_existing_timestamps(exch_str, sym_str)

                written = 0
                skipped = 0
                latest_ts = self.watermarks.get(symbol)
                overlap_minutes = 5
                recent_cutoff = None
                if existing_max is not None:
                    recent_cutoff = existing_max - pd.Timedelta(minutes=overlap_minutes)

                for _, row in df.iterrows():
                    try:
                        dt = row['datetime']
                        if hasattr(dt, 'tz_localize'):
                            dt_norm = pd.to_datetime(dt).tz_localize(None)
                        else:
                            dt_norm = pd.to_datetime(dt)

                        is_recent = (recent_cutoff is None) or (dt_norm > recent_cutoff)

                        if (not is_recent) and (dt_norm in existing_ts_set):
                            skipped += 1
                            continue

                        dh_row = df_row_to_dh_row(exchange, symbol, row)
                        self.writer.write_row(
                            dh_row['exchange'],
                            dh_row['symbol'],
                            dh_row['datetime'],
                            dh_row['open'],
                            dh_row['high'],
                            dh_row['low'],
                            dh_row['close'],
                            dh_row['volume']
                        )
                        if getattr(self, '_journal', None) is not None:
                            try:
                                self._journal.journal_bar_nonblocking(
                                    dh_row['exchange'],
                                    dh_row['symbol'],
                                    dh_row['datetime'],
                                    dh_row['open'],
                                    dh_row['high'],
                                    dh_row['low'],
                                    dh_row['close'],
                                    dh_row['volume'],
                                )
                            except Exception:
                                pass
                        written += 1
                        if latest_ts is None or dt_norm > latest_ts:
                            latest_ts = dt_norm
                    except Exception:
                        continue
                self.watermarks[symbol] = latest_ts
                emit_event(
                    "feeder", f"tradingview:{symbol}", "backfill", "INFO", "INGEST",
                    f"Ingested backfill rows for {symbol}: written={written}, skipped={skipped}",
                    {"rows": len(df), "written": written, "skipped": skipped}
                )
            except Exception as ex:
                emit_event("feeder", f"tradingview:{symbol}", "backfill", "ERROR", "INGEST_ERR",
                           f"Ingest error for {symbol}: {ex}", {"error": str(ex)})
