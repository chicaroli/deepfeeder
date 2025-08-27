"""
TradingViewGapFiller: Fills missing OHLCV bars in _TV_BARS_DTW using TradingView API.
Mirrors the BinanceGapFiller pattern for consistency.
"""
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from tvDatafeed import Interval

from feeders.tradingview.transform import df_row_to_dh_row
from feeders.tradingview.api import fetch_tv_data
from feeders.tradingview.schema import tv_bars_writer
from runtime.backfill import GapFiller
from runtime.eventlog import emit_event


class TradingViewGapFiller(GapFiller):
    """
    TradingView gap filler: inherits generic gap detection/backfill logic from GapFiller.
    Implements provider-specific gap detection and backfill using TradingView API.
    Uses a watermark (latest timestamp) for incremental backfill, mimicking Binance structure.
    """
    def __init__(self, symbol: str, exchange: str, interval: Interval = Interval.in_1_minute, journal: Optional[Any] = None):
        """Create a TradingViewGapFiller.

        journal: optional journaling instance (e.g., TradingViewJournal). If
        provided, written rows will be forwarded to it using
        journal.journal_bar_nonblocking(...).
        """
        super().__init__(provider="tradingview", symbol=symbol, key_column="Timestamp", exchange=exchange)
        self.interval = interval
        self.writer = tv_bars_writer()
        self.watermark = None  # Latest timestamp written
        # Optional journaling instance (e.g., TradingViewJournal)
        self._journal: Optional[Any] = journal

    def provider_gap_detection(self, dh_table, start: datetime = None, end: datetime = None):
        """
        Provider-specific gap detection for TradingView bars table.
        Returns a list of (start_ts, end_ts) tuples for missing intervals.
        On first run, returns full range. On subsequent runs, returns (watermark, now + overlap).
        """
        now = datetime.now(timezone.utc)
        overlap_minutes = 5  # Re-fetch last N minutes to refresh open bars
        # Ensure start and end are UTC-aware before arithmetic
        def ensure_utc(dt):
            if dt is None:
                return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        if self.watermark is None:
            # First run: fetch all available history (limit to 30 days for safety)
            start = now - timedelta(days=30)
            end = now
        else:
            # Subsequent runs: fetch from watermark, plus overlap
            start = self.watermark - timedelta(minutes=overlap_minutes)
            end = now
        start = ensure_utc(start)
        end = ensure_utc(end)
        return [(start, end)]

    def _coalesce_gaps(self, gaps, max_ids, merge_distance):
        """
        TradingView does not need gap coalescing; just return the input gaps unchanged.
        """
        return gaps

    def provider_backfill_gaps(self, gaps):
        """
        Provider-specific backfill logic for TradingView.
        Fills the single gap by fetching bars from TradingView API and writing to bars table.
        Updates watermark after each fetch. Emits events for start, ingest, and errors.
        """
        
        if not gaps:
            return
        start, end = gaps[0]
        emit_event("feeder", f"tradingview:{self.symbol}", "backfill", "INFO", "BACKFILL_INIT",
                   f"Initializing TradingView backfill for {self.symbol} {start}..{end}", {"gap": (start, end)})
        # Calculate number of bars (minutes) between start and end, cap at 10,000
        n_bars = int((end - start).total_seconds() // 60)
        n_bars = min(n_bars, 10_000)
        try:
            df = fetch_tv_data(
                symbol=self.symbol,
                exchange=self.exchange,
                interval=self.interval,
                n_bars=n_bars
            )
            if df.empty:
                emit_event(
                    "feeder", f"tradingview:{self.symbol}", "backfill", "ERROR", "API_EMPTY",
                    f"No data returned for {self.symbol} {start}..{end}", {"gap": (start, end)}
                )
                return
            # Convert start/end to pandas Timestamp for comparison
            start_ts = pd.Timestamp(start).tz_localize(None)
            end_ts = pd.Timestamp(end).tz_localize(None)
            df = df[(df['datetime'] >= start_ts) & (df['datetime'] <= end_ts)]
            written = 0
            latest_ts = self.watermark
            for _, row in df.iterrows():
                dh_row = df_row_to_dh_row(self.exchange, self.symbol, row)
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
                # Forward to the provided journal instance if any (non-blocking)
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
                        # Keep backfill robust; journaling failures must not break ingest
                        pass
                written += 1
                # Update watermark if this row is newer
                dt = row['datetime']
                if latest_ts is None or dt > latest_ts:
                    latest_ts = dt
            self.watermark = latest_ts
            emit_event(
                "feeder", f"tradingview:{self.symbol}", "backfill", "INFO", "INGEST",
                f"Ingested backfill rows for {self.symbol}: written={written}",
                {"rows": len(df), "written": written}
            )
        except Exception as ex:
            emit_event("feeder", f"tradingview:{self.symbol}", "backfill", "ERROR", "INGEST_ERR",
                       f"Ingest error for {self.symbol}: {ex}", {"error": str(ex)})
