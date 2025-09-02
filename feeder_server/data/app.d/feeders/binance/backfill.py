"""
BinanceGapFiller: provider-specific gap filler for Binance.

This module keeps Binance REST/backfill logic isolated from generic gap detection.
It subclasses the project's GapFiller and implements provider-specific methods.

Notes:
- For historical trades Binance offers endpoints that have trade-id or time-based pagination.
  - `/api/v3/historicalTrades` requires API key.
  - `/api/v3/aggTrades` supports startTime/endTime and no API key, but returns aggregated trades.
- Implement REST pagination and convert provider responses into the same schema used by the real-time feeder.
"""
from __future__ import annotations
from typing import List, Tuple, Optional, Callable
from datetime import datetime
import requests
import time

from runtime.eventlog import emit_event
from runtime.backfill import GapFiller
from runtime.dh_thread import spawn
from .config import load_config

from deephaven.arrow import to_arrow
from feeders.binance.journal_adapter import ingest_rest_rows


class BinanceGapFiller(GapFiller):
    """Binance-specific gap filler.

    Responsibilities:
      - Implement get_key_sequence to read TradeIDs from the supplied dh_table.
      - Implement backfill_gaps to call Binance REST API, translate rows to the feeder schema and ingest.

    This class intentionally keeps the concrete fetching logic as TODOs so you can choose which
    Binance endpoint and authentication to use in your environment.
    """

    BASE_URL = "https://api.binance.com"

    def __init__(self, symbol: str, key_column: str = "TradeID", exchange: str = None, api_key: str | None = None):
        super().__init__(provider='binance', symbols=[symbol], key_column=key_column, exchange=exchange, api_key=api_key)
        self.symbol = symbol
        self.gaps_tbl = None

    def provider_gap_detection(self, dh_table):
        """
        Compute gaps for the single symbol and return as a dict for compatibility with GapFiller.
        """
        if self.gaps_tbl is None:
            self.build_gaps_table(dh_table)

        arrow_gaps = to_arrow(self.gaps_tbl)
        if arrow_gaps.num_rows == 0:
            gaps = []
        else:
            prev_list = arrow_gaps.column('PrevID').to_pylist()
            cur_list = arrow_gaps.column(self.key_column).to_pylist()
            gaps = [(int(prev_val) + 1, int(cur_val) - 1) for prev_val, cur_val in zip(prev_list, cur_list, strict=False)]
        return {self.symbol: gaps}

    def build_gaps_table(self, dh_table):
        # Build symbol/exchange filter and select key column
        query = f"Symbol == '{self.symbol}'"
        if self.exchange is not None:
            query += f" && Exchange == '{self.exchange}'"
        tbl = dh_table.where(query).select_distinct(self.key_column).sort(self.key_column)

        self.gaps_tbl = (
            tbl.update_view([f"PrevID = {self.key_column}_[i-1]"])
            .where(f"PrevID != null && {self.key_column} != PrevID + 1")
            .select(["PrevID", self.key_column])
            )

    def provider_backfill_gaps(self, gaps_dict):
        """Backfill each gap using Binance REST `/api/v3/historicalTrades` with `fromId` pagination.

        Accepts a dict mapping symbol to list of gaps for compatibility with GapFiller.
        """
        gaps = gaps_dict.get(self.symbol, [])
        if not gaps:
            return

        data = self._request_gaps(gaps)
        if not data:
            return

        try:
            written, failed = ingest_rest_rows(data, self.symbol)
            emit_event("feeder", f"binance:{self.symbol}", "backfill", "INFO", "INGEST",
                       f"Ingested backfill rows for {self.symbol}: written={written} failed={failed}",
                       {"rows": len(data), "written": written, "failed": failed})
        except Exception as ex:
            emit_event("feeder", f"binance:{self.symbol}", "backfill", "ERROR", "INGEST_ERR",
                       f"Ingest error for {self.symbol}: {ex}", {"error": str(ex)})

    def _request_gaps(self, gaps: List[Tuple[int, int]]) -> List[dict]:
        endpoint = f"{self.BASE_URL}/api/v3/historicalTrades"
        page_limit = 1000
        all_rows: List[dict] = []

        emit_event("feeder", f"binance:{self.symbol}", "backfill", "INFO", "BACKFILL_INIT",
                   f"Initializing Binance backfill for {self.symbol} with {len(gaps)} gaps: ", {"gaps": gaps[:10]})

        for gap_start, gap_end in gaps:
            headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
            next_id = int(gap_start)
            requests_made = 0

            while next_id <= int(gap_end):
                limit = min(page_limit, int(gap_end) - next_id + 1)
                params = {"symbol": self.symbol.upper(), "fromId": next_id, "limit": limit}

                attempt = 0
                max_attempts = 2
                while attempt < max_attempts:
                    attempt += 1
                    try:
                        r = requests.get(endpoint, params=params, headers=headers, timeout=30)
                    except Exception as ex:
                        backoff = min(10, 2 ** attempt)
                        emit_event("feeder", f"binance:{self.symbol}", "backfill", "WARN", "REQ_ERR",
                                   f"Request error: {ex}; backing off {backoff}s", {"attempt": attempt, "error": str(ex)})
                        time.sleep(backoff)
                        continue

                    if r.status_code == 429:
                        retry_after = float(r.headers.get('Retry-After', 1.5))
                        emit_event("feeder", f"binance:{self.symbol}", "backfill", "WARN", "RATE_LIMIT",
                                   f"Rate limited; sleeping {retry_after}s", {"status": 429})
                        time.sleep(retry_after)
                        continue

                    if r.status_code >= 500:
                        backoff = min(10, 2 ** attempt)
                        emit_event("feeder", f"binance:{self.symbol}", "backfill", "WARN", "SERVER_ERR",
                                   f"Server error {r.status_code}; backing off {backoff}s", {"status": r.status_code})
                        time.sleep(backoff)
                        continue

                    r.raise_for_status()
                    data = r.json()
                    if not data:
                        # no more rows available
                        next_id = int(gap_end) + 1
                        break

                    # Buffer rows into all_rows so we can return a combined list
                    if data:
                        all_rows.extend(data)

                    # Advance to last returned id + 1. Spot trades expose 'id'.
                    try:
                        last_id = int(data[-1].get('id') or data[-1].get('a'))
                    except Exception:
                        # if malformed, abort to avoid endless loop
                        emit_event("feeder", f"binance:{self.symbol}", "backfill", "ERROR", "NO_ID",
                                   "Page rows lack identifiable id; aborting", {})
                        next_id = int(gap_end) + 1
                        break

                    next_id = last_id + 1
                    requests_made += 1

                    # polite pacing between pages (tunable on GapFiller)
                    time.sleep(getattr(self, 'min_sleep_between_requests', 0.02))
                    break

            # nothing to do per-gap; all rows appended to all_rows

            emit_event("feeder", f"binance:{self.symbol}", "backfill", "INFO", "BACKFILL_DONE",
                       f"Finished Binance backfill for {self.symbol} ids {gap_start}..{gap_end}", {"gap_start": gap_start, "gap_end": gap_end, "requests": requests_made})

        return all_rows

