from __future__ import annotations
import time
from typing import Dict, List, Optional
import requests
from requests.exceptions import RequestException


class BinanceRest:
    """
    Minimal REST client for Binance trades backfill.

    NOTE:
      - Choose which endpoint to use based on your canonical 'seq':
        * If your Tick.seq equals 'tradeId' from the WS Trade Stream:
            Consider /api/v3/historicalTrades (requires API key) with fromId
        * If you canonicalized to aggregated trade ids:
            Use /api/v3/aggTrades with fromId
      - This client defaults to /api/v3/historicalTrades for real tradeId paging.

    You can override `base_url` or switch to aggTrades by setting `use_agg_trades=True`.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str = "https://api.binance.com",
        timeout_s: float = 10.0,
        user_agent: str = "DeepFeeder/1.0",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = float(timeout_s)
        self.api_key = api_key
        self.user_agent = user_agent

    def _request(self, path: str, params: Dict[str, str], *, max_retries: int = 5) -> List[Dict]:
        """Perform a GET request to Binance and return JSON-decoded list on success.

        Retries on network errors and on specific HTTP statuses (429, 5xx, etc.)
        using exponential-ish backoff capped at 10s. Returns empty list on
        repeated failures to match previous behavior.
        """
        url = f"{self.base_url}{path}"
        headers = {"User-Agent": self.user_agent}
        if path.startswith("/api/v3/historicalTrades") and self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        retry_statuses = {429, 418, 500, 502, 503, 504}
        delay = 0.5
        for _ in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=self.timeout_s)
                status = resp.status_code
                if status == 200:
                    # requests already decodes JSON; ensure it's a list as expected
                    return resp.json()
                if status in retry_statuses:
                    time.sleep(delay)
                    delay = min(delay * 1.7, 10.0)
                    continue
                resp.raise_for_status()
            except RequestException:
                # Network error or timeout — backoff and retry
                time.sleep(delay)
                delay = min(delay * 1.7, 10.0)
                continue
        return []

    def get_trades(self, symbol: str, from_id: int, limit: int = 1000) -> List[Dict]:
        """Return the raw list of trade rows returned by Binance's REST API.

        The rows are left un-normalized so callers can shape them as needed.
        """
        path = "/api/v3/historicalTrades"
        params = {"symbol": symbol.upper(), "fromId": str(from_id), "limit": str(limit)}
        return self._request(path, params)
