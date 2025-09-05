# providers/binance/rest_client.py
from __future__ import annotations
import time
from typing import Dict, List, Optional, Tuple
import json
import urllib.request
import urllib.error

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
        use_agg_trades: bool = False,
        user_agent: str = "DeepFeeder/1.0",
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = float(timeout_s)
        self.api_key = api_key
        self.use_agg = bool(use_agg_trades)
        self.user_agent = user_agent

    def _request(self, path: str, params: Dict[str, str], *, max_retries: int = 5) -> List[Dict]:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{self.base_url}{path}?{qs}"
        headers = {
            "User-Agent": self.user_agent,
        }
        if path.startswith("/api/v3/historicalTrades") and self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        delay = 0.5
        for attempt in range(max_retries):
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    data = resp.read()
                    return json.loads(data.decode("utf-8"))
            except urllib.error.HTTPError as e:
                # 429/5xx → backoff and retry
                if e.code in (429, 418, 500, 502, 503, 504):
                    time.sleep(delay)
                    delay = min(delay * 1.7, 10.0)
                    continue
                raise
            except Exception:
                time.sleep(delay)
                delay = min(delay * 1.7, 10.0)
        return []

    def get_trades(self, symbol: str, from_id: int, limit: int = 1000) -> List[Dict]:
        """
        Return ascending-by-id list of trades dicts, each with:
          - 'id' (tradeId or aggTradeId), 'price', 'qty', 'time', 'isBuyerMaker'
        """
        if self.use_agg:
            # /api/v3/aggTrades: fromId, limit
            path = "/api/v3/aggTrades"
            params = {"symbol": symbol.upper(), "fromId": str(from_id), "limit": str(limit)}
            out = self._request(path, params)
            # Normalize keys to resemble trade format
            norm: List[Dict] = []
            for r in out:
                norm.append({
                    "id": int(r.get("a")),                      # aggTradeId
                    "price": r.get("p"),
                    "qty": r.get("q"),
                    "time": int(r.get("T")),
                    "isBuyerMaker": bool(r.get("m")),
                })
            norm.sort(key=lambda x: x["id"])
            return norm
        else:
            # /api/v3/historicalTrades: fromId, limit   (needs API key)
            path = "/api/v3/historicalTrades"
            params = {"symbol": symbol.upper(), "fromId": str(from_id), "limit": str(limit)}
            out = self._request(path, params)
            norm: List[Dict] = []
            for r in out:
                norm.append({
                    "id": int(r.get("id")),                     # tradeId
                    "price": r.get("price"),
                    "qty": r.get("qty"),
                    "time": int(r.get("time")),
                    "isBuyerMaker": bool(r.get("isBuyerMaker")),
                })
            norm.sort(key=lambda x: x["id"])
            return norm
