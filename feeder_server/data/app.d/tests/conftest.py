# tests/conftest.py
import sys
import time
from pathlib import Path
from typing import Optional
import pytest

# Make app.d a top-level import root so `from providers...` works
APPD = Path(__file__).resolve().parents[1]  # .../app.d
if str(APPD) not in sys.path:
    sys.path.insert(0, str(APPD))


# --- Binance test sample builder ---
def sample_binance_trade(symbol="BTCUSDT", price="65321.12", qty="0.010", ts_ms: Optional[int] = None, trade_id: int = 42, is_buyer_maker=True):
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)
    # Simplified spot trade payload (subset)
    return {
        "e": "trade",
        "E": ts_ms,                  # event time (ms)
        "s": symbol,                 # symbol
        "t": trade_id,               # trade id
        "p": price,                  # price (string)
        "q": qty,                    # qty (string)
        "T": ts_ms,                  # trade time (ms)
        "m": is_buyer_maker,         # true => seller aggressive, so trade initiator is SELL
    }

@pytest.fixture
def binance_msg():
    return sample_binance_trade()
