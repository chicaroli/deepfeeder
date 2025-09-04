# tests/providers/test_binance_adapter.py
import pytest
from providers.binance.adapter import flatten_trades, trade_json_to_tick
from core.contracts import Tick


@pytest.mark.unit
def test_trade_json_to_tick_shape(binance_msg):
    t = trade_json_to_tick(binance_msg)

    assert isinstance(t, Tick)
    assert t.provider == "binance"
    assert t.stream == "trades"
    assert t.symbol == binance_msg["s"]
    assert t.seq == binance_msg["t"]
    assert t.ts_ns == binance_msg["T"] * 1_000_000
    assert t.payload["price"] == float(binance_msg["p"])
    assert t.payload["qty"] == float(binance_msg["q"])
    assert t.payload["side"] in ("buy", "sell")  # m=True -> "sell", else "buy"


@pytest.mark.unit
def test_flatten_trades_rowdict(binance_msg):
    # make two ticks (m True/False)
    msg1 = {**binance_msg, "m": True,  "t": binance_msg["t"] + 1}
    msg2 = {**binance_msg, "m": False, "t": binance_msg["t"] + 2}
    ticks = [trade_json_to_tick(msg1), trade_json_to_tick(msg2)]

    rowdict = flatten_trades(ticks)
    # columns present
    for k in ["Provider", "Stream", "Symbol", "TsNanos", "TradeId", "Price", "Qty", "Side"]:
        assert k in rowdict
        assert len(rowdict[k]) == 2

    # sample values
    assert rowdict["Provider"] == ["binance", "binance"]
    assert rowdict["Stream"]   == ["trades", "trades"]
    assert rowdict["TradeId"]  == [msg1["t"], msg2["t"]]
    assert rowdict["Side"]     == ["sell", "buy"]  # m=True -> sell, m=False -> buy
