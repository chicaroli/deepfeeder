# tests/runtime/test_feeders_specs.py
from ingest.feeder_specs import load_feeder_specs
import json


def test_load_feeder_specs_binance_symbol_uppercase(tmp_path):
    path = tmp_path / "test_feeders_specs_feed.json"
    data = [
        {"provider": "Binance ",
         "name": "trades_btc",
         "symbols": ["btcusdt", "ethusdt"],
         "autostart": True
         }
    ]
    path.write_text(json.dumps(data), encoding="utf-8")
    specs = load_feeder_specs(str(path))
    assert len(specs) == 1
    spec = specs[0]
    assert spec.provider == "binance"
    assert spec.symbols == ["BTCUSDT", "ETHUSDT"]
    assert spec.autostart is True


def test_load_feeder_specs_tradingview_dedup_and_defaults(tmp_path):
    path = tmp_path / "test_feeders_specs_feed.json"
    data = [
        {"provider": "tradingview", "name": "quotes_main", "symbols": ["AAPL", "AAPL", "MSFT", "MSFT", "GOOG"]},
        {"provider": "TRADINGVIEW", "name": "bars_main", "symbols": ["EURUSD", "EURUSD"]},
    ]
    path.write_text(json.dumps(data), encoding="utf-8")
    specs = load_feeder_specs(str(path))
    # provider should be normalized to lowercase
    assert [s.provider for s in specs] == ["tradingview", "tradingview"]
    # duplicates removed, order preserved (first occurrence kept)
    assert specs[0].symbols == ["AAPL", "MSFT", "GOOG"]
    assert specs[1].symbols == ["EURUSD"]
    # autostart default should be False when not provided
    assert specs[0].autostart is False
    assert specs[1].autostart is False


def test_mixed_providers(tmp_path):
    path = tmp_path / "test_feeders_specs_feed.json"
    data = [
        {"provider": "BINANCE", "name": "trades_all", "symbols": ["btcusdt", "adausdt"], "autostart": False},
        {"provider": "tradingview", "name": "quotes_fx", "symbols": ["EURUSD", "GBPUSD", "EURUSD"]},
    ]
    path.write_text(json.dumps(data), encoding="utf-8")
    specs = load_feeder_specs(str(path))
    assert len(specs) == 2
    b, tv = specs
    assert b.symbols == ["BTCUSDT", "ADAUSDT"]  # uppercased
    assert tv.symbols == ["EURUSD", "GBPUSD"]   # dedup only
    assert b.autostart is False and tv.autostart is False
