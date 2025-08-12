# app.d/app.py
import sys, types
from core.registry import REGISTRY
from core.bus import get_trades_table, get_status_table
from providers.binance_schema import binance_trades_table, binance_ohlcv_1m

def _norm_csv(csv: str):
    return [s.strip().lower() for s in csv.split(",") if s.strip()]

def start_feeder(provider: str, name: str, symbols: list[str]) -> str:
    return REGISTRY.start(provider, name, symbols)

def start_feeder_csv(provider: str, name: str, symbols_csv: str) -> str:
    return REGISTRY.start(provider, name, _norm_csv(symbols_csv))

def stop_feeder(provider: str, name: str) -> str:
    return REGISTRY.stop(provider, name)

def update_symbols(provider: str, name: str, symbols: list[str]) -> str:
    return REGISTRY.update_symbols(provider, name, symbols)

def status_feeders() -> dict:
    return REGISTRY.status()

# Bindings module (tables only via bindings, so Panels stays clean)
_bind = types.ModuleType("deepfeeder_bindings")
_bind.start_feeder = start_feeder
_bind.start_feeder_csv = start_feeder_csv
_bind.stop_feeder = stop_feeder
_bind.update_symbols = update_symbols
_bind.status_feeders = status_feeders
_bind.binance_trades = get_trades_table()
_bind.status_table = get_status_table()
_bind.trades_table = get_trades_table()                     # generic, cross-provider
_bind.binance_trades_detailed = binance_trades_table()      # rich schema
_bind.binance_ohlcv_1m = binance_ohlcv_1m()                 # derived candles
sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")
