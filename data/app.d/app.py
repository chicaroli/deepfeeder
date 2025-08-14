# app.d/app.py
import sys, types
from os import getenv
from core.registry import REGISTRY
from core.bus import get_status_table, get_configs_table
from providers.binance_schema import binance_trades_table, binance_ohlcv_1m
from providers.tradingview_schema import tv_quotes_table, tv_ohlcv_1m_from_quotes, tv_ohlcv_5m_from_quotes

def configs_list() -> list[dict]:
    return REGISTRY.list_configs()

# existing bindings...
_bind = types.ModuleType("deepfeeder_bindings")

# Control
_bind.start_feeder = lambda provider, name, symbols: REGISTRY.start(provider, name, symbols)
_bind.stop_feeder = lambda provider, name: REGISTRY.stop(provider, name)
_bind.stop_all = REGISTRY.stop_all
_bind.start_all = REGISTRY.start_all

# Config
_bind.configs_list = configs_list
_bind.reload_configs = REGISTRY.reload_configs

# Tables
_bind.status_table = get_status_table()
_bind.configs_live_table = get_configs_table()              # LIVE mirror from registry/bus
## Binance
_bind.binance_trades = binance_trades_table()
_bind.binance_ohlcv_1m = binance_ohlcv_1m()                 # derived candles
## TradingView
_bind.tv_quotes = tv_quotes_table()
_bind.tv_ohlcv_1m = tv_ohlcv_1m_from_quotes()
_bind.tv_ohlcv_5m = tv_ohlcv_5m_from_quotes()

sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")

# optional: autostart on boot (guarded by env)
if getenv("DEEPFEEDER_AUTOSTART", "1") not in ("0", "false", "False"):
    print("[deepfeeder] auto-starting configured feeders...")
    print(REGISTRY.start_all_autostart())
