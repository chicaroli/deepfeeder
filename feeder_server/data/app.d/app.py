# app.d/app.py
import sys, types
from os import getenv
from core.registry import REGISTRY
from core.bus import get_status_table, get_configs_table
from providers.binance_schema import binance_trades_table, binance_ohlcv_1m
from providers.tradingview_schema import tv_quotes_table, tv_ohlcv_1m_from_quotes, tv_ohlcv_5m_from_quotes
from marketfeeder.core import MARKET_FEEDER, SYM_LISTENER, SCHEMAS

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
tb_status_table = get_status_table()
tb_configs_live_table = get_configs_table()              # LIVE mirror from registry/bus
## Binance
tb_binance_trades = binance_trades_table()
tb_binance_ohlcv_1m = binance_ohlcv_1m()                 # derived candles
## TradingView
tb_tv_quotes = tv_quotes_table()
tb_tv_ohlcv_1m = tv_ohlcv_1m_from_quotes()
tb_tv_ohlcv_5m = tv_ohlcv_5m_from_quotes()

# _bind assignments remain for internal use
_bind.status_table = tb_status_table
_bind.configs_live_table = tb_configs_live_table
_bind.binance_trades = tb_binance_trades
_bind.binance_ohlcv_1m = tb_binance_ohlcv_1m
_bind.tv_quotes = tb_tv_quotes
_bind.tv_ohlcv_1m = tb_tv_ohlcv_1m
_bind.tv_ohlcv_5m = tb_tv_ohlcv_5m


sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")

# Register MarketFeeder Registry
_mf_bind = types.ModuleType("marketfeeder_bindings")
_mf_bind.market_feeder = MARKET_FEEDER
_mf_bind.sym_listener = SYM_LISTENER
_mf_bind.schemas = SCHEMAS
sys.modules["marketfeeder_bindings"] = _mf_bind
print("[deepfeeder] marketfeeder bindings installed: import marketfeeder_bindings as mfb")

# optional: autostart on boot (guarded by env)
if getenv("DEEPFEEDER_AUTOSTART", "1") not in ("0", "false", "False"):
    print("[deepfeeder] auto-starting configured feeders...")
    print(REGISTRY.start_all_autostart())
