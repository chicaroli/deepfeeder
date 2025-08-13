# app.d/app.py (additions)
import sys, types, os
from core.registry import REGISTRY
from core.bus import get_trades_table, get_status_table, get_configs_table
from providers.binance_schema import binance_trades_table, binance_ohlcv_1m
from providers.tradingview_schema import tv_quotes_table, tv_ohlcv_1m_from_quotes

from deephaven import merge
from deephaven import pandas as dhpd
import pandas as pd

def configs_list() -> list[dict]:
    return REGISTRY.list_configs()

def configs_table():
    # lightweight snapshot table for UI
    cfgs = REGISTRY.list_configs()
    if not cfgs:
        return dhpd.to_table(pd.DataFrame(columns=["provider","name","symbols","autostart"]))
    return dhpd.to_table(pd.DataFrame(cfgs))

# existing bindings...
_bind = types.ModuleType("deepfeeder_bindings")

# Control
_bind.start_feeder = lambda provider, name, symbols: REGISTRY.start(provider, name, symbols)
_bind.stop_feeder = lambda provider, name: REGISTRY.stop(provider, name)
_bind.stop_all = REGISTRY.stop_all
_bind.reload_configs = REGISTRY.reload_configs
_bind.start_all_autostart = REGISTRY.start_all_autostart

# Config
_bind.configs_list = configs_list

# Tables
_bind.status_table = get_status_table()
_bind.configs_live_table = get_configs_table()              # LIVE mirror from registry/bus
_bind.trades_table = get_trades_table()                     # generic, cross-provider
## Binance
_bind.binance_trades_detailed = binance_trades_table()      # rich schema
_bind.binance_ohlcv_1m = binance_ohlcv_1m()                 # derived candles
## TradingView
_bind.tv_quotes = tv_quotes_table()
_bind.tv_ohlcv_1m = tv_ohlcv_1m_from_quotes()

sys.modules["deepfeeder_bindings"] = _bind
print("[deepfeeder] bindings installed: import deepfeeder_bindings as dfb")

# optional: autostart on boot (guarded by env)
if os.getenv("DEEPFEEDER_AUTOSTART", "1") not in ("0", "false", "False"):
    print("[deepfeeder] auto-starting configured feeders...")
    print(REGISTRY.start_all_autostart())
