"""Public API for deepfeeder in Deephaven app-mode.

Provides:
- services container (lazy singletons)
- orchestrator service (registered at startup in app.py)
- live table getters (status, configs, provider data, threads)
"""
from __future__ import annotations
from typing import Any, Dict

# --- NEW core/storage/sink services ---------------------------------------
from config.paths import PATHS
from core.event_bus import EventBus
from storage.eventstore_duckdb import DuckDbEventStore
from storage.journal_duckdb import DuckDbJournal
from sinks.registry import WriterRegistry
from sinks.dh_sink import DhSinkDynamic

from ingest.manager_tables import get_status_table, get_configs_table

from providers.binance import (
    binance_trades_table as get_binance_trades_table,
    binance_ohlcv_1m as get_binance_ohlcv_1m_table,
    binance_ohlcv_1m_filled as get_binance_ohlcv_1m_filled_table,
    binance_ohlcv_5m as get_binance_ohlcv_5m_table,
    binance_ohlcv_5m_filled as get_binance_ohlcv_5m_filled_table,
)
from feeders.tradingview import (
    tv_quotes_table as get_tv_quotes_table,
    tv_bars_table as get_tv_bars_table,
    tv_bars_table_deduped as get_tv_bars_table_deduped,
    tv_ohlcv_1m_from_quotes as get_tv_ohlcv_1m_from_quotes,
    tv_ohlcv_1m_filled as get_tv_ohlcv_1m_filled,
    tv_ohlcv_1m as get_tv_ohlcv_1m_table,
    tv_ohlcv_5m as get_tv_ohlcv_5m_table,
)
from feeders.bins import bins_recent
from fanout import get_fanout_stats_table as fanout_get_stats_table

from runtime.threads_bus import get_threads_table
from runtime.eventlog_bus import get_eventlog_table
from runtime.services import Services

# convenience namespace
import providers
import ingest
import fanout
import ui
import runtime

# --- services container ----------------------------------------------------
services = Services()
services.register("event_store", lambda: DuckDbEventStore(str(PATHS.event_store_db)))
services.register("journal_store", lambda: DuckDbJournal(str(PATHS.journal_db)))
_es = services.get("event_store")
services.register("event_bus", lambda es=_es: EventBus(es, max_envelopes=100_000))

# Provide a factory that returns the dict bound to your real DH writers.
def _make_dh_registry() -> WriterRegistry:
    reg = WriterRegistry()
    # Wrap your existing DynamicTableWriters here:

    # TradingView:
    # reg.add(
    #     provider="tradingview",
    #     stream="quotes",
    #     writer=feeders.tradingview.schema.tv_quotes_writer(),
    #     flatten=providers.tradingview.adapter.flatten_quotes
    #     )
    # reg.add(
    #     provider="tradingview",
    #     stream="bars",
    #     writer=feeders.tradingview.schema.tv_bars_writer(),
    #     flatten=providers.tradingview.adapter.flatten_ohlcv_1m
    #     )

    # Binance:
    reg.add(
        provider="binance",
        stream="trades",
        writer=providers.binance.schema.binance_trades_writer(),
        flatten=providers.binance.adapter.flatten_trades_for_dh
        )

    return reg

services.register("dh_sink", lambda: DhSinkDynamic(_make_dh_registry()))


def get_services_container():
    return services

def get_service(name: str):
    return services.get(name)


# --- table helpers ---------------------------------------------------------
Table = Any
def tables() -> Dict[str, Table]:
    """Return dict of commonly used live Deephaven tables."""
    return {
        "status": get_status_table(),
        "configs": get_configs_table(),
        "threads": get_threads_table(),
        "eventlog": get_eventlog_table(),
        "binance_trades": get_binance_trades_table(),
        "binance_ohlcv_1m": get_binance_ohlcv_1m_table(),
        "binance_ohlcv_1m_filled": get_binance_ohlcv_1m_filled_table(),
        "binance_ohlcv_5m": get_binance_ohlcv_5m_table(),
        "binance_ohlcv_5m_filled": get_binance_ohlcv_5m_filled_table(),
        "tv_quotes": get_tv_quotes_table(),
        "tv_bars": get_tv_bars_table(),
        "tv_bars_deduped": get_tv_bars_table_deduped(),
        "tv_ohlcv_1m_from_quotes": get_tv_ohlcv_1m_from_quotes(),
        "tv_ohlcv_1m_filled": get_tv_ohlcv_1m_filled(),
        "tv_ohlcv_1m": get_tv_ohlcv_1m_table(),
        "tv_ohlcv_5m": get_tv_ohlcv_5m_table(),
        # Lightweight recent windows (hard-coded 2 bars: current + previous)
        "bins_1m_recent": bins_recent(1, 2),
        "bins_5m_recent": bins_recent(5, 2),
    }

# Convenience re-export
get_fanout_stats_table = fanout_get_stats_table  # type: ignore

__all__ = [
    # Services API
    "services", "get_services_container", "get_service",
    # Table getters
    "get_status_table", "get_configs_table", "get_threads_table",
    "get_binance_trades_table", "get_binance_ohlcv_1m_table", "get_binance_ohlcv_1m_filled_table",
    "get_binance_ohlcv_5m_table", "get_binance_ohlcv_5m_filled_table",

    "get_tv_quotes_table", "get_tv_bars_table", "get_tv_bars_table_deduped",
    "get_tv_ohlcv_1m_from_quotes",
    "get_tv_ohlcv_1m_filled",
    "get_tv_ohlcv_1m_table",
    "get_tv_ohlcv_5m_table",

    # Helpers
    "tables", "get_fanout_stats_table",
    # Namespaces
    "ingest", "fanout", "ui",
    "runtime",
    # Event log
    "get_eventlog_table",
]
