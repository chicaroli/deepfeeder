"""Persistence layer public API.

Provides directory helpers and the journal service used to mirror live feed
rows to Parquet and support replay with de-duplication.

This module is import-safe in Deephaven app-mode. Heavy side effects (threads)
are only started via explicit JournalService.start() or dfb.start_journal().
"""
from __future__ import annotations

from .paths import (
    DATA_ROOT,
    PERSIST_ROOT,
    HOT_DIR,
    BINANCE_HOT_DIR,
    TV_HOT_BARS_DIR,
    META_DIR,
    DUCKDB_PATH,
    ensure_dirs,
)
from .journal import JournalService

__all__ = [
    "DATA_ROOT",
    "PERSIST_ROOT",
    "HOT_DIR",
    "BINANCE_HOT_DIR",
    "TV_HOT_BARS_DIR",
    "META_DIR",
    "DUCKDB_PATH",
    "ensure_dirs",
    "JournalService",
]
