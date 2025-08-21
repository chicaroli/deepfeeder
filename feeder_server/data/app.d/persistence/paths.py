"""Path configuration for persistence layer.

All folders live under /data by default, with a dedicated persistence root:

- /data/persistence/hot/binance_trades
- /data/persistence/hot/tv_quotes
- /data/persistence/meta/state.duckdb

These paths are safe for containerized Deephaven; /data is bind-mounted by
compose and .dockerignore excludes /data/persistence from image builds.
"""
from __future__ import annotations

import os
from typing import List

DATA_ROOT: str = os.getenv("DEEPNODE_DATA_ROOT", "/data")
PERSIST_ROOT: str = os.path.join(DATA_ROOT, "persistence")
HOT_DIR: str = os.path.join(PERSIST_ROOT, "hot")
BINANCE_HOT_DIR: str = os.path.join(HOT_DIR, "binance_trades")
TV_HOT_DIR: str = os.path.join(HOT_DIR, "tv_quotes")
META_DIR: str = os.path.join(PERSIST_ROOT, "meta")
DUCKDB_PATH: str = os.path.join(META_DIR, "state.duckdb")


def ensure_dirs() -> None:
    """Create required directories if they don't exist."""
    for d in [PERSIST_ROOT, HOT_DIR, BINANCE_HOT_DIR, TV_HOT_DIR, META_DIR]:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception:
            # best-effort; caller can re-raise if needed
            pass
