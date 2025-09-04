# persistence/config.py

"""Journal configuration module.

Provides a typed config with environment-driven defaults, mirroring feeder configs.

Environment variables (all optional):
  DEEPFEEDER_JOURNAL_FLUSH_ROWS            int   default 10000
  DEEPFEEDER_JOURNAL_FLUSH_SECS            float default 1.0
  DEEPFEEDER_JOURNAL_MAX_ROWS_PER_FILE     int   default unset/0 (ignored when <= 0)
  DEEPFEEDER_JOURNAL_MAX_ROWS_PER_GROUP    int   default unset/0 (ignored when <= 0)
  DEEPFEEDER_JOURNAL_COMPACT_ENABLED       0/1   default 1 (truthy unless 0/false)
  DEEPFEEDER_JOURNAL_COMPACT_INTERVAL_SECS float default 180
  DEEPFEEDER_JOURNAL_COMPACT_STABLE_SECS   float default 60
  DEEPFEEDER_JOURNAL_COMPACT_MIN_FILES     int   default 8
  DEEPFEEDER_JOURNAL_MASTER_PREFIX         str   default "master-"
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os


def _get_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return v not in ("0", "false", "False", "")


def _get_int_opt(name: str) -> Optional[int]:
    v = os.getenv(name)
    if v is None:
        return None
    try:
        i = int(v)
        return i if i > 0 else None
    except Exception:
        return None


@dataclass(slots=True)
class JournalConfig:
    """Dataclass holding journaling and compaction parameters."""

    flush_rows: int = 5000
    flush_secs: float = 5.0
    max_rows_per_file: Optional[int] = None
    max_rows_per_group: Optional[int] = None
    # Compaction
    compact_enabled: bool = True
    compact_interval_secs: float = 180.0
    compact_stable_secs: float = 60.0
    compact_min_files: int = 8
    master_prefix: str = "master-"
    # Logging
    log_flush_enabled: bool = False


def load_config() -> JournalConfig:
    """Load journal config from environment with defaults."""
    return JournalConfig(
        # flush settings
        flush_rows=int(os.getenv("DEEPFEEDER_JOURNAL_FLUSH_ROWS", "10000")),
        flush_secs=float(os.getenv("DEEPFEEDER_JOURNAL_FLUSH_SECS", "1.0")),
        log_flush_enabled=_get_bool("DEEPFEEDER_JOURNAL_LOG_FLUSH", "0"),
        max_rows_per_file=_get_int_opt("DEEPFEEDER_JOURNAL_MAX_ROWS_PER_FILE"),
        max_rows_per_group=_get_int_opt("DEEPFEEDER_JOURNAL_MAX_ROWS_PER_GROUP"),

        # compaction settings
        compact_enabled=_get_bool("DEEPFEEDER_JOURNAL_COMPACT_ENABLED", "1"),
        compact_interval_secs=float(os.getenv("DEEPFEEDER_JOURNAL_COMPACT_INTERVAL_SECS", "120")),
        compact_stable_secs=float(os.getenv("DEEPFEEDER_JOURNAL_COMPACT_STABLE_SECS", "10")),
        compact_min_files=int(os.getenv("DEEPFEEDER_JOURNAL_COMPACT_MIN_FILES", "5")),
        master_prefix=os.getenv("DEEPFEEDER_JOURNAL_MASTER_PREFIX", "master-"),

    )


__all__ = ["JournalConfig", "load_config"]
