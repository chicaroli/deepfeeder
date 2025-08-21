"""TradingView feeder configuration module.

Mirrors the Binance feeder config pattern to keep docker-compose uncluttered.

Environment variables (all optional):
  DEEPFEEDER_TV_BATCH_SIZE            int   default 200
  DEEPFEEDER_TV_FLUSH_INTERVAL_S      float default 0.25
  DEEPFEEDER_TV_METRICS_ENABLED       0/1   default 1 (truthy unless 0/false)
  DEEPFEEDER_TV_METRICS_INTERVAL      float default 60
  DEEPFEEDER_TV_METRICS_MIN_Q_DELTA   int   default 200
"""
from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(slots=True)
class TradingViewConfig:
    """Dataclass holding TradingView feeder tuning parameters."""

    batch_size: int = 200
    flush_interval_s: float = 0.25
    metrics_enabled: bool = True
    metrics_interval: float = 60.0
    metrics_min_q_delta: int = 200


def _get_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return v not in ("0", "false", "False", "")


def load_config() -> TradingViewConfig:
    """Load config from environment with defaults."""

    return TradingViewConfig(
        batch_size=int(os.getenv("DEEPFEEDER_TV_BATCH_SIZE", "200")),
        flush_interval_s=float(os.getenv("DEEPFEEDER_TV_FLUSH_INTERVAL_S", "0.25")),
        metrics_enabled=_get_bool("DEEPFEEDER_TV_METRICS_ENABLED", "1"),
        metrics_interval=float(os.getenv("DEEPFEEDER_TV_METRICS_INTERVAL", "60")),
        metrics_min_q_delta=int(os.getenv("DEEPFEEDER_TV_METRICS_MIN_Q_DELTA", "200")),
    )


__all__ = ["TradingViewConfig", "load_config"]
