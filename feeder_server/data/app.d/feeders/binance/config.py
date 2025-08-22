"""Binance feeder configuration.

Centralizes environment-derived tuning knobs so docker-compose stays clean.

Environment variables (all optional):
  # Warm replay on feeder start (provider override; falls back to global)
  DEEPFEEDER_BINANCE_REPLAY_ON_START      0/1   default 0 (fallback DEEPFEEDER_REPLAY_ON_START)
  DEEPFEEDER_BINANCE_REPLAY_WINDOW_SECS   int   default 900 (fallback DEEPFEEDER_REPLAY_WINDOW_SECS)

  DEEPFEEDER_BINANCE_BATCH_SIZE           int   default 10000
  DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S     float default 1.0
  DEEPFEEDER_BINANCE_METRICS_ENABLED      0/1   default 1
  DEEPFEEDER_BINANCE_METRICS_INTERVAL     float default 60
  DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA  int   default 500
"""
from __future__ import annotations
from dataclasses import dataclass
import os

@dataclass(slots=True)
class BinanceConfig:
    batch_size: int = 10000
    flush_interval_s: float = 1.0
    metrics_enabled: bool = True
    metrics_interval: float = 60.0
    metrics_min_q_delta: int = 500
    # Warm replay settings
    warm_replay_on_start: bool = True
    warm_replay_window_secs: int = 900


def _get_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return v not in ("0", "false", "False", "")


def load_config() -> BinanceConfig:
    # global fallbacks
    _global_replay_on = os.getenv("DEEPFEEDER_REPLAY_ON_START", "0")
    _global_replay_secs = os.getenv("DEEPFEEDER_REPLAY_WINDOW_SECS", "900")
    return BinanceConfig(
        batch_size=int(os.getenv("DEEPFEEDER_BINANCE_BATCH_SIZE", "10000")),
        flush_interval_s=float(os.getenv("DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S", "1.0")),
        
        metrics_enabled=_get_bool("DEEPFEEDER_BINANCE_METRICS_ENABLED", "1"),
        metrics_interval=float(os.getenv("DEEPFEEDER_BINANCE_METRICS_INTERVAL", "60")),
        metrics_min_q_delta=int(os.getenv("DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA", "500")),
        warm_replay_on_start=_get_bool("DEEPFEEDER_BINANCE_REPLAY_ON_START", _global_replay_on),
        warm_replay_window_secs=int(os.getenv("DEEPFEEDER_BINANCE_REPLAY_WINDOW_SECS", _global_replay_secs)),
    )

__all__ = ["BinanceConfig", "load_config"]
