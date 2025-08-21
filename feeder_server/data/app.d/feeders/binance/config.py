"""Binance feeder configuration.

Centralizes environment-derived tuning knobs so docker-compose stays clean.

Environment variables (all optional):
  DEEPFEEDER_BINANCE_BATCH_SIZE           int   default 400
  DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S     float default 0.1
  DEEPFEEDER_BINANCE_METRICS_ENABLED      0/1   default 1
  DEEPFEEDER_BINANCE_METRICS_INTERVAL     float default 60
  DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA  int   default 500
"""
from __future__ import annotations
from dataclasses import dataclass
import os

@dataclass(slots=True)
class BinanceConfig:
    batch_size: int = 400
    flush_interval_s: float = 0.1
    metrics_enabled: bool = True
    metrics_interval: float = 60.0
    metrics_min_q_delta: int = 500


def _get_bool(name: str, default: str = "1") -> bool:
    v = os.getenv(name, default)
    return v not in ("0", "false", "False", "")


def load_config() -> BinanceConfig:
    return BinanceConfig(
        batch_size=int(os.getenv("DEEPFEEDER_BINANCE_BATCH_SIZE", "400")),
        flush_interval_s=float(os.getenv("DEEPFEEDER_BINANCE_FLUSH_INTERVAL_S", "0.1")),
        metrics_enabled=_get_bool("DEEPFEEDER_BINANCE_METRICS_ENABLED", "1"),
        metrics_interval=float(os.getenv("DEEPFEEDER_BINANCE_METRICS_INTERVAL", "60")),
        metrics_min_q_delta=int(os.getenv("DEEPFEEDER_BINANCE_METRICS_MIN_Q_DELTA", "500")),
    )

__all__ = ["BinanceConfig", "load_config"]
