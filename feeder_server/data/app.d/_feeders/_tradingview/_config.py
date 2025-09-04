# _feeders/_tradingview/_config.py
# """TradingView feeder configuration module.
#
# Mirrors the Binance feeder config pattern to keep docker-compose uncluttered.
#
# Environment variables (all optional):
# # Warm replay on feeder start (provider override; falls back to global)
#   DEEPFEEDER_TV_REPLAY_ON_START       0/1   default 0 (fallback DEEPFEEDER_REPLAY_ON_START)
#   DEEPFEEDER_TV_REPLAY_WINDOW_SECS    int   default 900 (fallback DEEPFEEDER_REPLAY_WINDOW_SECS)
#
#   DEEPFEEDER_TV_BATCH_SIZE            int   default 5000
#   DEEPFEEDER_TV_FLUSH_INTERVAL_S      float default 1.0
#   DEEPFEEDER_TV_METRICS_ENABLED       0/1   default 1 (truthy unless 0/false)
#   DEEPFEEDER_TV_METRICS_INTERVAL      float default 60
#   DEEPFEEDER_TV_METRICS_MIN_Q_DELTA   int   default 200
# """
# from __future__ import annotations
#
# from dataclasses import dataclass
# import os
#
#
# @dataclass(slots=True)
# class TradingViewConfig:
#     """Dataclass holding TradingView feeder tuning parameters."""
#
#     batch_size: int = 5000
#     flush_interval_s: float = 1.0
#     metrics_enabled: bool = True
#     metrics_interval: float = 60.0
#     metrics_min_q_delta: int = 200
#     # Warm replay settings
#     warm_replay_on_start: bool = True
#     warm_replay_window_secs: int = 900
#     # Backfill
#     gapfill_scan_interval: int = 150  # seconds between gap scans
#
#
# def _get_bool(name: str, default: str = "1") -> bool:
#     v = os.getenv(name, default)
#     return v not in ("0", "false", "False", "")
#
#
# def load_config() -> TradingViewConfig:
#     """Load config from environment with defaults."""
#     # global fallbacks
#     _global_replay_on = os.getenv("DEEPFEEDER_REPLAY_ON_START", "0")
#     _global_replay_secs = os.getenv("DEEPFEEDER_REPLAY_WINDOW_SECS", "900")
#     return TradingViewConfig(
#         batch_size=int(os.getenv("DEEPFEEDER_TV_BATCH_SIZE", "5000")),
#         flush_interval_s=float(os.getenv("DEEPFEEDER_TV_FLUSH_INTERVAL_S", "1.0")),
#         metrics_enabled=_get_bool("DEEPFEEDER_TV_METRICS_ENABLED", "1"),
#         metrics_interval=float(os.getenv("DEEPFEEDER_TV_METRICS_INTERVAL", "60")),
#         metrics_min_q_delta=int(os.getenv("DEEPFEEDER_TV_METRICS_MIN_Q_DELTA", "200")),
#         warm_replay_on_start=_get_bool("DEEPFEEDER_TV_REPLAY_ON_START", _global_replay_on),
#         warm_replay_window_secs=int(os.getenv("DEEPFEEDER_TV_REPLAY_WINDOW_SECS", _global_replay_secs)),
#     )
#
#
# __all__ = ["TradingViewConfig", "load_config"]
