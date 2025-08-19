# Ingestion layer public API
from feeders.base import BaseFeeder  # updated path after moving feeders to top-level
from .manager import feeder_manager, FeederManager  # updated from registry to manager

__all__ = [
    'BaseFeeder',
    'FeederManager',
    'feeder_manager',
]
