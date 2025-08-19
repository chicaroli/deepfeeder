"""
MarketFeeder package init.

Public API for fanout used in Deephaven app-mode.

This module exposes a minimal set of accessors that are safe to import
in app-mode. Heavy work remains in the implementation modules; these
accessors return the existing singletons from `fanout.core`.
"""

from typing import Any

from .core import market_feeder, SYM_LISTENER
from .schemas import SCHEMAS

def get_market_feeder() -> Any:
    """Return the market_feeder singleton from fanout.core."""
    return market_feeder


def get_sym_listener() -> Any:
    """Return the SYM_LISTENER class from fanout.core (internal use)."""
    return SYM_LISTENER


def get_schemas() -> Any:
    """Return the SCHEMAS registry from fanout.core."""
    return SCHEMAS


__all__ = ["get_market_feeder", "get_sym_listener", "get_schemas", "market_feeder"]
