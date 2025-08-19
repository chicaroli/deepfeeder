"""
MarketFeeder package init.

Public API for fanout used in Deephaven app-mode.

This module exposes a minimal set of accessors that are safe to import
in app-mode. Heavy work remains in the implementation modules; these
accessors return the existing singletons from `fanout.core`.
"""

from typing import Any

from .core import MARKET_FEEDER, SYM_LISTENER
from .schemas import SCHEMAS

def get_market_feeder() -> Any:
    """Return the MARKET_FEEDER singleton from fanout.core."""
    return MARKET_FEEDER


def get_sym_listener() -> Any:
    """Return the SYM_LISTENER singleton from fanout.core."""
    return SYM_LISTENER


def get_schemas() -> Any:
    """Return the SCHEMAS registry from fanout.core."""
    return SCHEMAS


__all__ = ["get_market_feeder", "get_sym_listener", "get_schemas"]

