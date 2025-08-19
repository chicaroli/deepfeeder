"""
MarketFeeder package init.

Public API for marketfeeder used in Deephaven app-mode.

This module exposes a minimal set of accessors that are safe to import
in app-mode. Heavy work remains in the implementation modules; these
accessors return the existing singletons from `marketfeeder.core`.
"""

from typing import Any

from marketfeeder.core import MARKET_FEEDER, SYM_LISTENER, SCHEMAS


def get_market_feeder() -> Any:
    """Return the MARKET_FEEDER singleton from marketfeeder.core."""
    return MARKET_FEEDER


def get_sym_listener() -> Any:
    """Return the SYM_LISTENER singleton from marketfeeder.core."""
    return SYM_LISTENER


def get_schemas() -> Any:
    """Return the SCHEMAS registry from marketfeeder.core."""
    return SCHEMAS


__all__ = ["get_market_feeder", "get_sym_listener", "get_schemas"]

