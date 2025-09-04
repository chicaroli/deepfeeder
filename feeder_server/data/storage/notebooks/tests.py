# filepath: feeder_server/data/storage/notebooks/tests.py
"""Quick manual test for a market SymListener.

This script is intended to be run inside the Deephaven app/container to
exercise the listener initialization path. It is deliberately conservative:
- uses the public `deepfeeder` and `fanout` APIs
- avoids heavy work at import time
- starts the listener briefly and then stops it

Adjust provider/data_schema/symbol as needed.
"""

from typing import Any
import time

import deepfeeder as dfb
import marketfeeder as mfb


def main() -> None:
    """Initialize a SymListener for a given symbol, start it briefly and stop it."""
    provider = "binance"
    data_schema = "ohlcv_1m"
    symbol = "BTCUSDT"

    # Resolve schema
    schemas = mfb.get_schemas()
    spec = schemas.get((provider, data_schema)) if schemas is not None else None
    if spec is None:
        print(f"[tests] schema not found for {provider}/{data_schema}")
        return

    # Get a (lazy) table and create a view filtered to the requested symbol.
    try:
        base = dfb.get_binance_ohlcv_1m_table()
    except Exception as e:  # pragma: no cover - environment dependent
        print(f"[tests] failed to obtain base table: {e}")
        return

    try:
        view = base.where(f"symbol == '{symbol}'")
    except Exception:
        # If where/view is not supported in this environment, use base directly
        view = base

    def emit_completed(msg: Any) -> None:
        """Callback used by the listener to emit completed bars/messages."""
        print("[tests] emit_completed:", msg)

    # Construct listener from public accessor (may be a class)
    SymListener = mfb.get_sym_listener()
    listener = SymListener(provider, data_schema, symbol, spec, view, emit_completed)

    try:
        listener.start()
        print("[tests] listener started; sleeping briefly to observe activity...")
        time.sleep(1.0)
    except Exception as e:  # pragma: no cover - runtime dependent
        print(f"[tests] listener start failed: {e}")
    finally:
        try:
            listener.stop()
        except Exception:
            pass

    print("[tests] SymListener test completed successfully.")


if __name__ == "__main__":
    main()
