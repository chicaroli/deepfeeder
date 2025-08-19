# DeepFeeder

**DeepFeeder** is a real-time market data feeder framework built on [Deephaven](https://deephaven.io/).  
It provides **WebSocket-based ingestion**, **live table updates**, **UI controls**, and **configuration persistence** for multiple data providers (starting with Binance).  

The system is designed for **low-latency streaming**, **live analytics**, and **integration into trading infrastructure**.

---

## Recent API Simplification

The public Python API was simplified. Lifecycle alias functions (`start_feeder`, `stop_feeder`, etc.) and lazy attribute table accessors were removed. Use the singleton `feeder_manager` and explicit table getters instead:

```python
import deepfeeder as df

# Lifecycle
res = df.feeder_manager.start("binance", "default", ["btcusdt"])  # start
status = df.feeder_manager.status()                                   # dict snapshot

# Tables (Deephaven objects)
status_t = df.get_status_table()
configs_t = df.get_configs_table()
trades_t = df.get_binance_trades_table()
```

Old names like `deepfeeder.status_table` / `deepfeeder.configs_live_table` are no longer exported.

---

## Features

- **Modular feeder architecture** via `BaseFeeder` for adding new data providers.
- **Binance real-time trades** WebSocket ingestion.
- **Provider-specific schemas** (Binance detailed trades, aggregated OHLCV).
- **Live Deephaven tables** for trades, OHLCV, and feeder status.
- **Persistent feeder configs** stored in JSON, mirrored to live tables.
- **UI Dashboard** for managing feeders in real time.
- **Autostart mode** for predefined configs on server boot.

---

## Requirements

From [`feeder_server/requirements.txt`](feeder_server/requirements.txt):

```text
websocket-client>=1.8.0
pandas>=2.2.2
pyarrow>=16.1.0
pydeephaven>=0.32.0
```

You will also need **Deephaven Server** (Docker recommended).

---

## Project Structure

```text
feeder_server/
  Dockerfile
  requirements.txt
  data/
    app.d/
      app.py
      core/
        base.py                  # BaseFeeder abstract class
        bus.py                   # DynamicTableWriters for configs, trades, status
        registry.py              # FeederRegistry for config persistence & runtime control
        utils.py                 # Utility functions (symbol normalization)
      providers/
        binance_schema.py        # Detailed Binance trades schema + OHLCV aggregation
        binance_feeder.py        # Binance WebSocket feeder implementation
      ui/
        dashboard.py             # Deephaven UI components for managing feeders
    storage/
      notebooks/
        feeders.json             # Persistent configs (autostart, symbols, etc.)
feeder_client/
  Dockerfile
  pyproject.toml
  uv.lock
  src/
    ticking.py                 # Ticking logic for Deephaven integration
    connectors/
      deephaven_connector.py   # Deephaven connector implementation
  tests/
    connectors/
      test_deephaven_connector.py
requirements.txt
LICENSE
docker-compose.yml
README.md
```

---

## Quick Start

### Deephaven Server

Start the Deephaven server using Docker Compose:

```bash
docker compose up --build
```

### Feeder Client

The feeder client is located in the `feeder_client/` directory. It provides code for connecting to market data sources and interacting with Deephaven tables, including support for ticking features.

> **Note:** The feeder client must run on Linux due to Deephaven ticking requirements.

See [`feeder_client/README.md`](feeder_client/README.md) for client-specific instructions.

---

## License

See [LICENSE](LICENSE) for details.
