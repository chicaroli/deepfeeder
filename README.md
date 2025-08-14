# DeepFeeder

**DeepFeeder** is a real-time market data feeder framework built on [Deephaven](https://deephaven.io/).  
It provides **WebSocket-based ingestion**, **live table updates**, **UI controls**, and **configuration persistence** for multiple data providers (starting with Binance).  

The system is designed for **low-latency streaming**, **live analytics**, and **integration into trading infrastructure**.

---

## Features

- **Modular feeder architecture** via `BaseFeeder` for adding new data providers.
- **Binance real-time trades** WebSocket ingestion.
- **Canonical trades bus** for provider-agnostic downstream processing.
- **Provider-specific schemas** (Binance detailed trades, aggregated OHLCV).
- **Live Deephaven tables** for trades, OHLCV, and feeder status.
- **Persistent feeder configs** stored in JSON, mirrored to live tables.
- **UI Dashboard** for starting, stopping, and managing feeders in real time.
- **Autostart mode** for predefined configs on server boot.

---

## Requirements

From [`requirements.txt`](requirements.txt):

```
websocket-client>=1.8.0
pandas>=2.2.2
pyarrow>=16.1.0
pydeephaven>=0.32.0
```

You will also need **Deephaven Server** (Docker recommended).

---

## Project Structure

```
app.d/
  app.py                     # Deephaven application bindings
  core/
    base.py                  # BaseFeeder abstract class
    bus.py                   # DynamicTableWriters for configs, trades, status
    registry.py              # FeederRegistry for config persistence & runtime control
    utils.py                 # Utility functions (symbol normalization)
  providers/
    binance_schema.py        # Detailed Binance trades schema + OHLCV aggregation
    binance_feeder.py        # Binance WebSocket feeder implementation
  ui/
    dashboard.py              # Deephaven UI components for managing feeders
feeders.json                 # Persistent configs (autostart, symbols, etc.)
requirements.txt
docker-compose.yml
```

---

## Quick Start

1. **Create a minimal config file** at `/data/app.d/feeders.json`:

   ```json
   [
     {
       "provider": "binance",
       "name": "btc_only",
       "symbols": ["btcusdt"],
       "autostart": true
     }
   ]
   ```

2. **Start Deephaven with DeepFeeder**:

   ```bash
   docker compose up -d
   ```

3. **Open the dashboard** in your browser:  
   [http://localhost:10000](http://localhost:10000) → **Feeder Dashboard**.

4. You should now see **live BTC/USDT trades** updating in:
   - **Trades (canonical)**  
   - **Binance Trades (detailed)**  
   - **Binance OHLCV 1m**

---

## How It Works

1. **Feeder Lifecycle**
   - Feeders are instantiated via the `FeederRegistry`.
   - Each feeder runs a background thread connecting to the provider's WebSocket API.
   - Incoming messages are parsed, validated, and written into **DynamicTableWriter** streams.

2. **Data Flow**
   - **Provider-specific table**: Rich schema (e.g., Binance trade details).
   - **Canonical trades bus**: Uniform schema for cross-provider processing.
   - **Aggregated OHLCV**: Derived inside Deephaven from detailed trades.

3. **UI & Control**
   - The dashboard (`dashboard.py`) lets you:
     - View live trades and OHLCV.
     - Start/stop feeders.
     - Manage configs (create, update, delete).
     - Trigger autostart feeders.

4. **Persistence**
   - Configs are stored in `/data/app.d/feeders.json`.
   - Live configs table mirrors file state for real-time visibility.

---

## Adding a New Provider

1. Create a new feeder class in `providers/` inheriting from `BaseFeeder`.
2. Implement:
   - `start()` — to open connections and begin streaming.
   - `stop()` — to cleanly close connections.
   - `is_alive()` — to report health.
3. Register it in `FeederRegistry._make()`.

---

## Example `feeders.json`

```json
[
  {
    "provider": "binance",
    "name": "btc_only",
    "symbols": ["btcusdt"],
    "autostart": true
  },
  {
    "provider": "binance",
    "name": "eth_only",
    "symbols": ["ethusdt"],
    "autostart": false
  }
]
```

---

## Roadmap

- Additional providers (TradingView, Databento, B3).
- Enhanced fault tolerance & reconnection logic.
- Historical data sync & persistence to TimescaleDB.
- Subscription filtering to reduce bandwidth for multi-symbol tables.

---

## License

This project is proprietary unless otherwise specified.
