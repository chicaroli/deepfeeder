# DeepFeeder

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)

**DeepFeeder** is a real-time market data feeder framework for MFT strategies development built on [Deephaven](https://deephaven.io/).
It provides WebSocket-based ingestion, live table updates, UI controls, gap-filled bar generation, and
configuration persistence for multiple data providers (Binance, TradingView – extensible).

## ⚠️ Disclaimer

This project includes and runs a Deephaven server via Docker. No separate Deephaven installation is required.
You must comply with Deephaven’s license and terms of service. This project is not affiliated with or endorsed by Deephaven.

The TradingView feeder is for research and educational purposes only. Use at your own risk. Not intended for production or commercial use. You are responsible for complying with TradingView's terms of service and any applicable laws.
Users are responsible for ensuring compliance with all third-party service terms and local laws when deploying or extending this project.

**Status:** This is an ongoing project and is not yet complete. Features, APIs, and stability may change at any time.

**Security:** Never commit secrets, API keys, or credentials. Review code and configs before deployment.

**Support:**  This project is provided as-is, with no guarantees or support. Contributions are welcome via pull requests.

**Platform:** The client requires Linux due to Deephaven's ticking features. Use Docker for best compatibility.

---

## Current Architecture Snapshot

| Layer | Responsibility | Key Modules |
|-------|----------------|-------------|
| Ingest / Feeders | WebSocket connect, raw trade / quote writes | `feeders/binance/*`, `feeders/tradingview/*` |
| Aggregation | Sparse OHLCV (only minutes with trades) | provider schema functions (`binance_ohlcv_1m`, etc.) |
| Gap Fill Spine | Minimal recent-period bins (current + previous) | `feeders/bins.py` (`bins_recent`) |
| Filled Bars | Sparse bars joined to bins (placeholders w/ IsEmpty) | `*_ohlcv_*_filled` functions |
| Fanout | Subscription + bar completion logic | `fanout/core.py`, `fanout/listener.py`, `fanout/schemas.py` |
| Public API | Table getters, manager singleton | `deepfeeder/__init__.py` |
| UI | Dashboard helpers | `ui/dashboard.py` |
| Observability | Event log (append-only), derived views | `runtime/eventlog*.py` |

## Persistence Layer

The persistence layer is responsible for reliable storage, journaling, compaction, replay, and schema management of all provider data. It ensures that market data is written efficiently, can be recovered or replayed, and supports schema evolution for different providers and formats.

**Main components:**

- `persistence/`: Core journal logic, adapters, configuration
- `persistence/journal.py`: Handles batching, flush, compaction, replay, and event logging
- `persistence/journal_adapter.py`: Implements provider-specific partitioning, Arrow table conversion, and schema evolution

**Features:**

- Partitioned Parquet writing for efficient storage and retrieval
- Provider-specific partitioning and schema logic (see TradingView/Binance adapters)
- Background compaction and replay logic for data recovery and analysis
- Event logging for diagnostics and error tracking

**Usage:**

The persistence layer operates automatically as part of the feeder server. All diagnostics and errors are recorded in the event log system. For details on customizing persistence, see the code and comments in the `persistence/` directory.

Review provider API terms and data retention policies before deploying in production environments.

For questions or contributions, open an issue or pull request.

## OHLCV Tables

Both sparse and filled OHLCV tables are available per provider and interval. Use the sparse tables for display and the filled tables for bar completion and streaming workflows. See API for table getters.

### Example: Subscribing to Fanout

```python
import deepfeeder as df

# Subscribe to completed bars (fanout)
mf = df.fanout.market_feeder
handle = mf.subscribe("binance", "ohlcv_1m", "BTCUSDT", callback, only_completed=True)
```

---

## Bins Utility (Lightweight)

We now only maintain a *minimal* rolling window of recent bins for bar completion. Instead of
scaffolding the entire UTC day, we expose the current aligned bar and a fixed number of
previous bars (default 2 total: current + previous). This dramatically reduces the row
churn and memory footprint while still supporting forced completion of the last bar.

```python
from feeders.bins import bins_recent
recent_1m = bins_recent(1, 2)   # current + previous minute
recent_5m = bins_recent(5, 2)   # current + previous 5-minute window
```

Need a longer short-term window (e.g. last hour of 5m bins)?

```python
hour_5m = bins_recent(5, 12)  # if ever required
```

---

## Placeholder Handling & Bar Completion

- Filled tables produce `IsEmpty=True` rows for periods with no trades.
- Listener logic:
  - Detects a timestamp rollover (real or placeholder) → emits the previous bar as completed.
  - Suppresses placeholder rows (no `added` emission) while still advancing internal clock.
  - Real trades later in the same period convert placeholder to a normal bar (updates / modifies).

No first-bar emission until a second period (placeholder or real) appears—by design (open may not be final until rollover).

---

## Multi-Interval Architecture

Both 1m and 5m share unified schema column sets per provider (`BINANCE_OHLCV_SCHEMA_COLS`, `TV_OHLCV_SCHEMA_COLS`). Adding a new period:

1. Aggregate sparse bars (`lowerBin(Timestamp, N * MINUTE)`).
2. Gap-fill (if needed) by joining against a suitable `bins_recent(N, bars_back)` window. For
  current design we only require the most recent two bins to finalize bars.
3. Register canonical schema name `ohlcv_Nm` → *filled* table (no parallel sparse schema in fanout).
4. Expose sparse & filled getters only if needed for dashboards.

---

## Public API (Simplified)

```python
import deepfeeder as df

# Manage feeders
resp = df.feeder_manager.start("binance", "default", ["btcusdt","ethusdt"])
status_dict = df.feeder_manager.status()

# Table access (Deephaven live tables)
trades = df.get_binance_trades_table()
bar_1m_sparse = df.get_binance_ohlcv_1m_table()
bar_1m_filled = df.get_binance_ohlcv_1m_filled_table()

# Unified bins
# Recent bins (current + previous)
bins = df.bins_recent(1, 2)

# Threads / Services control-plane (new)
threads_live = df.get_threads_table()  # one row per (service,name,role) latest heartbeat

# Access services container (lazy singletons)
fm = df.feeder_manager  # FeederManager
fm.start("binance", "scalp", ["btcusdt"])  # start a feeder

# Inspect threads table inside Deephaven UI for lifecycle / heartbeat diagnostics.

# Event log (append-only): access and derive common diagnostic views
eventlog = df.get_eventlog_table()
recent_errors = eventlog.where("level == 'ERROR' && ts >= now()-MINUTE*10")
latest_error_per_instance = eventlog.where("level == 'ERROR'").last_by(["service","name"])
```

---

## Project Structure

```text
feeder_server/
  data/app.d/
    deepfeeder/           # Public API
    feeders/
      binance/            # Binance provider
      tradingview/        # TradingView provider
      bins.py             # bins_recent utility
      common/             # Shared logic
    fanout/               # Subscription + listener logic
    ingest/               # Feeder manager and tables
    persistence/          # Journal, config, adapters
    runtime/              # Threads bus, eventlog, services
    ui/                   # Dashboard components
    __init__.py           # App entry
  app.py, feeder.app      # Deephaven app entrypoints
feeder_client/
  src/
    connectors/           # Deephaven client connector
    bots/                 # Example trading bot
    ticking.py            # Ticking logic
  tests/                  # Client unit tests
  Dockerfile, pyproject.toml, uv.lock
```

---

## Installation & Run

Start Deephaven + feeder server:

```bash
docker compose up --build
```

Then connect via notebooks or the client.

---

## Extending Providers

1. Implement trades (or quotes) ingestion via a DynamicTableWriter.
2. Add a sparse OHLCV aggregation (if needed).
3. Use `bins_recent(period, 2)` (or a slightly larger window if required) to build the
  minimal filled version for bar completion.
4. Register canonical filled schema in `fanout/schemas.py`.
5. Add getters in `deepfeeder/__init__.py` (sparse & filled as desired).
6. Emit lifecycle / network events via `runtime.eventlog.emit_event()` (e.g. START, WS_OPEN, WS_ERR).

### Event Log Conventions

| Field | Purpose | Example |
|-------|---------|---------|
| service | Logical subsystem | `feeder` |
| name | Instance identifier | `binance:scalp` |
| role | Thread / role | `ws_loop` |
| level | Severity | `INFO`, `WARN`, `ERROR`, `DEBUG` |
| code | Machine tag | `START`, `WS_ERR`, `RESTART` |
| message | Human readable detail | `WebSocket open` |
| meta | JSON payload (compact) | `{`"symbols"`:["btcusdt"]}` |
| corr_id | Correlate multi-step flows | backfill run id |
| parent_corr_id | Parent correlation id | original trigger id |

Recommended minimal lifecycle events: `START`, `STOP`, `WS_CONNECT`, `WS_OPEN`, `WS_CLOSED`, `WS_ERR` plus domain-specific codes (e.g. `BACKFILL_BEGIN`).

---

## License

See [LICENSE](LICENSE) for details.
