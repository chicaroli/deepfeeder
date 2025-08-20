# DeepFeeder

**DeepFeeder** is a real-time market data feeder framework built on [Deephaven](https://deephaven.io/).
It provides WebSocket-based ingestion, live table updates, UI controls, gap-filled bar generation, and
configuration persistence for multiple data providers (Binance, TradingView – extensible).

---
## Current Architecture Snapshot

| Layer | Responsibility | Key Modules |
|-------|----------------|-------------|
| Ingest / Feeders | WebSocket connect, raw trade / quote writes | `feeders/binance/*`, `feeders/tradingview/*` |
| Aggregation | Sparse OHLCV (only minutes with trades) | provider schema functions (`binance_ohlcv_1m`, etc.) |
| Gap Fill Spine | Unified per-day minute / multi-minute bins | `feeders/bins.py` (`bins_today`) |
| Filled Bars | Sparse bars joined to bins (placeholders w/ IsEmpty) | `*_ohlcv_*_filled` functions |
| Fanout | Subscription + bar completion logic | `fanout/core.py`, `fanout/listener.py`, `fanout/schemas.py` |
| Public API | Table getters, manager singleton | `deepfeeder/__init__.py` |
| UI | Dashboard helpers | `ui/dashboard.py` |

---
## Sparse vs Filled OHLCV Bars

We now maintain two forms of OHLCV tables per provider & interval:

- **Sparse**: Only minutes (or 5m windows) with at least one trade / quote aggregation. No empty rows.
- **Filled**: Full time grid. Every expected period appears. Empty (no-trade) periods have all price/volume fields null and `IsEmpty = True`.

### Why both?
- **Dashboards / Visual tables**: Sparse is cleaner (no rows of nulls). Use the sparse getters.
- **Streaming / Completion detection**: Fanout needs a trigger at the next period boundary even if no trades; the filled table provides a placeholder row. The listener suppresses placeholder-only batches (they are used only to emit completion for the prior bar).

### Fanout Subscription Semantics
The fanout *schema names* (`ohlcv_1m`, `ohlcv_5m`) now resolve to the **filled** tables internally. Clients see only completed bars (placeholders never emitted). No parallel “_filled” schema names are exposed via fanout to avoid confusion.

| Subscribe Schema | Underlying Table | Placeholders Emitted? | Completion Triggered? |
|------------------|------------------|------------------------|------------------------|
| `ohlcv_1m`       | filled 1m        | No (suppressed)        | Yes (placeholder or real next bar) |
| `ohlcv_5m`       | filled 5m        | No                     | Yes |
| `trades`         | trades (sparse)  | N/A                    | All adds complete |

### Choosing Tables in Notebooks / UI
Use getters from `deepfeeder`:
```python
import deepfeeder as df

# Sparse (good for display)
binance_1m_sparse = df.get_binance_ohlcv_1m_table()
tradingview_5m_sparse = df.get_tv_ohlcv_5m_table()

# Filled (full grid + IsEmpty)
binance_1m_filled = df.get_binance_ohlcv_1m_filled_table()
tradingview_5m_filled = df.get_tv_ohlcv_5m_filled_table()

# Subscribe (always filled under the hood)
mf = df.fanout.market_feeder
handle = mf.subscribe("binance", "ohlcv_1m", "BTCUSDT", callback, only_completed=True)
```

---
## Bins Utility
A single cached function builds per-day time bins:
```python
from feeders.bins import bins_today
bins_1m = bins_today(1)
bins_5m = bins_today(5)
```
Used to gap-fill all OHLCV intervals. Easy to extend to 10, 15, 30, 60 minutes:
```python
bins_15m = bins_today(15)
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
2. Gap-fill with `bins_today(N)`.
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
bins = df.bins_today(1)  # or 5
```

---
## Project Structure (Updated Simplified View)
```text
feeder_server/
  data/app.d/
    deepfeeder/                # Public API
    feeders/                   # Provider implementations & schemas
      bins.py                  # bins_today utility
      binance/
      tradingview/
    fanout/                    # Subscription + listener logic
    ui/                        # Dashboard components
    storage/notebooks/         # Examples & tests
feeder_client/                 # Client (Linux required for ticking)
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
3. Use `bins_today(period)` to build filled version.
4. Register canonical filled schema in `fanout/schemas.py`.
5. Add getters in `deepfeeder/__init__.py` (sparse & filled as desired).

---
## License
See [LICENSE](LICENSE) for details.
