# DeepFeeder Client

DeepFeeder Client is a real-time market data feeder framework built on Deephaven. This client project provides the code and configuration for connecting to market data sources, processing data, and interacting with Deephaven tables.

## Features
- Real-time data ingestion from multiple providers (e.g., Binance, TradingView)
- Integration with Deephaven via `pydeephaven` and `pydeephaven-ticking`
- Modular architecture for adding new data sources
- Test suite using `pytest`
- Linting and type checking with `ruff` and `mypy`

## Project Structure
```
feeder_client/
├── Dockerfile           # Container setup for development and deployment
├── pyproject.toml       # Project metadata and dependencies
├── uv.lock              # Locked dependencies for reproducible builds
├── src/                 # Source code for the client
│   └── feeder_client/   # Main package
│       ├── client.py
│       ├── models.py
│       ├── protocol.py
│       ├── stream.py
│       └── trading_bot.py
├── tests/               # Test suite
│   ├── __init__.py
│   └── connectors/
│       └── test_deephaven_connector.py
```

## Installation

Install via [uv](https://github.com/astral-sh/uv) or pip:

```sh
uv pip install feeder_client
# or for development
uv pip install -e feeder_client
```

- Python 3.10+ required
- A running Deephaven server is needed for full functionality.

## Using as a Library

You can use DeepFeeder Client in your own Python projects after installation:

```python
from feeder_client import DeepFeederClient

client = DeepFeederClient("ws://localhost:8083/v1")
# ... use client.subscribe(...) as shown below ...
```

## Quick Start

High-level Python client for connecting to the DeepFeeder fanout server (FastAPI + WebSockets) and receiving unified market data envelopes.

### Requirements
- Python 3.10+
- websocket-client (installed via pyproject/uv)
- For Deephaven ticking features, run the client on Linux if you need local DH features (not required for consuming WS).

### Example

```python
from feeder_client import DeepFeederClient

client = DeepFeederClient("ws://localhost:8083/v1")


# Rows callback: gets only snapshot/replay/live with rows
def on_rows(phase, part, provider, schema, symbol, rows, meta):
    print(f"{phase}/{part} rows={len(rows)} seq={meta.get('seq')}")
    if rows:
        print("  first row:", rows[0])


# Envelope callback (optional): gets every envelope including connected / snapshot_boundary / error
# def on_event(env: dict):
#     print("env:", env)

sub = client.subscribe(
    "tradingview", "ohlcv_1m", "INDV2025",
    fields="Timestamp,Open,High,Low,Close,Volume,Symbol,Exchange",
    only_completed=True,  # set False if you want open-bar added/updated
    on_new_data=on_rows,
)

# ... run your app ...

# Stop subscription & cleanup all
sub.stop()
client.close()
```

## Versioning and API Stability

This package follows semantic versioning. The public API is defined by the classes and functions exported in `feeder_client/__init__.py`. Breaking changes will only occur in major version updates.

## Notes
- Envelopes follow: snapshot -> snapshot_boundary -> replay -> live.
- Use only_completed=False to receive open-bar activity (added/updated), especially useful for delayed feeds.
- To resume without resending snapshot, pass since_ns set to the last snapshot_boundary watermark_ns you observed.

## Development

### Prerequisites
- Docker and Docker Compose
- VSCode (recommended for remote development)

### Getting Started
1. **Build and start the container:**
   ```sh
   docker compose up --build
   ```
2. **Access the container:**
   - Use VSCode Remote - Containers, or
   - Run `docker exec -it feeder-client bash`
3. **Install dependencies:**
   - Dependencies are installed automatically if you build the image with `pyproject.toml` and `uv.lock` copied.
   - For live development (with volume mapping), run inside the container:
     ```sh
     uv sync
     ```

### Running Tests

- Run tests inside the container using:
  ```sh
  pytest
  ```

## Troubleshooting
- Ensure the server is up and reachable at the port you configured (default 8083 in docker-compose).
- WS URL base should be the API prefix, e.g., `ws://localhost:8083/v1`.
- If a connection closes early, check server event logs (WS_* codes) in the Deephaven UI tables.

## License
See [LICENSE](../LICENSE) for details.
