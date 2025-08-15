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
│   ├── ticking.py       # Ticking logic for Deephaven integration
│   └── connectors/
│       └── deephaven_connector.py  # Deephaven connector implementation
├── tests/               # Test suite
│   ├── __init__.py
│   └── connectors/
│       └── test_deephaven_connector.py
```

## Development

### Prerequisites
- Docker and Docker Compose
- VSCode (recommended for remote development)
- **Linux host or container required for Deephaven Ticking support**

> **Note:** The DeepFeeder Client must run on Linux due to Deephaven Ticking dependencies. Running on Windows or macOS is not supported for this feature.

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

## Notes
- The canonical table is not present in this version.
- All code and features are designed for Linux environments only.

## License
See [LICENSE](../LICENSE) for details.
