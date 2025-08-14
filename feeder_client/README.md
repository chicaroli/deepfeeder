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
│   ├── dh_client.py     # Deephaven client logic
│   └── test_ticking.py  # Example/test code for ticking
├── tests/               # Test suite
│   ├── __init__.py
│   └── test_tv_feed.py
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
```sh
pytest
```

### Linting and Type Checking
```sh
ruff check src/
mypy src/
```

## Configuration
- Project dependencies and dev tools are managed in `pyproject.toml`.
- Deephaven connection and provider configuration are handled in `src/dh_client.py` and related modules.

## Contributing
Pull requests and issues are welcome! Please ensure code is tested and linted before submitting.

## License
This project is licensed under the MIT License.
