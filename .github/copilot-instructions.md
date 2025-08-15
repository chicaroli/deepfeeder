# GitHub Copilot Instructions

## Project Overview

This repository contains a data feeder system and a client for integration with Deephaven. 
The main codebase is in Python, with a focus on Linux compatibility due to dependencies on Deephaven's ticking features.

## Coding Guidelines

- Use Python 3.10+ syntax and features.
- Follow PEP8 style for all Python code.
- Use type hints where possible.
- Organize code into logical modules: `src/` for implementation, `tests/` for unit tests.
- Use `uv` for dependency management (see `pyproject.toml` and `uv.lock`).

## Deephaven Server on Docker Image
- The Deephaven server is available as a Docker image defined in folder data, where it countains all the necessary files to run the server.
- The server can be started using the provided `docker-compose.yml` file in the root directory.
- The image is built to make the deephaven server run in app mode, and the code are available `in data/app.p`. It also maps the folder data to the `/data` directory in the container.    
- Ensure the server is running before testing the client code.

## Deephaven Client
- The client code is located in `feeder_client/src/`.
- The client interacts with the Deephaven server and should handle data ingestion and processing.
- Use the Deephaven Python API for all interactions with the server.
- Some features of the client uses a deephaven-ticking feature, which requires the client to run on Linux.

## Libraries and Tools

- Use standard Python libraries unless a third-party package is specified in `pyproject.toml`.
- For Deephaven integration, use the official Deephaven Python API.
- Use `pytest` for all tests in the `tests/` directory.

## Docker and Environment

- All code must run on Linux containers.
- The client Dockerfile is located in `feeder_client/`.
- Do not use Windows-specific features or paths.
- Ensure all dependencies are installed via `uv sync` in the container.

## Special Instructions

- Do not generate code that requires a GUI or Windows-only features.
- Always add docstrings to public functions and classes.
- When generating README or documentation, note that the client must run on Linux due to Deephaven's ticking requirements.

## File Structure

- `data/`: Deephaven server Docker image and related files.
- `feeder_client/src/`: Main client source code.
- `feeder_client/tests/`: Unit tests.
- `feeder_client/pyproject.toml`: Project dependencies.
- `feeder_client/uv.lock`: Locked dependencies.
- `feeder_client/Dockerfile`: Container build instructions.

## Example Prompts

- "Add a new connector for a data provider in `src/connectors/`."
- "Write a test for `deephaven_connector.py` using pytest."
- "Update the Dockerfile to install a new Python package."

---

This file expects Copilot to follow these instructions when generating code, comments, or documentation for this project.

