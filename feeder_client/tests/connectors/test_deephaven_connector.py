# filepath: feeder_client/tests/connectors/test_deephaven_connector.py

import os
import pytest
from connectors.deephaven_connector import DeephavenConnector

def test_connector_env(monkeypatch):
    monkeypatch.setenv("DEEPHAVEN_HOST", "testhost")
    monkeypatch.setenv("DEEPHAVEN_PORT", "12345")
    connector = DeephavenConnector()
    assert connector.host == "testhost"
    assert connector.port == 12345

def test_connector_args():
    connector = DeephavenConnector(host="customhost", port=54321)
    assert connector.host == "customhost"
    assert connector.port == 54321

def test_session_not_established():
    connector = DeephavenConnector()
    with pytest.raises(RuntimeError):
        connector.open_table("dummy_table")

def test_defaults(monkeypatch):
    monkeypatch.delenv("DEEPHAVEN_HOST", raising=False)
    monkeypatch.delenv("DEEPHAVEN_PORT", raising=False)
    connector = DeephavenConnector()
    assert connector.host == "localhost"
    assert connector.port == 10000

def test_context_manager(monkeypatch):
    class DummySession:
        def close(self): pass
        def open_table(self, name): return f"table:{name}"

    monkeypatch.setattr("pydeephaven.Session", lambda host, port: DummySession())
    with DeephavenConnector(host="foo", port=123) as conn:
        assert conn.session is not None
        table = conn.open_table("bar")
        assert table == "table:bar"
    assert conn.session is None

def test_deephaven_session_live():
    connector = DeephavenConnector()
    print(f"host: {connector.host}, port: {connector.port}")
    connector.establish_session()
    assert connector.session is not None
    # Optionally, check if the session is live by requesting tables list
    tables = connector.session.tables
    assert isinstance(tables, list)
