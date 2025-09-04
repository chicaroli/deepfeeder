"""Provider journal adapters registry and discovery.

Providers can supply a module ``feeders.<name>.journal_adapter`` exposing:

- name: str
- base_dir: str (dataset root directory, e.g., /data/persistence/hot/<stream>)
- register_tap(fn): callable to subscribe to writer writes
- to_record(args) -> dict: build typed record including keys: ts (datetime), dt (YYYY-MM-DD), symbol (lowercase)
- to_writer_args(record) -> tuple: reconstruct writer args for write_row_direct
- get_writer() -> writer mirror with write_row_direct
- build_seen(symbol: str, t0: datetime, t1: datetime) -> set: dedupe keys from live table
- to_arrow_table(batch: list[dict]) -> pyarrow.Table: convert batch records to a typed table
 - make_key(record: dict) -> Any: produce a hashable dedupe key for a record

Discovery attempts to import all ``feeders.*.journal_adapter`` modules dynamically.
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Dict, List


class AdapterProtocol:
    name: str
    base_dir: str
    def register_tap(self, fn): ...
    def to_record(self, args: tuple) -> Dict[str, Any]: ...
    def to_writer_args(self, record: Dict[str, Any]) -> tuple: ...
    def get_writer(self): ...
    def build_seen(self, symbol: str, t0, t1): ...
    def to_arrow_table(self, batch: List[Dict[str, Any]]): ...
    def make_key(self, record: Dict[str, Any]): ...


def _is_valid_adapter(mod: Any) -> bool:
    req = [
        hasattr(mod, 'name'), hasattr(mod, 'base_dir'),
        hasattr(mod, 'register_tap'), hasattr(mod, 'to_record'),
        hasattr(mod, 'to_writer_args'), hasattr(mod, 'get_writer'),
        hasattr(mod, 'build_seen'), hasattr(mod, 'to_arrow_table'), hasattr(mod, 'make_key'),
    ]
    return all(req)


def discover_adapters() -> List[AdapterProtocol]:
    adapters: List[AdapterProtocol] = []
    try:
        import _feeders  # type: ignore
    except Exception:
        return adapters
    try:
        pkg_path = _feeders.__path__  # type: ignore
        for finder, name, ispkg in pkgutil.iter_modules(pkg_path):  # type: ignore
            if not ispkg:
                continue
            mod_name = f"_feeders.{name}.journal_adapter"
            try:
                mod = importlib.import_module(mod_name)
                if _is_valid_adapter(mod):
                    adapters.append(mod)  # type: ignore
            except Exception:
                # ignore providers without adapter
                pass
    except Exception:
        pass
    return adapters
