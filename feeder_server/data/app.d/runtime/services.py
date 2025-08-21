"""Thread-safe service container for runtime-managed singletons."""
from __future__ import annotations
from threading import Lock
from typing import Callable, Any, Dict

class Services:
    """Minimal service locator / container."""
    def __init__(self) -> None:
        self._lock = Lock()
        self._factories: Dict[str, Callable[[], Any]] = {}
        self._instances: Dict[str, Any] = {}

    def register(self, name: str, factory: Callable[[], Any]) -> None:
        with self._lock:
            self._factories[name] = factory

    def get(self, name: str):
        with self._lock:
            if name in self._instances:
                return self._instances[name]
            if name not in self._factories:
                raise KeyError(f"Service '{name}' not registered")
            inst = self._factories[name]()
            self._instances[name] = inst
            return inst

    def has(self, name: str) -> bool:
        with self._lock:
            return name in self._instances or name in self._factories

    def shutdown(self) -> None:
        with self._lock:
            for obj in list(self._instances.values()):
                stop = getattr(obj, "stop_all", None) or getattr(obj, "stop", None)
                if callable(stop):
                    try:
                        stop()
                    except Exception:  # noqa: BLE001
                        pass
