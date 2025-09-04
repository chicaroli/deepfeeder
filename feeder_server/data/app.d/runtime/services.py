# runtime/services.py
from __future__ import annotations
import threading
from typing import Callable, Dict, Any, Optional, Iterable

Factory = Callable[[], Any]
CloserNameOrder = ("shutdown", "close", "stop", "flush")  # try in this order

class Services:
    def __init__(self) -> None:
        self._factories: Dict[str, Factory] = {}
        self._instances: Dict[str, Any] = {}
        self._construct_order: list[str] = []
        self._constructing: set[str] = set()
        self._lock = threading.RLock()
        self._shutdown_hooks: list[Callable[[], None]] = []

    def register(self, key: str, factory_or_instance: Factory | Any) -> None:
        with self._lock:
            if callable(factory_or_instance):
                self._factories[key] = factory_or_instance
            else:
                self._instances[key] = factory_or_instance
                self._construct_order.append(key)

    def get(self, key: str) -> Any:
        with self._lock:
            if key in self._instances:
                return self._instances[key]
            if key not in self._factories:
                raise KeyError(f"service '{key}' not registered")
            if key in self._constructing:
                raise RuntimeError(f"cyclic service construction for '{key}'")
            factory = self._factories[key]
            self._constructing.add(key)

        try:
            inst = factory()  # may recurse into get(); safe because we released the lock
        finally:
            with self._lock:
                self._constructing.discard(key)

        with self._lock:
            self._instances[key] = inst
            self._construct_order.append(key)
            return inst

    # Peek without constructing
    def try_get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._instances.get(key)

    # Optional external hooks to run during shutdown (e.g., orchestrator.stop_consumers)
    def on_shutdown(self, cb: Callable[[], None]) -> None:
        with self._lock:
            self._shutdown_hooks.append(cb)

    def shutdown(self, *, join: bool = False, timeout: float = 2.0) -> None:
        # run external hooks first (LIFO)
        for cb in reversed(self._shutdown_hooks):
            try:
                cb()
            except Exception:
                pass

        # close instances in reverse construction order
        for key in reversed(self._construct_order):
            obj = self._instances.get(key)
            if obj is None:
                continue
            # if it’s a thread-like service and join requested
            if join and hasattr(obj, "stop") and hasattr(obj, "join"):
                try:
                    obj.stop()          # type: ignore[attr-defined]
                    obj.join(timeout=timeout)  # type: ignore[attr-defined]
                    continue
                except Exception:
                    pass
            # otherwise try standard closer names
            for name in CloserNameOrder:
                fn = getattr(obj, name, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception:
                        pass
                    break
