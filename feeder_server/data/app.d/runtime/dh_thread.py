"""Managed threading wrapper that emits standardized heartbeats."""
from __future__ import annotations
import threading, traceback
from typing import Callable, Any, Optional
from .heartbeat import Heartbeater

from deephaven.execution_context import get_exec_ctx

class DHThread(threading.Thread):
    """Managed thread emitting lifecycle heartbeats.

    Target callable signature MUST accept (stop_event, *args, **kwargs).
    """
    def __init__(self, service: str, name: str, role: str, target: Callable[..., Any], *args, daemon: bool = True, exec_ctx: Optional[Any] = None, **kwargs):
        super().__init__(target=None, daemon=daemon, name=f"{service}:{name}:{role or 'main'}")
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._stop_event = threading.Event()
        self._hb = Heartbeater(service, name, role)
        self._exec_ctx = exec_ctx

    def run(self):  # noqa: D401 - standard Thread.run override
        self._hb.beat("starting")
        try:
            if self._exec_ctx is not None:
                # ensure the Deephaven ExecutionContext / QueryScope is active for this thread
                with self._exec_ctx:
                    self._target(self._stop_event, *self._args, **self._kwargs)
            else:
                self._target(self._stop_event, *self._args, **self._kwargs)
            self._hb.beat("stopped")
        except Exception as e:  # noqa: BLE001
            traceback.print_exc()
            self._hb.beat("error", last_error=str(e))

    def stop(self):
        self._stop_event.set()

    @property
    def stop_event(self):
        return self._stop_event


def spawn(service: str, name: str, role: str, target: Callable[..., Any], *args, **kwargs) -> DHThread:
    """Create and start a DHThread."""
    # obtain the system execution context from the script session
    try:
        ctx = get_exec_ctx()
    except Exception:
        ctx = None
    t = DHThread(service, name, role, target, *args, exec_ctx=ctx, **kwargs)
    t.start()
    return t
