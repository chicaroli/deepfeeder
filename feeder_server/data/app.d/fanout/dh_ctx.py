# Capture the Deephaven ExecutionContext on the main DH thread at import time,
# and expose a helper to run code under that context + a shared update-graph lock.
from __future__ import annotations
from contextlib import contextmanager

from deephaven import execution_context as ec
from deephaven import update_graph as ug

# This runs on import (in DH App Mode main thread) and grabs the active context.
_DH_CTX = ec.get_exec_ctx()

@contextmanager
def use_dh_ctx():
    """
    Enter the Deephaven ExecutionContext for the current thread.
    You can nest this with `dh_shared_lock(table)` when you have a specific table.
    """
    # ExecutionContext is a context manager; it registers QueryScope etc. on this thread.
    with _DH_CTX:
        yield

@contextmanager
def dh_shared_lock(table):
    """
    Acquire a shared lock on the table's update graph while doing read-side ops.
    """
    with ug.shared_lock(table.update_graph):
        yield
