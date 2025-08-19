"""
listener.py
Listener class for MarketFeeder.
"""
from collections import deque
import pyarrow as pa
from deephaven.table_listener import listen, TableListener, TableUpdate
from typing import Optional
from datetime import datetime, timezone
from .schemas import SchemaSpec

class _SymListener(TableListener):
    """Listener for a single (provider, schema, symbol) table view."""
    def __init__(self, provider: str, data_schema: str, symbol: str, spec: SchemaSpec, view, emit_completed):
        self.provider = provider
        self.data_schema = data_schema
        self.symbol = symbol
        self.spec = spec
        self.view = view
        self.emit_completed = emit_completed
        self.curr: Optional[dict] = None
        self.buf: deque[dict] = deque(maxlen=256)
        self.handle = listen(view, self)

    def start(self):
        self.handle.start()

    def stop(self):
        try:
            self.handle.stop()
        except Exception:
            pass

    def on_update(self, update: TableUpdate, is_replay: bool):
        """
        Forwards all added, updated, and completed rows as separate PyArrow Tables in a batch dict to the callback and buffer.
        Batch structure: {'added': pa.Table, 'updated': pa.Table, 'completed': pa.Table, 'meta': dict}
        OHLCV: completed bars are detected when a new bar is added (previous bar is completed).
        Trades/Quotes: all adds are complete.
        """
        added_tbl = None
        updated_tbl = None
        completed_tbl = None
        if update.added():
            added_tbl = pa.table(update.added())
            if self.spec.bin_period_minutes:
                time_col_idx = added_tbl.schema.get_field_index(self.spec.time_col)
                completed_rows = []
                for i in range(added_tbl.num_rows):
                    ts = added_tbl.column(time_col_idx)[i].as_py()
                    row_dict = {name: added_tbl.column(j)[i].as_py() for j, name in enumerate(added_tbl.schema.names)}
                    if self.curr is not None:
                        prev_ts = self.curr[self.spec.time_col]
                        if ts > prev_ts:
                            completed_rows.append(self.curr)
                    self.curr = row_dict
                if completed_rows:
                    completed_tbl = pa.Table.from_pylist(completed_rows)
            else:
                completed_tbl = added_tbl
        if update.modified():
            updated_tbl = pa.table(update.modified())
        batch = {
            'added': added_tbl,
            'updated': updated_tbl,
            'completed': completed_tbl,
            'meta': {
                'provider': self.provider,
                'schema': self.data_schema,
                'symbol': self.symbol,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
        }
        self.buf.append(batch)
        self.emit_completed(batch)

    def on_error(self, e: Exception):
        print(f"[MarketFeeder] listener error ({self.provider}/{self.data_schema}): {e}")

