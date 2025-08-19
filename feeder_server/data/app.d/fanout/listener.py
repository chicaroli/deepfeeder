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
import os

class _SymListener(TableListener):
    """Listener for a single (provider, schema, symbol) table view."""
    def __init__(self, provider: str, data_schema: str, symbol: str, spec: SchemaSpec, view, emit_completed,
                 debug: bool = False):
        self.provider = provider
        self.data_schema = data_schema
        self.symbol = symbol
        self.spec = spec
        self.view = view
        self.emit_completed = emit_completed
        self.curr: Optional[dict] = None
        self.buf: deque[dict] = deque(maxlen=256)
        self.debug = debug
        self._dbg(f"init bin_period={spec.bin_period_minutes} time_col={spec.time_col}")
        self.handle = listen(view, self)

    def start(self):
        # Use defensive start: some Deephaven versions auto-start the listener returned by listen()
        try:
            self._dbg("start")
            self.handle.start()
        except RuntimeError as e:  # already started
            if "already started" in str(e).lower():
                self._dbg("start ignored (already running)")
            else:
                raise

    def stop(self):
        try:
            self._dbg("stop")
            self.handle.stop()
        except Exception as e:
            self._dbg(f"stop ignore error: {e}")
            pass

    def _dbg(self, msg: str):
        if self.debug:
            print(f"[SymListener {self.provider}/{self.data_schema}/{self.symbol}] {msg}")

    def on_update(self, update: TableUpdate, is_replay: bool):
        """
        Forwards all added, updated, and completed rows as separate PyArrow Tables in a batch dict to the callback and buffer.
        Completion logic for bar schemas (bin_period_minutes set):
          - Keep self.curr holding the LATEST state of the current (still open) bar (updated on modifications).
          - When a NEW bar (Timestamp advances) is added, emit the previous self.curr as a completed bar.
        """
        added_tbl = None
        updated_tbl = None
        completed_tbl = None
        try:
            # 1. Handle modifications first so self.curr reflects final state before potential rollover
            if update.modified():
                mod_raw = update.modified()
                updated_tbl = pa.table(mod_raw)
                if self.spec.bin_period_minutes and updated_tbl.num_rows > 0:
                    # For aggregated bars, expect a single modified row (current bar update)
                    time_idx = updated_tbl.schema.get_field_index(self.spec.time_col)
                    # Use the LAST modified row (if multiple) as latest snapshot
                    i = updated_tbl.num_rows - 1
                    ts = updated_tbl.column(time_idx)[i].as_py()
                    row_dict = {name: updated_tbl.column(j)[i].as_py() for j, name in enumerate(updated_tbl.schema.names)}
                    if self.curr is None or ts >= self.curr[self.spec.time_col]:
                        self.curr = row_dict
                        self._dbg(f"curr bar updated ts={ts}")
                elif updated_tbl.num_rows > 0:
                    self._dbg(f"modified rows={updated_tbl.num_rows}")

            # 2. Handle additions (potential bar rollovers or new trades/quotes)
            if update.added():
                raw_added = update.added()
                added_tbl = pa.table(raw_added)
                if added_tbl.num_rows == 0:
                    # Drop empty add batches to reduce noise
                    added_tbl = None
                else:
                    self._dbg(f"added rows={added_tbl.num_rows}")
                    if self.spec.bin_period_minutes:  # bar-based schema
                        time_col_idx = added_tbl.schema.get_field_index(self.spec.time_col)
                        completed_rows = []
                        for i in range(added_tbl.num_rows):
                            ts = added_tbl.column(time_col_idx)[i].as_py()
                            row_dict = {name: added_tbl.column(j)[i].as_py() for j, name in enumerate(added_tbl.schema.names)}
                            # If we already have a current bar and timestamp advances -> previous bar completes
                            if self.curr is not None:
                                prev_ts = self.curr[self.spec.time_col]
                                if ts > prev_ts:
                                    completed_rows.append(self.curr)
                                    self._dbg(f"bar rollover prev_ts={prev_ts} new_ts={ts}")
                            else:
                                self._dbg(f"first bar seen ts={ts}")
                            # Set new current bar candidate (initial snapshot; later modified updates refine it)
                            self.curr = row_dict
                        if completed_rows:
                            completed_tbl = pa.Table.from_pylist(completed_rows)
                            self._dbg(f"completed bars emitted={completed_tbl.num_rows}")
                        else:
                            self._dbg("no completed bars this add batch")
                    else:  # non-bar schema: each add is a completed event
                        completed_tbl = added_tbl
        except Exception as e:
            self._dbg(f"error in on_update: {e}")
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
        self._dbg("emit batch " +
                  f"added={(added_tbl.num_rows if added_tbl else 0)} " +
                  f"updated={(updated_tbl.num_rows if updated_tbl else 0)} " +
                  f"completed={(completed_tbl.num_rows if completed_tbl else 0)}")
        self.buf.append(batch)
        self.emit_completed(batch)

    def on_error(self, e: Exception):
        print(f"[MarketFeeder] listener error ({self.provider}/{self.data_schema}): {e}")

    def snapshot(self) -> dict:
        """Return a lightweight dict describing current listener state (for diagnostics)."""
        last_added = last_updated = last_completed = 0
        last_timestamp = None
        if self.buf:
            b = self.buf[-1]
            last_timestamp = b.get('meta', {}).get('timestamp')
            at = b.get('added'); ut = b.get('updated'); ct = b.get('completed')
            if at is not None:
                try: last_added = at.num_rows
                except Exception: pass
            if ut is not None:
                try: last_updated = ut.num_rows
                except Exception: pass
            if ct is not None:
                try: last_completed = ct.num_rows
                except Exception: pass
        return {
            'provider': self.provider,
            'schema': self.data_schema,
            'symbol': self.symbol,
            'buffer_len': len(self.buf),
            'last_timestamp': last_timestamp,
            'last_added_rows': last_added,
            'last_updated_rows': last_updated,
            'last_completed_rows': last_completed,
        }
