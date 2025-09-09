# storage/eventstore_duckdb.py
from __future__ import annotations
from typing import List
import time, threading
import duckdb
import pyarrow as pa
from core.contracts import Envelope, EventStore, Tick
from runtime.eventlog import emit_event


# ---- Arrow codecs  ----------------------------------------------------------
def _ticks_to_arrow(ticks: List[Tick]) -> pa.Table:
    return pa.table({
        "provider": [t.provider for t in ticks],
        "stream":   [t.stream   for t in ticks],
        "symbol":   [t.symbol   for t in ticks],
        "ts_ns":    [t.ts_ns    for t in ticks],
        "seq":      [t.seq if t.seq is not None else -1 for t in ticks],
        "is_final": [t.is_final for t in ticks],
        "payload":  [t.payload  for t in ticks],
    })

def _arrow_to_ticks(tbl: pa.Table) -> List[Tick]:
    provider = tbl["provider"].to_pylist()
    stream   = tbl["stream"].to_pylist()
    symbol   = tbl["symbol"].to_pylist()
    ts_ns    = tbl["ts_ns"].to_pylist()
    seq      = tbl["seq"].to_pylist()
    is_final = tbl["is_final"].to_pylist()
    payload  = tbl["payload"].to_pylist()
    out: List[Tick] = []
    from core.contracts import Tick
    for i in range(tbl.num_rows):
        out.append(Tick(
            provider=provider[i], stream=stream[i], symbol=symbol[i],
            ts_ns=int(ts_ns[i]), seq=(None if seq[i] == -1 else int(seq[i])),
            payload=payload[i], is_final=bool(is_final[i]),
        ))
    return out

def _table_to_ipc_bytes(tbl: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    return sink.getvalue().to_pybytes()

def _ipc_bytes_to_table(data: bytes) -> pa.Table:
    buf = pa.py_buffer(data)
    with pa.ipc.open_stream(buf) as reader:
        return reader.read_all()


# ---- DuckDB EventStore (sequence-based IDs) ---------------------------------
class DuckDbEventStore(EventStore):
    def __init__(self, path: str, *,
                 prune_interval_s: float = 600,
                 prune_safety_batches: int = 100,
                 vacuum_every_n_prunes: int = 6):
        self._lock = threading.RLock()
        self.con = duckdb.connect(path)
        self._ensure_schema()

        # --- WAL retention knobs (env overrides) ---------------------------
        # Prune at most every PRUNE_INTERVAL_S seconds
        self._prune_interval_s = prune_interval_s
        # Keep a safety window of batches (don’t prune right up to the commit head)
        self.prune_safety_batches = prune_safety_batches
        # Run VACUUM every N prunes (CHECKPOINT runs every prune)
        self.vacuum_every_n_prunes = vacuum_every_n_prunes
        self._last_prune_ts: float = 0.0
        self._last_pruned_batch: int = 0
        self._prune_count: int = 0

    def _ensure_schema(self) -> None:
        # Tables
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS event_store(
            batch_id     BIGINT PRIMARY KEY,
            produced_at  TIMESTAMP,
            first_off    BIGINT,
            last_off     BIGINT,
            row_count    INTEGER,
            payload      BLOB
        );
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS acks(
            last_committed BIGINT
        );
        """)
        row = self.con.execute("SELECT COUNT(*) FROM acks").fetchone()
        if not row or row[0] == 0:
            self.con.execute("INSERT INTO acks VALUES (0)")
        self.con.execute("CREATE INDEX IF NOT EXISTS ix_event_store_batch_id ON event_store(batch_id);")
        # --- reseed sequence WITHOUT ALTER (drop + create) ---
        max_id = int(self.con.execute("SELECT COALESCE(MAX(batch_id), 0) FROM event_store").fetchone()[0])
        start = max_id + 1
        self.con.execute("BEGIN;")
        try:
            self.con.execute("DROP SEQUENCE IF EXISTS event_store_seq;")
            self.con.execute(f"CREATE SEQUENCE event_store_seq START {start};")
            self.con.execute("COMMIT;")
        except Exception:
            self.con.execute("ROLLBACK;")
            raise

    def append(self, env: Envelope) -> int:
        tbl = _ticks_to_arrow(env.rows)
        blob = _table_to_ipc_bytes(tbl)
        with self._lock:
            cur = self.con.execute(
                "INSERT INTO event_store "
                "(batch_id, produced_at, first_off, last_off, row_count, payload) "
                "VALUES (nextval('event_store_seq'), to_timestamp(?/1e9), ?, ?, ?, ?) "
                "RETURNING batch_id",
                [env.produced_at_ns, env.first_offset or -1, env.last_offset or -1, len(env.rows), blob],
            )
            batch_id = int(cur.fetchone()[0])
        return batch_id

    def next_from(self, next_batch_id: int, max_n: int) -> List[Envelope]:
        with self._lock:
            rows = self.con.execute(
                "SELECT batch_id, extract(epoch FROM produced_at)*1e9::BIGINT, first_off, last_off, payload "
                "FROM event_store WHERE batch_id >= ? ORDER BY batch_id LIMIT ?",
                [next_batch_id, max_n],
            ).fetchall()
        envs: List[Envelope] = []
        for b, ns, f, l, blob in rows:
            tbl = _ipc_bytes_to_table(blob)
            envs.append(Envelope(int(b), int(ns),
                                 None if int(f) < 0 else int(f),
                                 None if int(l) < 0 else int(l),
                                 _arrow_to_ticks(tbl)))
        return envs

    def last_committed(self) -> int:
        with self._lock:
            row = self.con.execute("SELECT last_committed FROM acks LIMIT 1").fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0])

    def mark_committed(self, batch_id: int) -> None:
        with self._lock:
            self.con.execute("UPDATE acks SET last_committed = ?", [int(batch_id)])
            self._maybe_prune_locked()

    def max_batch_id(self) -> int:
        with self._lock:
            row = self.con.execute("SELECT max(batch_id) FROM event_store").fetchone()
        if not row or row[0] is None:
            return 0
        return int(row[0])

    def prune_upto(self, batch_id: int) -> int:
        """
         Delete envelopes with batch_id <= specified cutoff (inclusive).
         Returns the number of rows deleted.
         """
        with self._lock:
            return self._prune_upto_locked(int(batch_id))

    # --------------------- internal retention helpers ----------------------
    def _maybe_prune_locked(self) -> None:
        """Prune WAL up to (last_committed - safety), at most every _prune_interval_s seconds."""
        if self._prune_interval_s <= 0:
            return  # disabled
        now = time.time()
        if (now - self._last_prune_ts) < self._prune_interval_s:
            return
        cutoff = int(self.last_committed()) - self.prune_safety_batches
        if cutoff <= 0 or cutoff <= self._last_pruned_batch:
            self._last_prune_ts = now
            return
        deleted = self._prune_upto_locked(cutoff)
        self._last_pruned_batch = cutoff
        self._last_prune_ts = now
        self._prune_count += 1
        # Periodic full compaction (CHECKPOINT runs inside _prune_upto_locked)
        if self._prune_count % self.vacuum_every_n_prunes == 0:
            try:
                self.con.execute("VACUUM")
                emit_event("feeder", "CORE", "event_store", "INFO", "VACUUM",
                           f"performed VACUUM after {self._prune_count} prunes")

            except Exception as e:
                emit_event("feeder", "CORE", "event_store", "ERROR", "VACUUM",
                           f"VACUUM failed: {e!r}")
                pass

    def _prune_upto_locked(self, cutoff_inclusive: int) -> int:
       """Internal: perform the DELETE + CHECKPOINT under the caller's lock."""
       # Count first (for logging/metrics)
       row = self.con.execute(
           "SELECT COUNT(*) FROM event_store WHERE batch_id <= ?",
           [int(cutoff_inclusive)]
       ).fetchone()
       n = int(row[0]) if row and row[0] is not None else 0
       if n == 0:
           return 0
       self.con.execute("BEGIN")
       try:
           self.con.execute("DELETE FROM event_store WHERE batch_id <= ?", [int(cutoff_inclusive)])
           self.con.execute("COMMIT")
       except Exception:
           self.con.execute("ROLLBACK")
           raise
       # Merge free pages; lighter than VACUUM and safe to do every prune
       try:
           self.con.execute("CHECKPOINT")
           emit_event("feeder", "CORE", "event_store", "INFO", "PRUNE",
                      f"pruned {n} envelopes up to batch_id {cutoff_inclusive}")
       except Exception:
           pass
       return n