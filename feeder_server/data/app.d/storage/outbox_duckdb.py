from __future__ import annotations
from typing import List
import duckdb, pyarrow as pa
from core.contracts import Envelope, Outbox, Tick

# ---- Arrow codecs (compact binary per-envelope) ---------------------------
def _ticks_to_arrow(ticks: List[Tick]) -> pa.Table:
    # Minimal generic schema; specialize later per-stream in sinks
    return pa.table({
        "provider": [t.provider for t in ticks],
        "stream":   [t.stream   for t in ticks],
        "symbol":   [t.symbol   for t in ticks],
        "ts_ns":    [t.ts_ns    for t in ticks],
        "seq":      [t.seq if t.seq is not None else -1 for t in ticks],
        "is_final": [t.is_final for t in ticks],
        "payload":  [t.payload  for t in ticks],  # dict -> struct via Arrow's extension
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
    from core.contracts import Tick  # local import to avoid cycles
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

# ---- DuckDB Outbox --------------------------------------------------------

class DuckDbOutbox(Outbox):
    def __init__(self, path: str):
        self.con = duckdb.connect(path)
        self.con.execute("""
          CREATE TABLE IF NOT EXISTS outbox(
            batch_id    BIGINT PRIMARY KEY,
            produced_at TIMESTAMP,
            first_off   BIGINT,
            last_off    BIGINT,
            row_count   INTEGER,
            payload     BLOB
          );
        """)
        self.con.execute("""
          CREATE TABLE IF NOT EXISTS acks(
            last_committed BIGINT
          );
        """)
        if self.con.execute("SELECT COUNT(*) FROM acks").fetchone()[0] == 0:
            self.con.execute("INSERT INTO acks VALUES (0)")

    def append(self, env: Envelope) -> None:
        tbl = _ticks_to_arrow(env.rows)
        blob = _table_to_ipc_bytes(tbl)
        self.con.execute(
            "INSERT INTO outbox VALUES (?, to_timestamp(?/1e9), ?, ?, ?, ?)",
            [env.batch_id, env.produced_at_ns, env.first_offset or -1,
             env.last_offset or -1, len(env.rows), blob]
        )

    def next_from(self, next_batch_id: int, max_n: int) -> List[Envelope]:
        rows = self.con.execute(
            "SELECT batch_id, extract(epoch FROM produced_at)*1e9::BIGINT, first_off, last_off, payload "
            "FROM outbox WHERE batch_id >= ? ORDER BY batch_id LIMIT ?",
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
        return int(self.con.execute("SELECT last_committed FROM acks").fetchone()[0])

    def mark_committed(self, batch_id: int) -> None:
        self.con.execute("UPDATE acks SET last_committed = ?", [int(batch_id)])
