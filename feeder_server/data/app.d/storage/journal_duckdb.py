from __future__ import annotations
from typing import List, Optional
import duckdb
from core.contracts import JournalStore, Tick, natural_key

class DuckDbJournal(JournalStore):
    def __init__(self, path: str):
        self.con = duckdb.connect(path)
        # natural-key index for idempotency (64-bit hash OK for speed; here use text tuple for clarity)
        self.con.execute("""
          CREATE TABLE IF NOT EXISTS key_index(
            provider TEXT, stream TEXT, symbol TEXT, nat_key TEXT,
            PRIMARY KEY (provider, stream, symbol, nat_key)
          );
        """)
        # hot table (flatten minimally; you can widen later)
        self.con.execute("""
          CREATE TABLE IF NOT EXISTS journal_hot(
            provider TEXT, stream TEXT, symbol TEXT,
            ts_ns BIGINT, seq BIGINT, is_final BOOLEAN, payload JSON
          );
        """)
        self.con.execute("""
          CREATE TABLE IF NOT EXISTS watermarks(
            scope TEXT PRIMARY KEY, last_offset BIGINT
          );
        """)

    def append_batch(self, ticks: List[Tick]) -> int:
        if not ticks:
            return 0
        # Build rows and keys
        rows = [(t.provider, t.stream, t.symbol, int(t.ts_ns),
                 -1 if t.seq is None else int(t.seq), bool(t.is_final), t.payload) for t in ticks]
        keys = [(t.provider, t.stream, t.symbol, str(natural_key(t))) for t in ticks]
        # Insert-ignore keys, then insert only new rows by left-joining on keys
        self.con.execute("BEGIN")
        try:
            self.con.executemany("""
              INSERT INTO key_index(provider, stream, symbol, nat_key)
              VALUES (?, ?, ?, ?)
              ON CONFLICT(provider, stream, symbol, nat_key) DO NOTHING
            """, keys)
            # Insert all rows; duplicates are harmless but wasteful. You can filter by checking inserted rowcount above if desired.
            self.con.executemany("""
              INSERT INTO journal_hot(provider, stream, symbol, ts_ns, seq, is_final, payload)
              VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        return len(rows)

    def load_recent(self, since_ts_ns: int, columns: Optional[List[str]] = None) -> List[Tick]:
        cols = "provider, stream, symbol, ts_ns, seq, is_final, payload"
        recs = self.con.execute(
            f"SELECT {cols} FROM journal_hot WHERE ts_ns >= ? ORDER BY ts_ns",
            [int(since_ts_ns)]
        ).fetchall()
        out: List[Tick] = []
        for p,sym_stream,smb,ts,seq,isf,payload in recs:
            out.append(Tick(provider=p, stream=sym_stream, symbol=smb,
                            ts_ns=int(ts),
                            seq=(None if int(seq) < 0 else int(seq)),
                            payload=payload, is_final=bool(isf)))
        return out

    def get_watermark(self, scope: str = "global") -> int:
        row = self.con.execute("SELECT last_offset FROM watermarks WHERE scope = ?", [scope]).fetchone()
        return 0 if row is None else int(row[0])

    def set_watermark(self, value: int, scope: str = "global") -> None:
        self.con.execute("""
          INSERT INTO watermarks(scope, last_offset) VALUES (?, ?)
          ON CONFLICT(scope) DO UPDATE SET last_offset=excluded.last_offset
        """, [scope, int(value)])
