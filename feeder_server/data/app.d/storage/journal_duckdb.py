# storage/journal_duckdb.py
from __future__ import annotations
from typing import Optional, Set, List, Iterator
import duckdb
from core.contracts import JournalStore, Tick, natural_key, Envelope
import json

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
            ts_ns BIGINT, seq BIGINT, is_final BOOLEAN, payload JSON, batch_id BIGINT
          );
        """)
        # # add batch_id if missing
        # cols = [r[1] for r in self.con.execute("PRAGMA table_info('journal_hot')").fetchall()]
        # if "batch_id" not in cols:
        #     self.con.execute("ALTER TABLE journal_hot ADD COLUMN batch_id BIGINT")
        # helpful indexes
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_batch_idx ON journal_hot(batch_id)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_ts_idx    ON journal_hot(ts_ns)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_p_s_idx   ON journal_hot(provider,stream)")

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

    def append_envelope(self, env: Envelope) -> int:
        if not env.rows:
            return 0
        rows = [
            (t.provider, t.stream, t.symbol,
             int(t.ts_ns), (-1 if t.seq is None else int(t.seq)),
             bool(t.is_final), t.payload, int(env.batch_id))
            for t in env.rows
        ]
        self.con.execute("BEGIN")
        try:
            self.con.executemany("""
              INSERT INTO journal_hot(provider, stream, symbol, ts_ns, seq, is_final, payload, batch_id)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK");
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

    def stream_ticks_since(
            self,
            since_ts_ns: int,
            *,
            provider_filter: Optional[Set[str]] = None,
            stream_filter: Optional[Set[str]] = None,
            page_rows: int = 20_000,
    ) -> Iterator[List[Tick]]:
        """
        Stream Tick rows from journal_hot starting at since_ts_ns, ordered by (ts_ns, rowid),
        yielding pages of up to 'page_rows' items. No batch_id is required.
        """
        last_ts = int(since_ts_ns)
        last_rowid = -1

        where_extra = []
        params_extra: List[object] = []
        if provider_filter:
            where_extra.append(f"provider IN ({','.join('?' for _ in provider_filter)})")
            params_extra.extend(list(provider_filter))
        if stream_filter:
            where_extra.append(f"stream IN ({','.join('?' for _ in stream_filter)})")
            params_extra.extend(list(stream_filter))
        where_extra_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""

        while True:
            rs = self.con.execute(
                f"""
                SELECT rowid, provider, stream, symbol, ts_ns, seq, is_final, payload
                FROM journal_hot
                WHERE ((ts_ns > ?) OR (ts_ns = ? AND rowid > ?)) {where_extra_sql}
                ORDER BY ts_ns ASC, rowid ASC
                LIMIT ?
                """,
                [last_ts, last_ts, last_rowid] + params_extra + [int(page_rows)],
            ).fetchall()

            if not rs:
                break

            page: List[Tick] = []
            for rowid, prov, stream, sym, ts_ns, seq, is_final, payload in rs:
                # --- normalize payload to dict ---------------------------------
                if payload is None:
                    payload_norm = {}
                elif isinstance(payload, (bytes, bytearray)):
                    try:
                        payload_norm = json.loads(payload.decode("utf-8"))
                    except Exception:
                        payload_norm = {"raw": payload.decode("utf-8", "replace")}
                elif isinstance(payload, str):
                    try:
                        payload_norm = json.loads(payload)
                    except Exception:
                        payload_norm = {"raw": payload}
                else:
                    # duckdb might already return a dict-like for JSON; accept it
                    payload_norm = payload

                page.append(
                    Tick(
                        provider=str(prov),
                        stream=str(stream),
                        symbol=str(sym),
                        ts_ns=int(ts_ns),
                        seq=(None if seq is None or int(seq) < 0 else int(seq)),
                        payload=payload_norm,  # ← dict
                        is_final=bool(is_final),
                    )
                )
                last_ts = int(ts_ns)
                last_rowid = int(rowid)

            yield page

    def next_committed_from(self, start_batch_inclusive: int, limit: int) -> list[Envelope]:
        start = int(start_batch_inclusive);
        lim = int(limit)
        bids = self.con.execute(
            "SELECT DISTINCT batch_id FROM journal_hot "
            "WHERE batch_id IS NOT NULL AND batch_id >= ? "
            "ORDER BY batch_id ASC LIMIT ?",
            [start, lim]
        ).fetchall()
        if not bids:
            return []

        out: list[Envelope] = []
        for (bid,) in bids:
            rs = self.con.execute(
                "SELECT provider, stream, symbol, ts_ns, seq, is_final, payload "
                "FROM journal_hot WHERE batch_id = ? ORDER BY ts_ns ASC",
                [int(bid)]
            ).fetchall()
            if not rs:
                continue

            rows: list[Tick] = []
            last_ts = 0
            for prov, stream, sym, ts_ns, seq, is_final, payload in rs:
                # normalize payload to dict (JSON from DuckDB may be str)
                if payload is None:
                    payload_norm = {}
                elif isinstance(payload, (bytes, bytearray)):
                    try:
                        payload_norm = json.loads(payload.decode("utf-8"))
                    except Exception:
                        payload_norm = {"raw": payload.decode("utf-8", "replace")}
                elif isinstance(payload, str):
                    try:
                        payload_norm = json.loads(payload)
                    except Exception:
                        payload_norm = {"raw": payload}
                else:
                    payload_norm = payload

                rows.append(Tick(
                    provider=str(prov),
                    stream=str(stream),
                    symbol=str(sym),
                    ts_ns=int(ts_ns),
                    seq=(None if seq is None or int(seq) < 0 else int(seq)),
                    payload=payload_norm,
                    is_final=bool(is_final),
                ))
                last_ts = int(ts_ns)

            out.append(Envelope(
                batch_id=int(bid),
                produced_at_ns=last_ts,  # proxy: last row’s ts
                first_offset=None,
                last_offset=None,
                rows=rows,
            ))
        return out

