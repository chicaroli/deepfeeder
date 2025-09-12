# storage/journal_duckdb.py
from __future__ import annotations
from typing import Optional, Set, List, Iterator
import threading
import time
import json
from contextlib import contextmanager
import duckdb
from core.contracts import JournalStore, Tick, natural_key, Envelope


class DuckDbJournal(JournalStore):
    """DuckDB-backed JournalStore using one shared connection protected by an RLock.

    All DB access is serialized to avoid concurrent query errors on a single duckdb.Connection.
    Streaming methods fetch pages under the lock (fetchall) and release the lock before yielding.
    """

    def __init__(self, path: str):
        self.con = duckdb.connect(path)
        # Re-entrant lock to serialize access to the shared duckdb connection
        self._lock = threading.RLock()
        # natural-key index for idempotency (64-bit hash OK for speed; here use text tuple for clarity)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS key_index(
                provider TEXT, 
                stream TEXT, 
                symbol TEXT, 
                nat_key TEXT,
                PRIMARY KEY (provider, stream, symbol, nat_key)
            );
        """)
        # hot table (flatten minimally; you can widen later)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS journal_hot(
                provider TEXT, 
                stream TEXT, 
                symbol TEXT,
                ts_ns BIGINT, 
                seq BIGINT, 
                is_final BOOLEAN, 
                payload JSON, 
                batch_id BIGINT
            );
        """)
        # helpful indexes
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_batch_idx ON journal_hot(batch_id)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_ts_idx    ON journal_hot(ts_ns)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jh_p_s_idx   ON journal_hot(provider,stream)")

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS watermarks(
                scope TEXT PRIMARY KEY, last_offset BIGINT
            );
        """)

        # canonical table: last/write-wins per (provider, stream, symbol, nat_key)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS journal_canonical(
                provider TEXT,
                stream   TEXT,
                symbol   TEXT,
                nat_key  TEXT,
                ts_ns    BIGINT,
                seq      BIGINT,
                is_final BOOLEAN,
                payload  JSON,
                updated_at_ns BIGINT,
                PRIMARY KEY (provider, stream, symbol, nat_key)
            );
        """)
        # Helpful indexes for the new readers:
        self.con.execute("CREATE INDEX IF NOT EXISTS jc_ts_idx      ON journal_canonical(ts_ns)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jc_upd_idx     ON journal_canonical(updated_at_ns)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jc_ps_idx      ON journal_canonical(provider,stream)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jc_ts_nat_idx  ON journal_canonical(ts_ns, nat_key)")
        self.con.execute("CREATE INDEX IF NOT EXISTS jc_upd_nat_idx ON journal_canonical(updated_at_ns, nat_key)")

    # inside DuckDbJournal
    @contextmanager
    def _txn(self):
        # Serialize access to the single connection
        with self._lock:
            # Start a txn explicitly
            self.con.execute("BEGIN")
            try:
                yield
                # Commit if all statements succeeded
                self.con.execute("COMMIT")
            except Exception:
                # Try to rollback; ignore "no transaction is active"
                try:
                    self.con.execute("ROLLBACK")
                except Exception:
                    pass
                raise

    def _merge_into_canonical_no_txn(self, ticks):
        if not ticks:
            return 0

        ts_ns = time.time_ns()  # one consistent timestamp for updated_at_ns

        cand_rows = [
            (t.provider, t.stream, t.symbol, str(natural_key(t)),
             int(t.ts_ns), (-1 if t.seq is None else int(t.seq)),
             bool(t.is_final), t.payload)
            for t in ticks
        ]

        # caller holds lock and is inside a transaction
        self.con.execute("DROP TABLE IF EXISTS tmp_cand")
        self.con.execute("""
            CREATE TEMPORARY TABLE tmp_cand(
                provider TEXT,
                stream   TEXT,
                symbol   TEXT,
                nat_key  TEXT,
                ts_ns    BIGINT,
                seq      BIGINT,
                is_final BOOLEAN,
                payload  JSON
            )
        """)
        self.con.executemany("""
            INSERT INTO tmp_cand(provider,stream,symbol,nat_key,ts_ns,seq,is_final,payload)
            VALUES (?,?,?,?,?,?,?,?)
        """, cand_rows)

        # Collapse to one BEST row per key to avoid duplicate inserts
        self.con.execute("DROP VIEW IF EXISTS tmp_best")
        self.con.execute("""
            CREATE TEMPORARY VIEW tmp_best AS
            SELECT provider, stream, symbol, nat_key, ts_ns, seq, is_final, payload
            FROM (
                SELECT s.*,
                       ROW_NUMBER() OVER (
                         PARTITION BY provider, stream, symbol, nat_key
                         ORDER BY is_final DESC, COALESCE(seq, -1) DESC, ts_ns DESC
                       ) AS rn
                FROM tmp_cand s
            )
            WHERE rn = 1
        """)

        # 1) UPDATE existing rows when the incoming "best" should win
        self.con.execute("""
            UPDATE journal_canonical AS c
            SET ts_ns = b.ts_ns,
                seq = b.seq,
                is_final = b.is_final,
                payload = b.payload,
                updated_at_ns = ?
            FROM tmp_best AS b
            WHERE c.provider = b.provider
              AND c.stream   = b.stream
              AND c.symbol   = b.symbol
              AND c.nat_key  = b.nat_key
              AND (
                    b.is_final
                 OR (NOT c.is_final AND (
                        (b.seq IS NOT NULL AND b.seq >= c.seq)
                     OR ((b.seq IS NULL OR b.seq < 0)
                         AND (c.seq IS NULL OR c.seq < 0)
                         AND b.ts_ns >= c.ts_ns)
                 ))
              )
        """, [ts_ns])

        # 2) INSERT keys that still don't exist (one row per key)
        self.con.execute("""
            INSERT INTO journal_canonical(
                provider, stream, symbol, nat_key, ts_ns, seq, is_final, payload, updated_at_ns
            )
            SELECT b.provider, b.stream, b.symbol, b.nat_key, b.ts_ns, b.seq, b.is_final, b.payload, ?
            FROM tmp_best AS b
            WHERE NOT EXISTS (
                SELECT 1 FROM journal_canonical c
                WHERE c.provider = b.provider
                  AND c.stream   = b.stream
                  AND c.symbol   = b.symbol
                  AND c.nat_key  = b.nat_key
            )
        """, [ts_ns])

        return len(ticks)

    def append_envelope(self, env: Envelope) -> int:
        if not env.rows:
            return 0
        rows = [
            (t.provider, t.stream, t.symbol,
             int(t.ts_ns), (-1 if t.seq is None else int(t.seq)),
             bool(t.is_final), t.payload, int(env.batch_id))
            for t in env.rows
        ]
        with self._txn():
            self.con.executemany("""
                INSERT INTO journal_hot(provider, stream, symbol, ts_ns, seq, is_final, payload, batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            # Upsert into canonical from env.rows
            self._merge_into_canonical_no_txn(env.rows)
        return len(rows)

    def load_recent(
            self,
            since_ts_ns: int,
            columns: Optional[List[str]] = None,
            *,
            canonical: bool = True,
    ) -> List[Tick]:
        cols = "provider, stream, symbol, ts_ns, seq, is_final, payload"
        table = "journal_canonical" if canonical else "journal_hot"
        with self._lock:
            recs = self.con.execute(
                f"SELECT {cols} FROM {table} WHERE ts_ns >= ? ORDER BY ts_ns",
                [int(since_ts_ns)],
            ).fetchall()
        out: List[Tick] = []
        for p, s, sym, ts, seq, isf, payload in recs:
            out.append(Tick(
                provider=p, stream=s, symbol=sym,
                ts_ns=int(ts),
                seq=(None if seq is None or int(seq) < 0 else int(seq)),
                payload=(payload if isinstance(payload, dict) else
                         (json.loads(payload) if isinstance(payload, str) else ({} if payload is None else payload))),
                is_final=bool(isf),
            ))
        return out

    def get_watermark(self, scope: str = "global") -> int:
        with self._lock:
            row = self.con.execute("SELECT last_offset FROM watermarks WHERE scope = ?", [scope]).fetchone()
        return 0 if row is None else int(row[0])

    def set_watermark(self, value: int, scope: str = "global") -> None:
        with self._lock:
            self.con.execute("""
                INSERT INTO watermarks(scope, last_offset) VALUES (?, ?)
                ON CONFLICT(scope) DO UPDATE SET last_offset=excluded.last_offset
            """, [scope, int(value)])


    def stream_ticks_since(                         # type: ignore[override]
            self,
            since_ts_ns: int,
            *,
            provider_filter: Optional[Set[str]] = None,
            stream_filter: Optional[Set[str]] = None,
            page_rows: int = 20_000,
            canonical: bool = True,
            by_updated: bool = False,
    ) -> Iterator[List[Tick]]:
        """
        Stream rows starting at since_ts_ns.
        - canonical=False → read append-only journal_hot ordered by (ts_ns,rowid) [legacy].
        - canonical=True  → read journal_canonical ordered by (key_time, nat_key),
          where key_time = ts_ns (by_updated=False) or updated_at_ns (by_updated=True).
        """
        if not canonical:
            # --- original hot-path logic (unchanged) ---
            last_ts = int(since_ts_ns)
            last_rowid = -1
            where_extra, params_extra = [], []
            if provider_filter:
                where_extra.append(f"provider IN ({','.join('?' for _ in provider_filter)})")
                params_extra.extend(list(provider_filter))
            if stream_filter:
                where_extra.append(f"stream IN ({','.join('?' for _ in stream_filter)})")
                params_extra.extend(list(stream_filter))
            where_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""
            while True:
                with self._lock:
                    rs = self.con.execute(
                        f"""
                        SELECT rowid, provider, stream, symbol, ts_ns, seq, is_final, payload
                        FROM journal_hot
                        WHERE ((ts_ns > ?) OR (ts_ns = ? AND rowid > ?)) {where_sql}
                        ORDER BY ts_ns ASC, rowid ASC
                        LIMIT ?
                        """,
                        [last_ts, last_ts, last_rowid] + params_extra + [int(page_rows)],
                    ).fetchall()
                if not rs:
                    break
                page: List[Tick] = []
                for rowid, prov, stream, sym, ts_ns, seq, is_final, payload in rs:
                    payload_norm = (payload if isinstance(payload, dict) else
                                    (json.loads(payload) if isinstance(payload, str) else (
                                        {} if payload is None else payload)))
                    page.append(Tick(
                        provider=str(prov), stream=str(stream), symbol=str(sym),
                        ts_ns=int(ts_ns),
                        seq=(None if seq is None or int(seq) < 0 else int(seq)),
                        payload=payload_norm, is_final=bool(is_final),
                    ))
                    last_ts = int(ts_ns)
                    last_rowid = int(rowid)
                yield page
            return

        # --- canonical path ---
        key_col = "updated_at_ns" if by_updated else "ts_ns"
        last_t = int(since_ts_ns)
        last_nat = ""  # tie-breaker

        where_extra, params_extra = [], []
        if provider_filter:
            where_extra.append(f"provider IN ({','.join('?' for _ in provider_filter)})")
            params_extra.extend(list(provider_filter))
        if stream_filter:
            where_extra.append(f"stream IN ({','.join('?' for _ in stream_filter)})")
            params_extra.extend(list(stream_filter))
        where_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""

        while True:
            with self._lock:
                rs = self.con.execute(
                    f"""
                    SELECT provider, stream, symbol, nat_key, ts_ns, seq, is_final, payload
                    FROM journal_canonical
                    WHERE (({key_col} > ?) OR ({key_col} = ? AND nat_key > ?)) {where_sql}
                    ORDER BY {key_col} ASC, nat_key ASC
                    LIMIT ?
                    """,
                    [last_t, last_t, last_nat] + params_extra + [int(page_rows)],
                ).fetchall()
            if not rs:
                break

            page: List[Tick] = []
            for prov, stream, sym, nat_key, ts_ns, seq, is_final, payload in rs:
                payload_norm = (payload if isinstance(payload, dict) else
                                (json.loads(payload) if isinstance(payload, str) else (
                                    {} if payload is None else payload)))
                page.append(Tick(
                    provider=str(prov), stream=str(stream), symbol=str(sym),
                    ts_ns=int(ts_ns),
                    seq=(None if seq is None or int(seq) < 0 else int(seq)),
                    payload=payload_norm, is_final=bool(is_final),
                ))
                # # advance cursor on the chosen key
                # last_t = int(ts_ns) if not by_updated else max(last_t, int(self.con.execute(
                #     "SELECT updated_at_ns FROM journal_canonical WHERE provider=? AND stream=? AND symbol=? AND nat_key=?",
                #     [prov, stream, sym, nat_key]).fetchone()[0]))
                # last_nat = str(nat_key)

            # advance cursor using last row from this page (no extra SELECT)
            last_key_time, last_nat_key = rs[-1][-2], rs[-1][3]
            last_t = int(last_key_time)
            last_nat = str(last_nat_key)

            yield page

    def next_committed_from(self, start_batch_inclusive: int, limit: int) -> list[Envelope]:
        start = int(start_batch_inclusive)
        lim = int(limit)
        # read bids under lock
        with self._lock:
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
            with self._lock:
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
