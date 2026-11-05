"""DuckDB storage: staging, published tables, a hash-checked watermark, and an audit log.

Rules this module enforces:
* Amounts are stored as decimal strings and summed as BIGNUM, so uint256 never rounds.
* The natural key (chain_id, block_number, log_index) is the primary key.
* Rows land in `staging_transfers` first. Only `publish()` moves them to `transfers`,
  and it rebuilds the affected days of the mart and advances the watermark in the SAME
  transaction. A failed reconciliation therefore leaves published data untouched.
* The watermark stores the block hash. If the chain no longer has that hash at that height,
  the next run refuses to continue (reorg tripwire) until you rewind.
* Daily rollups replace whole days, never merge by key, so a day that lost its last row
  after a rewind loses its mart row too.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from .decode import Transfer

SCHEMA = """
CREATE TABLE IF NOT EXISTS transfers ({COLUMNS}    PRIMARY KEY (chain_id, block_number, log_index)
);

-- Same columns AND the same CHECK constraints as `transfers`, so a malformed row fails at
-- stage time, before any RPC evidence is gathered. The key adds run_id: rows of an abandoned
-- run may still be here when the next run stages the same blocks.
CREATE TABLE IF NOT EXISTS staging_transfers ({COLUMNS}    PRIMARY KEY (run_id, chain_id, block_number, log_index)
);

CREATE TABLE IF NOT EXISTS watermarks (
    chain_id      INTEGER NOT NULL,
    token         VARCHAR NOT NULL,
    block_number  BIGINT  NOT NULL,
    block_hash    VARCHAR NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (chain_id, token)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id        VARCHAR PRIMARY KEY,
    chain_id      INTEGER NOT NULL,
    token         VARCHAR NOT NULL,
    start_block   BIGINT  NOT NULL,
    end_block     BIGINT  NOT NULL,
    status        VARCHAR NOT NULL,             -- staged | published | failed | abandoned
    row_count     BIGINT,
    checks        VARCHAR,                      -- JSON
    error         VARCHAR,
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP
);

-- Ranges deliberately skipped because the chain state needed to verify them was no longer
-- served (pruned). Recorded, never hidden: the dashboard and `status` show them.
CREATE TABLE IF NOT EXISTS gaps (
    chain_id      INTEGER NOT NULL,
    token         VARCHAR NOT NULL,
    from_block    BIGINT  NOT NULL,
    to_block      BIGINT  NOT NULL,
    reason        VARCHAR NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (chain_id, token, from_block)
);

CREATE TABLE IF NOT EXISTS token_daily (
    chain_id      INTEGER NOT NULL,
    token         VARCHAR NOT NULL,
    day           DATE    NOT NULL,             -- UTC day
    transfers     BIGINT  NOT NULL,
    volume_raw    VARCHAR NOT NULL,             -- exact
    minted_raw    VARCHAR NOT NULL,
    burned_raw    VARCHAR NOT NULL,
    senders       BIGINT  NOT NULL,
    receivers     BIGINT  NOT NULL,
    first_block   BIGINT  NOT NULL,
    last_block    BIGINT  NOT NULL,
    first_time    TIMESTAMP NOT NULL,
    last_time     TIMESTAMP NOT NULL,
    PRIMARY KEY (chain_id, token, day)
);
"""

SCHEMA = SCHEMA.replace(
    "{COLUMNS}",
    """
    chain_id      INTEGER  NOT NULL,
    block_number  BIGINT   NOT NULL,
    log_index     INTEGER  NOT NULL,
    block_hash    VARCHAR  NOT NULL CHECK (length(block_hash) = 66),
    block_time    TIMESTAMP NOT NULL CHECK (block_time >= TIMESTAMP '2015-07-30 00:00:00'),  -- UTC, after Ethereum genesis
    tx_hash       VARCHAR  NOT NULL CHECK (length(tx_hash) = 66),
    token         VARCHAR  NOT NULL CHECK (length(token) = 42),
    from_address  VARCHAR  NOT NULL CHECK (length(from_address) = 42),
    to_address    VARCHAR  NOT NULL CHECK (length(to_address) = 42),
    amount_raw    VARCHAR  NOT NULL CHECK (regexp_full_match(amount_raw, '[0-9]{1,78}')),
    amount        DOUBLE   NOT NULL,            -- decimal-adjusted, charts only, never reconcile on it
    run_id        VARCHAR  NOT NULL,
""",
)

ZERO = "0x" + "0" * 40


class StoreError(RuntimeError):
    pass


class WatermarkMoved(StoreError):
    """Someone else published, or the run was already published. Refusing to double-write."""


def _utc(ts: int | None) -> datetime:
    if ts is None:
        raise StoreError("transfer has no block_time; extract must fill timestamps before staging")
    return datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: list) -> tuple:
    row = con.execute(sql, params).fetchone()
    if row is None:
        raise StoreError(f"query returned no row: {sql.split()[0:4]}")
    return row


class Store:
    def __init__(self, path: str | Path = ":memory:", decimals: int = 6) -> None:
        self.path = str(path)
        self.con = duckdb.connect(self.path)
        self.con.execute("SET TimeZone = 'UTC'")
        self._migrate_staging()
        self.con.execute(SCHEMA)
        self.con.execute("ALTER TABLE runs ADD COLUMN IF NOT EXISTS finalized_block BIGINT")
        self.decimals = decimals

    def _migrate_staging(self) -> None:
        """Databases created by 0.1.0 have a staging table without constraints. Staging only holds
        rows of in-flight runs, so it is safe to drop and recreate it with the real DDL."""
        exists = self.con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'staging_transfers'").fetchone()[0]
        if not exists:
            return
        checks = self.con.execute(
            "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'staging_transfers' AND constraint_type = 'CHECK'"
        ).fetchone()[0]
        if checks == 0:
            self.con.execute("DROP TABLE staging_transfers")

    def close(self) -> None:
        self.con.close()

    # ---- watermark -------------------------------------------------------

    def watermark(self, chain_id: int, token: str) -> tuple[int, str] | None:
        row = self.con.execute(
            "SELECT block_number, block_hash FROM watermarks WHERE chain_id = ? AND token = ?",
            [chain_id, token],
        ).fetchone()
        return (int(row[0]), row[1]) if row else None

    # ---- runs ------------------------------------------------------------

    def begin_run(self, chain_id: int, token: str, start: int, end: int, finalized: int | None = None) -> str:
        run_id = f"{_now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
        self.con.execute(
            """INSERT INTO runs (run_id, chain_id, token, start_block, end_block, status, started_at, finalized_block)
               VALUES (?, ?, ?, ?, ?, 'staged', ?, ?)""",
            [run_id, chain_id, token, start, end, _now(), finalized],
        )
        return run_id

    def abandon_stale_runs(self, chain_id: int, token: str) -> int:
        """Mark runs left in 'staged' by a killed process as abandoned and drop their staged rows.

        DuckDB allows one writer per database file, so while this process holds the file no other
        run can be in flight: any 'staged' run found here is dead.
        """
        stale = [
            r[0]
            for r in self.con.execute(
                "SELECT run_id FROM runs WHERE chain_id = ? AND token = ? AND status = 'staged'", [chain_id, token]
            ).fetchall()
        ]
        for run_id in stale:
            self.con.execute("DELETE FROM staging_transfers WHERE run_id = ?", [run_id])
            self.con.execute(
                "UPDATE runs SET status = 'abandoned', error = ?, finished_at = ? WHERE run_id = ?",
                ["process exited before the run was published or failed", _now(), run_id],
            )
        return len(stale)

    def fail_run(self, run_id: str, error: str, checks: list[dict] | None = None) -> None:
        self.con.execute("DELETE FROM staging_transfers WHERE run_id = ?", [run_id])
        self.con.execute(
            "UPDATE runs SET status = 'failed', error = ?, checks = ?, finished_at = ? WHERE run_id = ?",
            [error[:4000], json.dumps(checks) if checks else None, _now(), run_id],
        )

    # ---- staging ---------------------------------------------------------

    def stage(self, run_id: str, transfers: list[Transfer]) -> int:
        scale = 10**self.decimals
        tbl = pa.table(
            {
                "chain_id": pa.array([t.chain_id for t in transfers], pa.int32()),
                "block_number": pa.array([t.block_number for t in transfers], pa.int64()),
                "log_index": pa.array([t.log_index for t in transfers], pa.int32()),
                "block_hash": [t.block_hash for t in transfers],
                "block_time": pa.array([_utc(t.block_time) for t in transfers], pa.timestamp("us")),
                "tx_hash": [t.tx_hash for t in transfers],
                "token": [t.token for t in transfers],
                "from_address": [t.from_address for t in transfers],
                "to_address": [t.to_address for t in transfers],
                "amount_raw": [str(t.amount_raw) for t in transfers],
                "amount": pa.array([t.amount_raw / scale for t in transfers], pa.float64()),
                "run_id": [run_id] * len(transfers),
            }
        )
        self.con.register("_incoming", tbl)
        try:
            self.con.execute("INSERT INTO staging_transfers SELECT * FROM _incoming")
        finally:
            self.con.unregister("_incoming")
        self.con.execute("UPDATE runs SET row_count = ? WHERE run_id = ?", [len(transfers), run_id])
        return len(transfers)

    def staged(self, run_id: str) -> list[Transfer]:
        """Read staged rows back, so the gate checks what is actually stored, not what we meant to store."""
        rows = self.con.execute(
            """SELECT chain_id, block_number, block_hash, log_index, tx_hash, token,
                      from_address, to_address, amount_raw, epoch(block_time)::BIGINT
               FROM staging_transfers WHERE run_id = ? ORDER BY block_number, log_index""",
            [run_id],
        ).fetchall()
        return [Transfer(c, bn, bh, li, tx, tok, fa, ta, int(raw), ts) for c, bn, bh, li, tx, tok, fa, ta, raw, ts in rows]

    # ---- publish (one transaction) ---------------------------------------

    def publish(
        self,
        run_id: str,
        chain_id: int,
        token: str,
        start: int,
        end: int,
        end_hash: str,
        checks: list[dict],
    ) -> int:
        con = self.con
        con.execute("BEGIN TRANSACTION")
        try:
            wm = self.watermark(chain_id, token)
            if wm is not None and wm[0] != start - 1:
                raise WatermarkMoved(f"watermark is {wm[0]}, run starts at {start}; expected {start - 1}")
            status = con.execute("SELECT status FROM runs WHERE run_id = ?", [run_id]).fetchone()
            if not status or status[0] != "staged":
                raise WatermarkMoved(f"run {run_id} is {status and status[0]}, not staged")

            con.execute(
                "DELETE FROM transfers WHERE chain_id = ? AND token = ? AND block_number BETWEEN ? AND ?",
                [chain_id, token, start, end],
            )
            con.execute("INSERT INTO transfers SELECT * FROM staging_transfers WHERE run_id = ?", [run_id])
            days = [
                r[0]
                for r in con.execute(
                    "SELECT DISTINCT CAST(block_time AS DATE) FROM staging_transfers WHERE run_id = ?", [run_id]
                ).fetchall()
            ]
            self._rebuild_days(chain_id, token, days)
            con.execute(
                "INSERT OR REPLACE INTO watermarks VALUES (?, ?, ?, ?, ?)",
                [chain_id, token, end, end_hash, _now()],
            )
            n = _one(con, "SELECT count(*) FROM staging_transfers WHERE run_id = ?", [run_id])[0]
            con.execute("DELETE FROM staging_transfers WHERE run_id = ?", [run_id])
            con.execute(
                "UPDATE runs SET status = 'published', checks = ?, finished_at = ? WHERE run_id = ?",
                [json.dumps(checks), _now(), run_id],
            )
            con.execute("COMMIT")
            return int(n)
        except Exception:
            con.execute("ROLLBACK")
            raise

    def _rebuild_days(self, chain_id: int, token: str, days: list) -> None:
        if not days:
            return
        lo = datetime.combine(min(days), datetime.min.time())
        hi = datetime.combine(max(days) + timedelta(days=1), datetime.min.time())
        self.con.execute(
            "DELETE FROM token_daily WHERE chain_id = ? AND token = ? AND list_contains(?, day)",
            [chain_id, token, days],
        )
        self.con.execute(
            f"""
            INSERT INTO token_daily
            SELECT chain_id, token, CAST(block_time AS DATE) AS day,
                   count(*),
                   sum(amount_raw::BIGNUM)::VARCHAR,
                   coalesce(sum(CASE WHEN from_address = '{ZERO}' THEN amount_raw::BIGNUM END), 0::BIGNUM)::VARCHAR,
                   coalesce(sum(CASE WHEN to_address   = '{ZERO}' THEN amount_raw::BIGNUM END), 0::BIGNUM)::VARCHAR,
                   count(DISTINCT from_address) FILTER (WHERE from_address <> '{ZERO}'),
                   count(DISTINCT to_address)   FILTER (WHERE to_address   <> '{ZERO}'),
                   min(block_number), max(block_number), min(block_time), max(block_time)
            FROM transfers
            WHERE chain_id = ? AND token = ? AND block_time >= ? AND block_time < ?
              AND list_contains(?, CAST(block_time AS DATE))
            GROUP BY ALL
            """,
            [chain_id, token, lo, hi, days],
        )

    # ---- rewind ----------------------------------------------------------

    def stored_block_hashes(self, chain_id: int, token: str, limit: int = 256) -> list[tuple[int, str]]:
        return [
            (int(b), h)
            for b, h in self.con.execute(
                """SELECT DISTINCT block_number, block_hash FROM transfers
                   WHERE chain_id = ? AND token = ? ORDER BY block_number DESC LIMIT ?""",
                [chain_id, token, limit],
            ).fetchall()
        ]

    def rewind(self, chain_id: int, token: str, to_block: int, to_hash: str) -> int:
        """Delete everything above `to_block`, rebuild the days it touched, move the watermark back."""
        con = self.con
        con.execute("BEGIN TRANSACTION")
        try:
            days = [
                r[0]
                for r in con.execute(
                    """SELECT DISTINCT CAST(block_time AS DATE) FROM transfers
                       WHERE chain_id = ? AND token = ? AND block_number > ?""",
                    [chain_id, token, to_block],
                ).fetchall()
            ]
            n = _one(
                con,
                "SELECT count(*) FROM transfers WHERE chain_id = ? AND token = ? AND block_number > ?",
                [chain_id, token, to_block],
            )[0]
            con.execute(
                "DELETE FROM transfers WHERE chain_id = ? AND token = ? AND block_number > ?",
                [chain_id, token, to_block],
            )
            self._rebuild_days(chain_id, token, days)
            con.execute(
                "INSERT OR REPLACE INTO watermarks VALUES (?, ?, ?, ?, ?)",
                [chain_id, token, to_block, to_hash, _now()],
            )
            con.execute("COMMIT")
            return int(n)
        except Exception:
            con.execute("ROLLBACK")
            raise

    # ---- repair --------------------------------------------------------------

    def bad_time_blocks(self, chain_id: int, token: str) -> list[int]:
        """Blocks published with an impossible block_time (0.1.0 stored arb1's "0x0" as 1970)."""
        return [
            int(r[0])
            for r in self.con.execute(
                """SELECT DISTINCT block_number FROM transfers
                   WHERE chain_id = ? AND token = ? AND block_time < TIMESTAMP '2015-07-30 00:00:00'
                   ORDER BY 1""",
                [chain_id, token],
            ).fetchall()
        ]

    def repair_block_times(self, chain_id: int, token: str, times: dict[int, int]) -> int:
        """Set the real block_time for the given blocks and rebuild every day they touched, old and new."""
        if not times:
            return 0
        con = self.con
        con.execute("BEGIN TRANSACTION")
        try:
            blocks = sorted(times)
            old_days = [
                r[0]
                for r in con.execute(
                    """SELECT DISTINCT CAST(block_time AS DATE) FROM transfers
                       WHERE chain_id = ? AND token = ? AND list_contains(?, block_number)""",
                    [chain_id, token, blocks],
                ).fetchall()
            ]
            fix = pa.table(
                {
                    "block_number": pa.array(blocks, pa.int64()),
                    "block_time": pa.array([_utc(times[b]) for b in blocks], pa.timestamp("us")),
                }
            )
            con.register("_fix", fix)
            try:
                n = _one(
                    con,
                    """SELECT count(*) FROM transfers t JOIN _fix f USING (block_number)
                       WHERE t.chain_id = ? AND t.token = ?""",
                    [chain_id, token],
                )[0]
                con.execute(
                    """UPDATE transfers SET block_time = f.block_time FROM _fix f
                       WHERE transfers.block_number = f.block_number AND transfers.chain_id = ? AND transfers.token = ?""",
                    [chain_id, token],
                )
            finally:
                con.unregister("_fix")
            new_days = [datetime.fromtimestamp(times[b], UTC).date() for b in blocks]
            self._rebuild_days(chain_id, token, sorted(set(old_days) | set(new_days)))
            con.execute("COMMIT")
            return int(n)
        except Exception:
            con.execute("ROLLBACK")
            raise

    # ---- re-anchor (explicit gap) -------------------------------------------

    def reanchor(self, chain_id: int, token: str, to_block: int, to_hash: str, reason: str) -> tuple[int, int] | None:
        """Skip ahead: record (watermark+1 .. to_block) as a gap and move the watermark to `to_block`.

        Used when the state needed to verify the skipped range is no longer served. Published rows
        are untouched; the skipped range is visible in `gaps` instead of silently missing.
        """
        con = self.con
        con.execute("BEGIN TRANSACTION")
        try:
            wm = self.watermark(chain_id, token)
            if wm is None or to_block <= wm[0]:
                con.execute("ROLLBACK")
                return None
            gap = (wm[0] + 1, to_block)
            con.execute(
                "INSERT INTO gaps VALUES (?, ?, ?, ?, ?, ?)",
                [chain_id, token, gap[0], gap[1], reason[:500], _now()],
            )
            con.execute(
                "INSERT OR REPLACE INTO watermarks VALUES (?, ?, ?, ?, ?)",
                [chain_id, token, to_block, to_hash, _now()],
            )
            con.execute("COMMIT")
            return gap
        except Exception:
            con.execute("ROLLBACK")
            raise

    def gaps(self, chain_id: int, token: str) -> list[tuple]:
        return self.con.execute(
            "SELECT from_block, to_block, reason, created_at FROM gaps WHERE chain_id = ? AND token = ? ORDER BY from_block",
            [chain_id, token],
        ).fetchall()

    # ---- reads for reports -------------------------------------------------

    def daily(self, chain_id: int, token: str) -> list[tuple]:
        return self.con.execute(
            """SELECT day, transfers, volume_raw, minted_raw, burned_raw, senders, receivers,
                      first_block, last_block, first_time, last_time
               FROM token_daily WHERE chain_id = ? AND token = ? ORDER BY day""",
            [chain_id, token],
        ).fetchall()

    def runs(self, limit: int = 20) -> list[tuple]:
        return self.con.execute(
            """SELECT run_id, chain_id, token, start_block, end_block, status, row_count, error, checks
               FROM runs ORDER BY started_at DESC LIMIT ?""",
            [limit],
        ).fetchall()

    def published_count(self, chain_id: int, token: str) -> int:
        return int(_one(self.con, "SELECT count(*) FROM transfers WHERE chain_id = ? AND token = ?", [chain_id, token])[0])
