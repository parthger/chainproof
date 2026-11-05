"""End-to-end tests on the fake chain, including every failure the gate is supposed to stop."""

from collections import defaultdict
from datetime import UTC, datetime

import pytest

from chainproof.config import Chain, Token
from chainproof.decode import ZERO_ADDRESS, Transfer
from chainproof.extract import ExtractError, extract_transfers, fetch_logs
from chainproof.pipeline import ReorgDetected, run
from chainproof.reconcile import ReconciliationError, evaluate, gather_evidence, plant_fault
from chainproof.store import Store

from .fake_chain import TOKEN, FakeChain, FaultyReader

CHAIN = Chain("fakebase", 8453, ("fake://primary", "fake://verifier"), "")
USDC = Token("USDC", "fakebase", TOKEN, 6)


def _run(primary, verifier, store, **kw):
    kw.setdefault("from_block", primary.first)
    kw.setdefault("max_blocks", 120)
    kw.setdefault("chunk", 25)
    return run(primary, verifier, store, CHAIN, USDC, **kw)


def _published_state(store: Store):
    rows = store.con.execute(
        "SELECT block_number, log_index, from_address, to_address, amount_raw FROM transfers ORDER BY 1, 2"
    ).fetchall()
    return rows


def _expected_daily(store: Store):
    """Full recompute in Python, to compare with the incrementally maintained mart."""
    days: dict = defaultdict(lambda: [0, 0])
    for _bn, _li, _f, _t, amt, ts in store.con.execute(
        "SELECT block_number, log_index, from_address, to_address, amount_raw, epoch(block_time) FROM transfers"
    ).fetchall():
        d = datetime.fromtimestamp(ts, UTC).date()
        days[d][0] += 1
        days[d][1] += int(amt)
    return {d: (n, str(v)) for d, (n, v) in days.items()}


def _mart(store: Store):
    return {r[0]: (r[1], r[2]) for r in store.daily(CHAIN.chain_id, TOKEN)}


# ---- happy path -----------------------------------------------------------


def test_happy_path_publishes_and_resumes():
    chain = FakeChain()
    store = Store()
    r1 = _run(chain, chain, store)
    assert r1.status == "published"
    assert {c["name"]: c["passed"] for c in r1.checks} == {
        "completeness": True,
        "supply": True,
        "balances": True,
        "canonical": True,
        "timestamps": True,
        "decimals": True,
        "canary": True,
    }
    assert store.watermark(CHAIN.chain_id, TOKEN)[0] == r1.end

    r2 = _run(chain, chain, store)
    assert r2.start == r1.end + 1
    fin, _ = chain.finalized_block()
    assert r2.end == min(fin, r2.start + 119)

    # everything published equals the chain, row for row
    want = [
        (
            int(lg["blockNumber"], 16),
            int(lg["logIndex"], 16),
            "0x" + lg["topics"][1][-40:],
            "0x" + lg["topics"][2][-40:],
            str(int(lg["data"], 16)),
        )
        for n in range(chain.first, r2.end + 1)
        for lg in chain.blocks[n].logs
        if lg["address"] == TOKEN
    ]
    assert _published_state(store) == want
    assert _mart(store) == _expected_daily(store)
    assert len(_mart(store)) == 2, "fake range crosses UTC midnight; the mart must split it into two days"


def test_rerun_at_head_is_a_noop():
    chain = FakeChain(n_blocks=80)
    store = Store()
    _run(chain, chain, store, max_blocks=1_000)
    again = _run(chain, chain, store, max_blocks=1_000)
    assert again.status == "noop"


def test_never_reads_past_finality():
    chain = FakeChain(n_blocks=50, finality_lag=12)
    store = Store()
    r = _run(chain, chain, store, max_blocks=1_000)
    assert r.end == chain.head - 12


def test_timestamps_are_backfilled_when_logs_lack_them():
    chain = FakeChain(n_blocks=40)
    store = Store()
    r = _run(FaultyReader(chain, strip_timestamps=True), chain, store, max_blocks=1_000)
    assert r.status == "published"


# ---- the gate must stop bad data ------------------------------------------


def _assert_nothing_published(store: Store, before_rows, before_wm):
    assert _published_state(store) == before_rows
    assert store.watermark(CHAIN.chain_id, TOKEN) == before_wm
    assert store.con.execute("SELECT count(*) FROM staging_transfers").fetchone()[0] == 0
    assert store.runs(1)[0][5] == "failed"


def test_dropped_logs_fail_completeness_and_supply_or_balances():
    chain = FakeChain()
    store = Store()
    _run(chain, chain, store)  # one good run first: the failure must not touch it
    before_rows, before_wm = _published_state(store), store.watermark(CHAIN.chain_id, TOKEN)
    with pytest.raises(ReconciliationError) as e:
        _run(FaultyReader(chain, drop_every=7), chain, store)
    failed = {c.name for c in e.value.failed}
    assert "completeness" in failed
    _assert_nothing_published(store, before_rows, before_wm)


def test_one_raw_unit_off_is_caught():
    chain = FakeChain()
    store = Store()
    bump = next(n for n in range(chain.first + 5, chain.head) if chain.blocks[n].logs)
    with pytest.raises(ReconciliationError) as e:
        _run(FaultyReader(chain, bump_amount_at=bump), chain, store)
    assert "completeness" in {c.name for c in e.value.failed}
    _assert_nothing_published(store, [], None)


def test_supply_change_without_transfer_fails_with_direction():
    chain = FakeChain()
    chain.silent_mint(chain.first + 50, 123)
    store = Store()
    with pytest.raises(ReconciliationError) as e:
        _run(chain, chain, store)
    (supply,) = [c for c in e.value.failed if c.name == "supply"]
    assert "diff=-123" in supply.summary
    assert "state exceeds events" in supply.summary


def test_wrong_decimals_in_config_fails():
    chain = FakeChain(n_blocks=40, decimals=18)
    store = Store()
    with pytest.raises(ReconciliationError) as e:
        _run(chain, chain, store, max_blocks=1_000)
    assert [c.name for c in e.value.failed] == ["decimals"]


def test_wrong_chain_id_is_refused_before_any_work():
    chain = FakeChain(n_blocks=40, chain_id=1)
    with pytest.raises(Exception, match="chain 1, expected 8453"):
        _run(chain, chain, Store())


# ---- reorgs ---------------------------------------------------------------


def test_reorg_trips_the_wire():
    chain = FakeChain()
    store = Store()
    r = _run(chain, chain, store)
    chain.reorg(r.end - 3)  # deeper than finality would allow on a real chain: simulate a bad provider
    with pytest.raises(ReorgDetected, match="Run with --auto-rewind"):
        _run(chain, chain, store)


def test_auto_rewind_repairs_history_and_mart():
    chain = FakeChain()
    store = Store()
    r = _run(chain, chain, store)
    chain.reorg(r.end - 30)
    r2 = _run(chain, chain, store, auto_rewind=True)
    assert r2.rewound_to is not None and r2.rewound_to < r.end - 30
    assert r2.status == "published"
    # no row from the orphaned history survives
    canon = {(n, chain.blocks[n].hash) for n in chain.blocks}
    stored = set(store.con.execute("SELECT DISTINCT block_number, block_hash FROM transfers").fetchall())
    assert stored <= canon
    assert _mart(store) == _expected_daily(store)


def test_rewind_removes_days_that_lost_all_rows():
    chain = FakeChain()
    store = Store()
    r = _run(chain, chain, store, max_blocks=200)
    days_before = set(_mart(store))
    assert len(days_before) == 2
    # rewind to a block before midnight: the second day has no rows left and must disappear
    midnight_block = next(
        n for n in range(chain.first, r.end) if datetime.fromtimestamp(chain.blocks[n].timestamp, UTC).hour == 0
    )
    store.rewind(CHAIN.chain_id, TOKEN, midnight_block - 1, chain.block_hash(midnight_block - 1))
    assert len(_mart(store)) == 1
    assert _mart(store) == _expected_daily(store)


# ---- extraction -----------------------------------------------------------


def test_adaptive_window_halves_on_range_errors():
    chain = FakeChain(range_limit=17)
    transfers = extract_transfers(chain, 8453, TOKEN, chain.first, chain.first + 199, chunk=200)
    direct = FakeChain()
    want = extract_transfers(direct, 8453, TOKEN, direct.first, direct.first + 199, chunk=10)
    assert [t.key for t in transfers] == [t.key for t in want]


def test_adaptive_window_gives_up_at_the_floor():
    chain = FakeChain(n_blocks=20, range_limit=0)
    with pytest.raises(ExtractError, match="floor"):
        fetch_logs(chain, TOKEN, chain.first, chain.first + 5, chunk=4)


# ---- why one invariant is not enough ---------------------------------------


def test_supply_check_alone_is_blind_to_holder_to_holder_errors():
    chain = FakeChain()
    end = chain.first + 100
    rows = extract_transfers(chain, 8453, TOKEN, chain.first + 1, end)
    ev = gather_evidence(chain, chain, USDC, 8453, rows, chain.first + 1, end, balance_k=25)
    # corrupt a plain transfer (neither mint nor burn)
    victim = next(t for t in rows if t.from_address != ZERO_ADDRESS and t.to_address != ZERO_ADDRESS)
    bad = [t.with_amount(t.amount_raw + 1) if t.key == victim.key else t for t in rows]
    result = {c.name: c.passed for c in evaluate(bad, ev)}
    assert result["supply"] is True  # supply cannot see it...
    assert result["balances"] is False  # ...balances can
    assert result["completeness"] is False


def test_canary_mutation_is_detected():
    chain = FakeChain()
    rows = extract_transfers(chain, 8453, TOKEN, chain.first + 1, chain.first + 100)
    ev = gather_evidence(chain, chain, USDC, 8453, rows, chain.first + 1, chain.first + 100)
    mutated, _ = plant_fault(rows, ev)
    assert not all(c.passed for c in evaluate(mutated, ev))
    assert all(c.passed for c in evaluate(rows, ev))


# ---- regressions found on 2026-11-05 ---------------------------------------


def test_canary_never_picks_a_self_transfer():
    from chainproof.reconcile import Evidence, run_canary

    a, b, h = "0x" + "a" * 40, "0x" + "b" * 40, "0x" + "1" * 64
    rows = [
        Transfer(1, 10, h, 0, h, TOKEN, a, b, 5),
        Transfer(1, 10, h, 1, h, TOKEN, a, a, 1_000_000),  # largest row, but a self-transfer
    ]
    vrows = {t.key: (t.tx_hash, t.from_address, t.to_address, t.amount_raw) for t in rows}
    ev = Evidence(10, 10, 6, 6, 100, 100, [a, b], {a: 50, b: 0}, {a: 45, b: 5}, {10: h}, {}, vrows)
    assert all(c.passed for c in evaluate(rows, ev))
    canary = run_canary(rows, ev)
    assert canary.passed, canary.summary
    assert "log 0" in canary.summary


def test_staging_enforces_the_same_constraints_as_published():
    import duckdb

    store = Store()
    run_id = store.begin_run(8453, TOKEN, 1, 1)
    with pytest.raises(duckdb.ConstraintException):
        store.con.execute(
            "INSERT INTO staging_transfers VALUES (8453, 1, 0, ?, now()::TIMESTAMP, ?, ?, ?, ?, '12a', 0, ?)",
            ["0x" + "1" * 64, "0x" + "2" * 64, TOKEN, TOKEN, TOKEN, run_id],
        )


def test_old_unconstrained_staging_table_is_migrated(tmp_path):
    import duckdb

    db = tmp_path / "old.duckdb"
    Store(db).close()
    con = duckdb.connect(str(db))
    con.execute("DROP TABLE staging_transfers")
    con.execute("CREATE TABLE staging_transfers AS SELECT * FROM transfers WHERE false")  # 0.1.0 DDL
    con.close()
    store = Store(db)
    n = store.con.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'staging_transfers' AND constraint_type = 'CHECK'"
    ).fetchone()[0]
    assert n > 0


def test_killed_run_is_abandoned_and_cleaned_up():
    chain = FakeChain()
    store = Store()
    rows = extract_transfers(chain, 8453, TOKEN, chain.first, chain.first + 20)
    dead = store.begin_run(8453, TOKEN, chain.first, chain.first + 20)
    store.stage(dead, rows)  # ...and then the process is killed
    r = _run(chain, chain, store)
    assert r.status == "published" and r.abandoned == 1
    status = dict(store.con.execute("SELECT run_id, status FROM runs").fetchall())
    assert status[dead] == "abandoned"
    assert store.con.execute("SELECT count(*) FROM staging_transfers").fetchone()[0] == 0


# ---- catch-up mode and re-anchoring ----------------------------------------


def test_sync_catches_up_to_finality_in_gated_steps():
    from chainproof.pipeline import sync

    chain = FakeChain(n_blocks=400)
    store = Store()
    reps = sync(chain, chain, store, CHAIN, USDC, from_block=chain.first, max_blocks=100, chunk=25)
    fin, _ = chain.finalized_block()
    assert [r.status for r in reps][-1] == "noop"
    assert all(r.status == "published" for r in reps[:-1]) and len(reps) >= 4
    assert store.watermark(CHAIN.chain_id, TOKEN)[0] == fin
    assert _mart(store) == _expected_daily(store)


def test_sync_stops_at_the_first_failed_gate():
    from chainproof.pipeline import sync

    chain = FakeChain(n_blocks=400)
    store = Store()
    with pytest.raises(ReconciliationError):
        sync(FaultyReader(chain, drop_every=7), chain, store, CHAIN, USDC, from_block=chain.first, max_blocks=100)
    assert store.watermark(CHAIN.chain_id, TOKEN) is None


def test_max_lag_reanchors_and_records_the_gap():
    chain = FakeChain(n_blocks=400)
    store = Store()
    r1 = _run(chain, chain, store, max_blocks=50)
    chain.advance(200)
    r2 = _run(chain, chain, store, max_lag=100, lookback_blocks=30, max_blocks=1_000)
    fin, _ = chain.finalized_block()
    assert r2.gap == (r1.end + 1, fin - 30)
    assert r2.start == fin - 29 and r2.status == "published"
    assert store.gaps(CHAIN.chain_id, TOKEN)[0][:2] == (r1.end + 1, fin - 30)


def test_missing_state_reanchors_once_in_sync():
    from chainproof.pipeline import sync
    from chainproof.rpc import StateUnavailable

    class Pruned(FaultyReader):
        """State older than `keep` blocks behind the head is gone, like a public full node."""

        def call_many(self, calls, block):
            if block < self.inner.head - 150:
                raise StateUnavailable(f"historical state for {block} is not available")
            return self.inner.call_many(calls, block)

    chain = FakeChain(n_blocks=400)
    store = Store()
    _run(chain, chain, store, max_blocks=50)
    chain.advance(300)
    pruned = Pruned(chain)
    reps = sync(pruned, pruned, store, CHAIN, USDC, max_blocks=1_000, lookback_blocks=40, reanchor_on_missing_state=True)
    assert reps[-1].status == "noop" and reps[0].status == "published"
    assert len(store.gaps(CHAIN.chain_id, TOKEN)) == 1
    assert store.con.execute("SELECT count(*) FROM runs WHERE status = 'failed'").fetchone()[0] == 1


# ---- dashboard -----------------------------------------------------------------


def test_dashboard_renders_pass_fail_and_gaps(tmp_path):
    from chainproof.dashboard import render

    good_db, bad_db = tmp_path / "good.duckdb", tmp_path / "bad.duckdb"
    chain = FakeChain(n_blocks=400)
    good = Store(good_db)
    _run(chain, chain, good, max_blocks=100)
    chain.advance(200)
    _run(chain, chain, good, max_lag=100, lookback_blocks=30, max_blocks=1_000)  # records a gap
    good.close()
    bad = Store(bad_db)
    with pytest.raises(ReconciliationError):
        _run(FaultyReader(chain, drop_every=7), chain, bad)
    bad.close()

    # the fake chain lives in Oct 2025: render "as of" just after its last block
    last = datetime.fromtimestamp(chain.blocks[chain.head].timestamp, UTC)
    out = render([(CHAIN, USDC, str(good_db)), (CHAIN, USDC, str(bad_db))], tmp_path / "d.html", now=last)
    page = out.read_text()
    assert "All checks passing" in page and "Latest run failed" in page
    assert "1 of 2 chains failing" in page
    assert "Gap recorded" in page
    assert page.count('class="bar"') > 0 and "<script" in page and "http" not in page.split("<script>")[1]


# ---- timestamps (arb1 sends blockTimestamp "0x0") ------------------------------


def test_zero_block_timestamp_is_backfilled_not_stored_as_1970():
    chain = FakeChain(n_blocks=40)
    store = Store()
    r = _run(FaultyReader(chain, zero_timestamps=True), chain, store, max_blocks=1_000)
    assert r.status == "published"
    first = store.con.execute("SELECT min(block_time) FROM transfers").fetchone()[0]
    assert first.year == 2025
    assert {c["name"]: c["passed"] for c in r.checks}["timestamps"]


def test_wrong_block_time_fails_the_timestamps_check():
    chain = FakeChain()
    rows = extract_transfers(chain, 8453, TOKEN, chain.first + 1, chain.first + 100)
    ev = gather_evidence(chain, chain, USDC, 8453, rows, chain.first + 1, chain.first + 100)
    b = next(iter(ev.canonical_times))
    shifted = [t if t.block_number != b else Transfer(**{**t.__dict__, "block_time": t.block_time + 3600}) for t in rows]
    result = {c.name: c.passed for c in evaluate(shifted, ev)}
    assert result["timestamps"] is False
    assert result["completeness"] is True  # completeness compares amounts and addresses, not times


def test_1970_rows_from_an_old_database_are_repaired_on_the_next_run():
    chain = FakeChain(n_blocks=200)
    store = Store()
    _run(chain, chain, store, max_blocks=60)
    # simulate a 0.1.0 database: arb1's "0x0" stored as 1970 (no CHECK on old tables)
    store.con.execute("ALTER TABLE transfers RENAME TO t_old")
    store.con.execute("CREATE TABLE transfers AS SELECT * FROM t_old")
    store.con.execute("DROP TABLE t_old")
    store.con.execute("UPDATE transfers SET block_time = TIMESTAMP '1970-01-01' WHERE block_number < ?", [chain.first + 30])
    store._rebuild_days(CHAIN.chain_id, TOKEN, [datetime(1970, 1, 1).date()])
    assert datetime(1970, 1, 1).date() in _mart(store)
    _run(chain, chain, store, max_blocks=60)
    assert store.bad_time_blocks(CHAIN.chain_id, TOKEN) == []
    assert datetime(1970, 1, 1).date() not in _mart(store)
    assert _mart(store) == _expected_daily(store)
