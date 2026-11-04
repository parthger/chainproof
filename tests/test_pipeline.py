"""End-to-end tests on the fake chain, including every failure the gate is supposed to stop."""

from collections import defaultdict
from datetime import UTC, datetime

import pytest

from chainproof.config import Chain, Token
from chainproof.decode import ZERO_ADDRESS
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
