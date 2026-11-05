"""One pipeline run: watermark -> extract -> stage -> reconcile -> publish.

    read watermark, check its hash against the chain (reorg tripwire)
    choose range: watermark+1 .. min(finalized, watermark+max_blocks)
    extract Transfer logs (adaptive window)        -> staging_transfers
    read the staged rows back                       (check what is stored, not what we meant to store)
    gather evidence from the chain at the range edges, plus an independent re-read
    gate: every check must pass, and a planted fault must be caught
    publish rows + rebuild affected days + advance watermark, in one transaction

Anything that fails leaves published tables and the watermark exactly as they were.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field

from .config import Chain, Token
from .extract import ExtractStats, extract_transfers
from .reconcile import CheckResult, Evidence, ReconciliationError, gate, gather_evidence
from .rpc import ChainReader, StateUnavailable
from .store import Store

log = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    pass


class ReorgDetected(PipelineError):
    pass


@dataclass
class RunReport:
    run_id: str | None
    chain: str
    token: str
    start: int
    end: int
    finalized: int
    rows: int = 0
    status: str = "noop"
    checks: list[dict] = field(default_factory=list)
    extract: dict = field(default_factory=dict)
    seconds: float = 0.0
    evidence: dict = field(default_factory=dict)
    rewound_to: int | None = None
    gap: tuple[int, int] | None = None
    abandoned: int = 0


def find_rewind_point(store: Store, reader: ChainReader, chain_id: int, token: str) -> tuple[int, str]:
    """Highest stored block whose hash is still canonical."""
    for number, stored in store.stored_block_hashes(chain_id, token):
        if reader.block_hash(number) == stored:
            return number, stored
    raise ReorgDetected("no stored block in the last 256 is canonical; rewind manually with `chainproof rewind`")


def reanchor(
    store: Store, reader: ChainReader, chain: Chain, token: Token, finalized: int, lookback_blocks: int, why: str
) -> tuple[int, int] | None:
    """Move the watermark to finalized - lookback and record the skipped blocks as a gap."""
    to_block = finalized - lookback_blocks
    gap = store.reanchor(chain.chain_id, token.address, to_block, reader.block_hash(to_block), why)
    if gap:
        log.warning("re-anchored: blocks %d..%d recorded as a gap (%s)", gap[0], gap[1], why)
    return gap


def sync(
    reader: ChainReader,
    verifier: ChainReader,
    store: Store,
    chain: Chain,
    token: Token,
    *,
    time_budget: float = 900.0,
    max_runs: int = 100,
    reanchor_on_missing_state: bool = False,
    on_report=None,
    **kw,
) -> list[RunReport]:
    """Catch-up mode: run gated ranges back to back until the watermark reaches finality.

    Stops at the first failure (a failed gate must never be skipped over), when the time
    budget is spent, or after `max_runs`. Each run is still all-or-nothing.
    """
    t0 = time.monotonic()
    reports: list[RunReport] = []
    reanchored = False
    while len(reports) < max_runs and time.monotonic() - t0 < time_budget:
        try:
            rep = run(reader, verifier, store, chain, token, **kw)
        except StateUnavailable as e:
            # The range start is older than the node keeps state for. Verifying it is impossible
            # on this endpoint; skip ahead once, explicitly, and keep going.
            if not reanchor_on_missing_state or reanchored:
                raise
            fin = min(reader.finalized_block()[0], verifier.finalized_block()[0])
            reanchor(store, reader, chain, token, fin, kw.get("lookback_blocks", 1_000), f"state unavailable: {e}"[:300])
            reanchored = True
            continue
        reports.append(rep)
        if on_report:
            on_report(rep)
        if rep.status == "noop":
            break
        kw.pop("from_block", None)  # only meaningful for the very first run
    return reports


def run(
    reader: ChainReader,
    verifier: ChainReader,
    store: Store,
    chain: Chain,
    token: Token,
    *,
    lookback_blocks: int = 1_000,
    from_block: int | None = None,
    max_blocks: int = 5_000,
    chunk: int = 200,
    balance_k: int = 10,
    canary: bool = True,
    auto_rewind: bool = False,
    max_lag: int | None = None,
) -> RunReport:
    """One gated run. `max_lag`: if the watermark is more than this many blocks behind finality,
    skip ahead to the last `lookback_blocks` and record the skipped range as a gap. Public RPCs
    prune old state, so a range that starts too far back can no longer be verified at all."""
    t0 = time.monotonic()
    addr = token.address
    abandoned = store.abandon_stale_runs(chain.chain_id, addr)
    if abandoned:
        log.warning("marked %d stale staged run(s) as abandoned", abandoned)
    bad = store.bad_time_blocks(chain.chain_id, addr)
    if bad:
        times: dict[int, int] = {}
        for i in range(0, len(bad), 200):
            times.update(reader.block_timestamps(bad[i : i + 200]))
        fixed = store.repair_block_times(chain.chain_id, addr, times)
        log.warning("repaired block_time on %d published rows in %d blocks", fixed, len(bad))

    for name, r in (("primary", reader), ("verifier", verifier)):
        cid = r.chain_id()
        if cid != chain.chain_id:
            raise PipelineError(f"{name} RPC is chain {cid}, expected {chain.chain_id} ({chain.name})")

    fin_a, _ = reader.finalized_block()
    fin_b, _ = verifier.finalized_block()
    finalized = min(fin_a, fin_b)  # both sources must agree the range is final

    rewound_to = None
    gap = None
    wm = store.watermark(chain.chain_id, addr)
    if wm is not None:
        wm_block, wm_hash = wm
        live = reader.block_hash(wm_block)
        if live != wm_hash:
            if not auto_rewind:
                raise ReorgDetected(
                    f"watermark block {wm_block} was {wm_hash} but the chain now has {live}. "
                    "Run with --auto-rewind or `chainproof rewind`."
                )
            to_block, _ = find_rewind_point(store, reader, chain.chain_id, addr)
            to_hash = reader.block_hash(to_block)
            deleted = store.rewind(chain.chain_id, addr, to_block, to_hash)
            log.warning("reorg: rewound to block %d, deleted %d rows", to_block, deleted)
            rewound_to, wm_block = to_block, to_block
        if max_lag is not None and finalized - wm_block > max_lag:
            gap = reanchor(store, reader, chain, token, finalized, lookback_blocks, f"lag {finalized - wm_block} > {max_lag}")
            wm_block = finalized - lookback_blocks
        start = wm_block + 1
    else:
        start = from_block if from_block is not None else finalized - lookback_blocks + 1

    end = min(finalized, start + max_blocks - 1)
    report = RunReport(None, chain.name, token.symbol, start, end, finalized, rewound_to=rewound_to, gap=gap, abandoned=abandoned)
    if end < start:
        report.seconds = time.monotonic() - t0
        return report

    run_id = store.begin_run(chain.chain_id, addr, start, end, finalized)
    report.run_id = run_id
    results: list[CheckResult] = []
    stats = ExtractStats()
    try:
        transfers = extract_transfers(reader, chain.chain_id, addr, start, end, chunk=chunk, stats=stats)
        store.stage(run_id, transfers)
        staged = store.staged(run_id)

        ev: Evidence = gather_evidence(
            reader,
            verifier,
            token,
            chain.chain_id,
            staged,
            start,
            end,
            balance_k=balance_k,
            verifier_url=getattr(verifier, "url", ""),
        )
        try:
            results = gate(staged, ev, canary=canary)
        except ReconciliationError as e:
            results = e.results
            raise

        end_hash = reader.block_hash(end)
        checks = [asdict(c) for c in results]
        report.rows = store.publish(run_id, chain.chain_id, addr, start, end, end_hash, checks)
        report.status = "published"
        report.checks = checks
        report.evidence = ev.to_dict()
    except Exception as e:
        checks = [asdict(c) for c in results]
        store.fail_run(run_id, f"{type(e).__name__}: {e}", checks)
        report.status = "failed"
        report.checks = checks
        raise
    finally:
        report.extract = {
            "requests": stats.requests,
            "splits": stats.splits,
            "windows": len(stats.windows),
            "skipped_non_erc20": stats.skipped_non_erc20,
        }
        report.seconds = round(time.monotonic() - t0, 2)
    return report
