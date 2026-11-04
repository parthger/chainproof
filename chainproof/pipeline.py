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
from .rpc import ChainReader
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


def find_rewind_point(store: Store, reader: ChainReader, chain_id: int, token: str) -> tuple[int, str]:
    """Highest stored block whose hash is still canonical."""
    for number, stored in store.stored_block_hashes(chain_id, token):
        if reader.block_hash(number) == stored:
            return number, stored
    raise ReorgDetected("no stored block in the last 256 is canonical; rewind manually with `chainproof rewind`")


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
) -> RunReport:
    t0 = time.monotonic()
    addr = token.address

    for name, r in (("primary", reader), ("verifier", verifier)):
        cid = r.chain_id()
        if cid != chain.chain_id:
            raise PipelineError(f"{name} RPC is chain {cid}, expected {chain.chain_id} ({chain.name})")

    fin_a, _ = reader.finalized_block()
    fin_b, _ = verifier.finalized_block()
    finalized = min(fin_a, fin_b)  # both sources must agree the range is final

    rewound_to = None
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
        start = wm_block + 1
    else:
        start = from_block if from_block is not None else finalized - lookback_blocks + 1

    end = min(finalized, start + max_blocks - 1)
    report = RunReport(None, chain.name, token.symbol, start, end, finalized, rewound_to=rewound_to)
    if end < start:
        report.seconds = time.monotonic() - t0
        return report

    run_id = store.begin_run(chain.chain_id, addr, start, end)
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
